from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REQUIRED = {
    "README.txt",
    "backfill_request.json",
    "change_request.txt",
    "existing_dagruns.csv",
    "task_plan.csv",
    "starter/daily_metrics.py",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def build_source(task_rows: list[dict[str, str]]) -> str:
    lines = [
        "from datetime import datetime, timezone",
        "",
        "from airflow import DAG",
        "from airflow.operators.empty import EmptyOperator",
        "from airflow.operators.latest_only import LatestOnlyOperator",
        "",
        "with DAG(",
        "    dag_id='internet_event_daily_metrics',",
        "    start_date=datetime(2026, 6, 1, tzinfo=timezone.utc),",
        "    schedule='@daily',",
        "    catchup=True,",
        "    max_active_runs=1,",
        ") as dag:",
    ]
    variables: list[str] = []
    for row in task_rows:
        task_id = row["task_id"]
        operator = row["operator"]
        depends = row["depends_on_past"].lower() == "true"
        if operator == "LatestOnlyOperator":
            lines.append(f"    {task_id} = LatestOnlyOperator(task_id='{task_id}')")
        elif operator == "EmptyOperator":
            suffix = ", depends_on_past=True" if depends else ""
            lines.append(f"    {task_id} = EmptyOperator(task_id='{task_id}'{suffix})")
        else:
            raise ValueError("任务计划包含不支持的Operator")
        variables.append(task_id)
    lines.extend(["", "    " + " >> ".join(variables), ""])
    return "\n".join(lines)


def probe_dag(dags_dir: Path, start: str, end: str) -> dict[str, object]:
    airflow_home = Path(tempfile.mkdtemp(prefix="airflow_daily_recovery_"))
    env = os.environ.copy()
    env.update({
        "AIRFLOW_HOME": str(airflow_home),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
        "AIRFLOW__CORE__DAGS_FOLDER": str(dags_dir),
    })
    completed = subprocess.run(
        [sys.executable, str(ROOT / "probe_dags.py"), "--dags", str(dags_dir), "--start", start, "--end", end],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=300,
        env=env,
    )
    shutil.rmtree(airflow_home, ignore_errors=True)
    if completed.returncode:
        raise RuntimeError(completed.stdout + completed.stderr)
    marker = next((line for line in reversed(completed.stdout.splitlines()) if line.startswith("DAILY_RECOVERY=")), None)
    if marker is None:
        raise RuntimeError("DagBag没有返回日指标结构")
    return json.loads(marker.split("=", 1)[1])


def group_dates(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    days = [date.fromisoformat(str(row["logical_date"])[:10]) for row in rows]
    groups: list[list[date]] = []
    for day in days:
        if not groups or (day - groups[-1][1]).days != 1:
            groups.append([day, day])
        else:
            groups[-1][1] = day
    return [{"start_date": start.isoformat(), "end_date": end.isoformat()} for start, end in groups]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    input_dir = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    try:
        present = {path.relative_to(input_dir).as_posix() for path in input_dir.rglob("*") if path.is_file()}
        if present != REQUIRED:
            raise ValueError("恢复材料集合发生变化")
        request = json.loads((input_dir / "backfill_request.json").read_text(encoding="utf-8"))
        existing = read_csv(input_dir / "existing_dagruns.csv")
        tasks = read_csv(input_dir / "task_plan.csv")
        if not (input_dir / "change_request.txt").read_text(encoding="utf-8").strip():
            raise ValueError("变更说明为空")
        if "internet_event_daily_metrics" not in (input_dir / "starter/daily_metrics.py").read_text(encoding="utf-8"):
            raise ValueError("当前DAG身份缺失")
        if len({row["logical_date"] for row in existing}) != len(existing):
            raise ValueError("现有运行账日期重复")
        if len({row["task_id"] for row in tasks}) != len(tasks) or sorted(int(row["task_order"]) for row in tasks) != list(range(1, len(tasks) + 1)):
            raise ValueError("任务计划身份或顺序重复")
        start = request["authorized_interval_start"]
        end = request["authorized_interval_end_exclusive"]
        if start >= end or request["dag_id"] != "internet_event_daily_metrics":
            raise ValueError("补数申请边界不正确")
        if request["historical_publish_action"] != "SKIP":
            raise ValueError("历史发布动作不受支持")
        release_fields = ["release_window", "affected_service", "wait_budget_seconds", "rollout_mode", "rollback_condition", "observation_metrics"]
        if any(field not in request for field in release_fields) or not request["observation_metrics"]:
            raise ValueError("发布安排不完整")

        output_dir.mkdir(parents=True)
        dags_dir = output_dir / "dags"
        results_dir = output_dir / "results"
        dags_dir.mkdir()
        results_dir.mkdir()
        (dags_dir / "internet_event_daily_metrics.py").write_text(build_source(tasks), encoding="utf-8")
        observed = probe_dag(dags_dir, start, end)
        if observed["import_errors"]:
            raise ValueError("DAG导入失败")
        if observed["dag_id"] != request["dag_id"] or observed["task_order"] != [row["task_id"] for row in tasks]:
            raise ValueError("DagBag任务图与任务计划不一致")
        if observed["schedule"] != "@daily" or observed["timezone"] != "UTC" or not observed["catchup"] or observed["max_active_runs"] != 1:
            raise ValueError("DAG日程属性不正确")
        if not observed["compute_depends_on_past"] or observed["latest_only_type"] != "LatestOnlyOperator":
            raise ValueError("补数依赖或发布屏障不正确")

        existing_by = {row["logical_date"]: row for row in existing}
        coverage: list[dict[str, object]] = []
        plan: list[dict[str, object]] = []
        for interval in observed["intervals"]:
            logical = interval["logical_date"]
            if not (start <= logical < end) or interval["data_interval_end"] > end:
                continue
            state = existing_by.get(logical, {}).get("state", "MISSING")
            action = "KEEP" if state == "SUCCESS" else "REPROCESS" if state in request["reprocess_states"] else "CREATE"
            row = {**interval, "existing_state": state, "action": action}
            coverage.append(row)
            if action != "KEEP":
                plan.append({"plan_order": len(plan) + 1, **row})
        plan_dates = {str(row["logical_date"]) for row in plan}
        predecessor_rows = []
        for row in plan:
            day = date.fromisoformat(str(row["logical_date"])[:10])
            predecessor = date.fromordinal(day.toordinal() - 1).isoformat() + "T00:00:00Z"
            if existing_by.get(predecessor, {}).get("state") == "SUCCESS":
                source = "EXISTING_SUCCESS"
            elif predecessor in plan_dates:
                source = "PLANNED_PREDECESSOR"
            else:
                source = "UNSATISFIED"
            predecessor_rows.append({"logical_date": row["logical_date"], "predecessor_logical_date": predecessor, "predecessor_source": source})
        if any(row["predecessor_source"] == "UNSATISFIED" for row in predecessor_rows):
            raise ValueError("恢复计划存在未覆盖前驱")
        command_groups = group_dates(plan)
        command_rows = [{"command_order": index + 1, "dag_id": request["dag_id"], **group, "rerun_failed_tasks": "true"} for index, group in enumerate(command_groups)]

        write_csv(results_dir / "dag_inventory.csv", ["dag_id", "schedule", "timezone", "catchup", "max_active_runs", "task_id", "task_type", "depends_on_past", "upstream_task_ids", "downstream_task_ids"], observed["dag_inventory"])
        write_csv(results_dir / "interval_coverage.csv", ["logical_date", "data_interval_start", "data_interval_end", "existing_state", "action"], coverage)
        write_csv(results_dir / "backfill_plan.csv", ["plan_order", "logical_date", "data_interval_start", "data_interval_end", "existing_state", "action"], plan)
        write_csv(results_dir / "predecessor_coverage.csv", ["logical_date", "predecessor_logical_date", "predecessor_source"], predecessor_rows)
        write_csv(results_dir / "command_plan.csv", ["command_order", "dag_id", "start_date", "end_date", "rerun_failed_tasks"], command_rows)
        write_csv(results_dir / "release_handoff.csv", ["release_window", "affected_service", "wait_budget_seconds", "rollout_mode", "rollback_condition", "observation_metrics", "historical_publish_action"], [{
            "release_window": request["release_window"],
            "affected_service": request["affected_service"],
            "wait_budget_seconds": request["wait_budget_seconds"],
            "rollout_mode": request["rollout_mode"],
            "rollback_condition": request["rollback_condition"],
            "observation_metrics": "|".join(request["observation_metrics"]),
            "historical_publish_action": request["historical_publish_action"],
        }])
        (output_dir / "README.txt").write_text(
            "这份恢复材料交给数据平台版本负责人。dags目录是待进入版本库的日指标DAG，interval_coverage.csv对齐授权区间与现有运行账，backfill_plan.csv只保留需要重算或创建的日分区。\n\npredecessor_coverage.csv说明depends_on_past的前驱来源，command_plan.csv供维护窗值班转换为补数命令。历史补数通过LatestOnly时跳过线上发布。release_handoff.csv记录现场窗口、观察指标和回退条件，实际启用与补跑由维护窗值班继续执行。\n",
            encoding="utf-8",
        )
    except Exception:
        if output_dir.exists():
            shutil.rmtree(output_dir)
        raise


if __name__ == "__main__":
    main()
