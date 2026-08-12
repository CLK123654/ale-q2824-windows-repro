from __future__ import annotations

import argparse
import json
from pathlib import Path

from airflow.models import DagBag
import pendulum

parser = argparse.ArgumentParser()
parser.add_argument("--dags", required=True)
parser.add_argument("--start", required=True)
parser.add_argument("--end", required=True)
args = parser.parse_args()
bag = DagBag(dag_folder=str(Path(args.dags).resolve()), include_examples=False, safe_mode=False, read_dags_from_db=False)
dag = bag.get_dag("internet_event_daily_metrics")
payload: dict[str, object] = {"import_errors": {str(key): str(value) for key, value in bag.import_errors.items()}}
if dag is not None:
    inventory = []
    for task in dag.tasks:
        inventory.append({
            "dag_id": dag.dag_id,
            "schedule": str(dag.schedule_interval),
            "timezone": str(dag.timezone),
            "catchup": str(bool(dag.catchup)).lower(),
            "max_active_runs": dag.max_active_runs,
            "task_id": task.task_id,
            "task_type": task.task_type,
            "depends_on_past": str(bool(task.depends_on_past)).lower(),
            "upstream_task_ids": "|".join(sorted(task.upstream_task_ids)),
            "downstream_task_ids": "|".join(sorted(task.downstream_task_ids)),
        })
    start = pendulum.parse(args.start)
    end = pendulum.parse(args.end)
    intervals = []
    for info in dag.iter_dagrun_infos_between(start, end, align=True):
        intervals.append({
            "logical_date": info.logical_date.in_timezone("UTC").format("YYYY-MM-DD[T]HH:mm:ss[Z]"),
            "data_interval_start": info.data_interval.start.in_timezone("UTC").format("YYYY-MM-DD[T]HH:mm:ss[Z]"),
            "data_interval_end": info.data_interval.end.in_timezone("UTC").format("YYYY-MM-DD[T]HH:mm:ss[Z]"),
        })
    payload.update({
        "dag_id": dag.dag_id,
        "schedule": str(dag.schedule_interval),
        "timezone": str(dag.timezone),
        "catchup": bool(dag.catchup),
        "max_active_runs": dag.max_active_runs,
        "task_order": [task.task_id for task in dag.tasks],
        "compute_depends_on_past": bool(dag.get_task("compute_daily_metrics").depends_on_past),
        "latest_only_type": dag.get_task("latest_only").task_type,
        "dag_inventory": inventory,
        "intervals": intervals,
    })
print("DAILY_RECOVERY=" + json.dumps(payload, ensure_ascii=False, sort_keys=True))
