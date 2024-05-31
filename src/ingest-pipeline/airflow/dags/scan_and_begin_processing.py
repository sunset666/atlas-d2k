import sys
from datetime import datetime, timedelta
from pprint import pprint

from airflow.operators.bash import BashOperator
from niddk_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
)
from niddk_operators.flex_multi_dag_run import FlexMultiDagRunOperator
from utils import (
    HMDAG,
    get_tmp_dir_path,
    get_git_provenance_list,
    get_preserve_scratch_resource,
    downstream_workflow_iter,
    get_queue_resource,
)
from airflow.configuration import conf as airflow_conf
# from airflow.exceptions import AirflowException
# from airflow.operators.python import PythonOperator

sys.path.append(airflow_conf.as_dict()["connections"]["SRC_PATH"].strip("'").strip('"'))
# from submodules import atlasd2k_prepare_replicate


sys.path.pop()


# Following are defaults which can be overridden later on
default_args = {
    "owner": "NIDDK",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["dbetancur@psc.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "xcom_push": True,
    "queue": get_queue_resource("scan_and_begin_processing"),
    "executor_config": {"SlurmExecutor": {"slurm_output_path": "/hive/users/niddk/airflow-logs/slurm/"}},
}

with HMDAG(
        "scan_and_begin_processing",
        schedule_interval=None,
        is_paused_upon_creation=False,
        default_args=default_args,
        user_defined_macros={
            "tmp_dir_path": get_tmp_dir_path,
            "preserve_scratch": get_preserve_scratch_resource("scan_and_begin_processing"),
        },
) as dag:

    # def download_replicate(**kwargs):
    #     try:
    #         replicate = atlasd2k_prepare_replicate(server_name="", args={"replicate": kwargs["replicate"]})
    #     except Exception as e:
    #         raise AirflowException(e)
    #
    #
    # t_download_replicate = PythonOperator(
    #     task_id="download_replicate",
    #     python_callable=download_replicate,
    #     provide_context=True,
    #     op_kwargs={},
    # )

    t_download_replicate = BashOperator(
        task_id="download_replicate",
        bash_command="src_dir={{dag_run.conf.atlas_d2k_path}}; \
                      tmp_dir={{tmp_dir_path(run_id)}}; \
                      deriva-download-cli --catalog 2 www.atlas-d2k.org \
                      $src_dir/Replicate_Input_Bag.json \
                      $tmp_dir \
                      rid={{dag_run.conf.submission_id}} > $tmp_dir/session.log 2>&1 ; \
                      echo $?",
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_temp_dir")


    def flex_maybe_spawn(**kwargs):
        """
        This is a generator which returns appropriate DagRunOrders
        """
        print("kwargs:")
        pprint(kwargs)
        print("dag_run conf:")
        ctx = kwargs["dag_run"].conf
        pprint(ctx)
        download_replcate_retcode = int(kwargs["ti"].xcom_pull(task_ids="download_replicate"))
        if download_replcate_retcode == 0:
            payload = {
                "ingest_id": ctx["run_id"],
                "parent_submission_id": kwargs["replicate"],
                "dag_provenance_list": get_git_provenance_list(
                    [__file__, kwargs["ti"].xcom_pull(task_ids="run_validation", key="ivt_path")]
                ),
            }
            for next_dag in downstream_workflow_iter("collectiontype", "assay_type"):
                yield next_dag, payload
        else:
            return None


    t_maybe_spawn = FlexMultiDagRunOperator(
        task_id="flex_maybe_spawn",
        dag=dag,
        trigger_dag_id="scan_and_begin_processing",
        python_callable=flex_maybe_spawn,
    )

    t_create_tmpdir >> t_download_replicate >> t_maybe_spawn >> t_cleanup_tmpdir
