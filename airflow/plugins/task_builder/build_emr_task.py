from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_file_to_s3(filename, key, bucket_name, aws_connection_id):
    s3 = S3Hook(aws_connection_id)
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)


def build_emr_task(
    dag: DAG,
    task_group_id,
    aws_connection_id,
    s3_bucket_name,
    local_dependencies_path,
    dependencies_s3_key,
    local_configs_path,
    configs_s3_key,
    scripts_by_order,
    local_scripts_path,
    scripts_s3_key,
    spark_job_flow_overrides,
    emr_connection_id,
):

    with TaskGroup(group_id=task_group_id, dag=dag) as submit_emr_job:
        with TaskGroup(
            group_id="upload_scripts_to_S3", dag=dag
        ) as upload_scripts_to_S3:
            upload_dependencies_to_s3 = PythonOperator(
                task_id="upload_dependencies_to_s3",
                dag=dag,
                python_callable=upload_file_to_s3,
                op_kwargs=dict(
                    filename=local_dependencies_path,
                    key=dependencies_s3_key,
                    bucket_name=s3_bucket_name,
                    aws_connection_id=aws_connection_id,
                ),
            )
            upload_configs_to_s3 = PythonOperator(
                task_id="upload_configs_to_s3",
                dag=dag,
                python_callable=upload_file_to_s3,
                op_kwargs=dict(
                    filename=local_configs_path,
                    key=configs_s3_key,
                    bucket_name=s3_bucket_name,
                    aws_connection_id=aws_connection_id,
                ),
            )

            for script in scripts_by_order:
                script_path = local_scripts_path / script
                PythonOperator(
                    task_id=f"upload_{script}_to_s3",
                    dag=dag,
                    python_callable=upload_file_to_s3,
                    op_kwargs=dict(
                        filename=script_path,
                        key=f"{scripts_s3_key}{script}",
                        bucket_name=s3_bucket_name,
                        aws_connection_id=aws_connection_id,
                    ),
                )

        create_emr_cluster = EmrCreateJobFlowOperator(
            task_id="create_emr_cluster",
            dag=dag,
            job_flow_overrides=spark_job_flow_overrides,
            aws_conn_id=aws_connection_id,
            emr_conn_id=emr_connection_id,
            region_name="ap-southeast-1",
        )

        spark_steps = [
            {
                "Name": script,
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--py-files",
                        "s3://{{ params.BUCKET_NAME }}/{{ params.s3_dependencies }},s3://{{ params.BUCKET_NAME }}/{{ params.s3_configs }}",
                        f"s3://{{{{ params.BUCKET_NAME }}}}/{{{{ params.s3_etl_script_key }}}}/{script}",
                    ],
                },
            }
            for script in scripts_by_order
        ]

        step_adder = EmrAddStepsOperator(
            task_id="add_steps_to_emr_cluster",
            dag=dag,
            job_flow_id=f"{{{{ task_instance.xcom_pull(task_ids='{task_group_id}.create_emr_cluster', key='return_value') }}}}",
            aws_conn_id=aws_connection_id,
            steps=spark_steps,
            params=dict(
                BUCKET_NAME=s3_bucket_name,
                s3_dependencies=dependencies_s3_key,
                s3_configs=configs_s3_key,
                s3_etl_script_key=scripts_s3_key,
            ),
        )
        last_step = len(spark_steps) - 1

        step_checker = EmrStepSensor(
            task_id="watch_step",
            dag=dag,
            job_flow_id=f"{{{{ task_instance.xcom_pull(task_ids='{task_group_id}.create_emr_cluster', key='return_value') }}}}",
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='{task_group_id}.add_steps_to_emr_cluster', key='return_value')["
            + str(last_step)
            + "] }}",
            aws_conn_id=aws_connection_id,
        )

        terminate_emr_cluster = EmrTerminateJobFlowOperator(
            task_id="terminate_emr_cluster",
            dag=dag,
            job_flow_id=f"{{{{ task_instance.xcom_pull(task_ids='{task_group_id}.create_emr_cluster', key='return_value') }}}}",
            aws_conn_id=aws_connection_id,
        )

        (
            upload_scripts_to_S3
            >> create_emr_cluster
            >> step_adder
            >> step_checker
            >> terminate_emr_cluster
        )

    return submit_emr_job
