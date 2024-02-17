from datetime import datetime, timedelta

from airflow.models import DAG, BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensor,
)
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="airplane_raw_data_pipeline",
    start_date=datetime(2024, 2, 14),
    schedule="@weekly",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "azure-data-factory",
        "factory_name": "airplane-df",
        "resource_group_name": "ads507-team6",
    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="download_incident_data") as group1pipeline:
        run_group1_pipeline1 = AzureDataFactoryRunPipelineOperator(
            task_id="run_group1_pipeline1", pipeline_name="...."
        )
        run_group1_pipeline2 = AzureDataFactoryRunPipelineOperator(
         task_id="run_group1_pipeline2", pipeline_name="...."
        )
        run_group1_pipeline1 >> run_group1_pipeline2

    with TaskGroup(group_id="download_accident_data") as group2pipeline:
        run_group2_pipeline1 = AzureDataFactoryRunPipelineOperator(
            task_id="run_group2_pipeline1", pipeline_name="...."
        )
        run_group2_pipeline2 = AzureDataFactoryRunPipelineOperator(
            task_id="run_group2_pipeline2",
            pipeline_name="....",
            wait_for_termination=False,
        )
        pipeline_run_sensor = AzureDataFactoryPipelineRunStatusSensor(
            task_id="pipeline_run_sensor",
            run_id=run_group2_pipeline2.output["run_id"],
            poke_interval=3,
        )
        [run_group2_pipeline1, pipeline_run_sensor]

    with TaskGroup(group_id="download_operator_risk_data") as group3pipeline:
        run_group3_pipeline1 = AzureDataFactoryRunPipelineOperator(
            task_id="run_group3_pipeline1", pipeline_name="...."
        )
        run_group3_pipeline2 = AzureDataFactoryRunPipelineOperator(
            task_id="run_group3_pipeline2",
            pipeline_name="....",
            wait_for_termination=False,
        )
        pipeline_run_sensor_group3 = AzureDataFactoryPipelineRunStatusSensor(
            task_id="pipeline_run_sensor_group3",
            run_id=run_group3_pipeline2.output["run_id"],
            poke_interval=3,
        )
        [run_group3_pipeline1, pipeline_run_sensor_group3]

    begin >> [group1pipeline, group2pipeline, group3pipeline] >> end