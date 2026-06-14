import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQClXz5RGQmW7CgR1lgFdbIKMTxmLAzRdGMrGz/eBdVRaEcIgI6lZTH+ozsVohLSmDmZkyU+BjVvvuBO1vIzFfzRACuV8Rp9Ob2IZTyCBGeoYEjj5w+69n8lbuHqqZWkP62sty4CXwgpsjArI1D0REbM7mBHCTLOqFN1Kwv5hStgE7noVp+wCsbm+ZEx+Qma+UIB66PlJpvpnXZzDk9sgRfrS6PN5TWk2XpoQk9vpkcecha0uGzpBo5sBpLALOb11LdSMRR1fRDb232Sar7Rut4c2ztMtc0dmDBzYLnuU2PV52eyzHDULO0dLp2kE6mMZGQ+vFR1I0NhQpgvtAHZW8XZ'
YC_DP_SUBNET_ID = 'e9b8bssmdmi5uvt3dssd'
YC_DP_SA_ID = 'ajeq6pv6uf1cl8u8mtjl'
YC_BUCKET = 'bucket-managed-airflow'

with DAG(
        'DATA_INGEST',
        schedule='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-hdd',
        masternode_disk_size=40,
        computenode_resource_preset='s2.small',
        computenode_disk_type='network-hdd',
        computenode_disk_size=40,
        computenode_count=1,
        computenode_max_hosts_count=2,
        services=['YARN', 'SPARK'],
        datanode_count=0,
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/create-table.py',
    )

    poke_spark_cashback = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-cashback-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/calc-cashback.py',
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> poke_spark_cashback >> delete_spark_cluster
