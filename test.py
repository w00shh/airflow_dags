from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='pyspark_on_k8s',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    run_spark_pi = KubernetesPodOperator(
        task_id='run_spark_pi',
        name='spark-pi-job',
        namespace='airflow',
        image='bitnami/spark:latest',
        cmds=['spark-submit', '--master', 'k8s://10.96.0.1:443',
              '--deploy-mode', 'cluster',
              '--class', 'org.apache.spark.examples.SparkPi',
              'file:///opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.2.jar'],
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
    )

    run_spark_pi