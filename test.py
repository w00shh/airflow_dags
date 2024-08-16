from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

# 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# DAG 정의
dag = DAG(
    'pyspark_test_dag',
    default_args=default_args,
    description='test for DAG using pyspark',
    schedule_interval=None,  # 스케줄링을 비활성화하고 수동으로 실행
    catchup=False,
)

# 시작 태스크
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# PySpark 작업을 실행할 SparkKubernetesOperator
spark_task = SparkKubernetesOperator(
    task_id='spark_pi_task',
    namespace='airflow',  # Kubernetes 네임스페이스를 지정
    application_file='spark-pi.yaml',  # PySpark 작업을 정의하는 YAML 파일 경로
    kubernetes_conn_id='k8s_default',  # Airflow의 Kubernetes 연결 ID
    do_xcom_push=True,  # 작업 결과를 XCom에 푸시
    dag=dag,
)

# 종료 태스크
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# 태스크 순서 정의
start >> spark_task >> end