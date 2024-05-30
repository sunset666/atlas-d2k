#set airflow environment
AF_CONFIG=/hive/users/niddk/repositories/dev/ingest-pipeline/src/ingest-pipeline/airflow/airflow.cfg
AF_HOME=/hive/users/niddk/repositories/dev/ingest-pipeline/src/ingest-pipeline/airflow

AF_METHOD='conda'
AF_ENV_NAME="condaEnv_niddk_python_${NIDDK_PYTHON_VERSION}_dev"
