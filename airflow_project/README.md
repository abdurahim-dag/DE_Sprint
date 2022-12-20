**Задание**


**Общая информация.**

Local setup:
1. Setup venv ./airflow
2. Setup ENV AIRFLOW_HOME=~/airflow
3. pip install 'apache-airflow==2.5.0' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"
4. airflow db init
5. airflow webserver -p 8080
6. airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@a.a
7. airflow scheduler
8. airflow cheat-sheet | airflow connections list | airflow pools list | airflow variables list | airflow variables set key value | airflow dags unpause <dagname>
9. nano airflow/airflow.cfg

Airflow for prod:
- sqlite -> PG
- SequentialExecutor -> LocalExecutor, [CeleryExecutor, KubernetesExecutor]
- if CeleryExecutor: Сов всех WorkersCeleryExecutor должны быть доступны папка с дагами, плагинами и БД метаданных. 
  И так же возможно одинаковое окружение воркерах и версии airflow.  
- Логирование[Local, StatsD, ES, S3, ...]
- Systemd для управления процессами

Docker compose:
    docker compose up
Если у вас Linux/Unix а не Windows/MacOS, то необходимо в корень нашего проекта добавить UID в .env:
AIRFLOW_UID=<...>

Go to REST API...