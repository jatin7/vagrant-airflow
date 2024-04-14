"""
# DAG DE LIMPEZA

Limpeza dos logs das dags na pasta

$AIRFLOW_HOME/logs/scheduler

sao os logs mais pesados do sistema e precisam ser expurgados para evitar que o servidor fique
cheio
"""

import os
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

default_args = {
    'start_date': datetime(2021, 12, 6, 0, 0, 0),
    'depends_on_past': False,
}

dag = DAG(
    dag_id='dag_limpeza_logs',
    default_args=default_args,
    schedule_interval='@daily',
    # schedule_interval=None,
    catchup=False,
    tags=['LIMPEZA','airflow-maintenance-dags']
)

dag.doc_md = __doc__


def python_limpeza(**kwargs):
    airflo_home = Variable.get("AIRFLOW_HOME")
    logs_day = int(Variable.get("LOGS_SCHEDULER"))
    diretorio = airflo_home + '/logs/scheduler'
    pastas = []

    # Percorre todos os arquivos e pastas no diretório
    for arquivo in os.listdir(diretorio):
        # Verifica se é uma pasta
        if os.path.isdir(os.path.join(diretorio, arquivo)):
            pastas.append(arquivo)
    print(f"Diretorios: {pastas}")  # lista todas as pastas

    # o days=n sao o limite de dias de logs
    data_ref = (datetime.today() - timedelta(days=logs_day)).strftime('%Y-%m-%d')
    for date in pastas:
        if date < data_ref:
            comando_remove = f"rm -rf {diretorio}/{date}"
            print(comando_remove)
            os.system(comando_remove)


t1 = PythonOperator(
    task_id='python_limpeza',
    python_callable=python_limpeza,
    dag=dag,
)

t1
