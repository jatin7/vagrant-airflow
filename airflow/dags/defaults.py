import urllib
#import configparser

from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator


from MSTeams.ms_teams_webhook_operator import MSTeamsWebhookOperator
from datetime import timedelta, datetime
from typing import Sequence, Any, Dict


##############################################################################
#Not sure if this is a good idea?
############################################################################## 
#config = configparser.ConfigParser()
#config.read('/opt/airflow/airflow.conf')

environment_conf = 'stg'
default_packages = 'com.typesafe:config:1.4.0,io.delta:delta-core_2.12:2.4.0'
remote_one_package = '/home/vagrant/repos/spark-apps/spark-datapipelines-one_2.12-1.0.jar'
remote_salesforce_package = '/home/vagrant/libs/spark-salesforce-1.1.5.jar'
config_file_path = '/home/vagrant/repos/spark-apps/application.json'


teams_fail_conn_id = {
    'Data Team': 'msteams_webhook_url'
}
teams_success_conn_id = {
}


def add_conn_id(conn_type, conn_name, conn_id):
    if conn_type == 'success':
        teams_success_conn_id[conn_name] = conn_id
    elif conn_type == 'failure':
        teams_fail_conn_id[conn_name] = conn_id


def add_live_conn_id():
    teams_fail_conn_id['Live Data Team'] = 'live_msteams_webhook_url'


def on_failure(context):
    for conn in teams_fail_conn_id.values():
        ms_teams_webhook(context, conn)


def on_success(context):
    for conn in teams_success_conn_id.values():
        ms_teams_webhook(context, conn, 'success')

def ms_teams_webhook(context, conn_id='msteams_webhook_url', status='failed'):
    dag_id = context['dag_run'].dag_id

    task_id = context['task_instance'].task_id

    context['task_instance'].xcom_push(key=dag_id, value=True)

    logs_url = "http://192.168.2.21:8080/admin/airflow/log?dag_id={}&task_id={}&execution_date={}".format(
        dag_id, task_id, urllib.parse.quote_plus(context['ts']))

    theme_colour = 'FF0000'

    if status == 'success':
        theme_colour = '00FF00'

    teams_notification = MSTeamsWebhookOperator(
        task_id="msteams_notify_failure", trigger_rule="all_done",
        message="`{}` has {} on task: `{}`".format(dag_id, status, task_id),
        button_text="View log", button_url=logs_url,
        theme_color=theme_colour, http_conn_id=conn_id)
    teams_notification.execute(context)


default_args = {
    'on_failure_callback': on_failure,
    'on_success_callback': on_success,
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}


def combine_with_defaults(other_args):
    default_args_copy = default_args.copy()
    default_args_copy.update(other_args)
    return default_args_copy


def spark_task_factory(task_id: str, name: str, application: str, java_class_name: str, dag_param, wait: bool = True,
                       application_args=None, packages: str = 'com.typesafe:config:1.4.0', jars: Sequence[str] = None, timeout: str = 'true',
                       max_num_executors=2, driver_memory=1, executor_memory=1, total_executor_cores: int = 2,
                       driver_memory_overhead=1, executor_memory_overhead=1,
                       default_parallelism: int = 100, shuffle_partitions: int = 100,
                       live_dag: bool = False, additional_config:Dict[str, Any]=None):
    conf = {
        'spark.yarn.submit.waitAppCompletion': wait,
        'spark.driver.memoryOverhead': str(driver_memory_overhead) + 'g',
        'spark.task.reaper.enabled': timeout,
        'spark.task.reaper.killTimeout': '3h',
        'spark.driver.extraJavaOptions': '-Dconfig.file=application.json',
        'spark.executor.extraJavaOptions': '-Dconfig.file=application.json',
        'spark.default.parallelism': default_parallelism,
        'spark.sql.shuffle.partitions': shuffle_partitions,
        'park.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
    }

    if additional_config is not None:  
        conf.update(additional_config)

    if live_dag:
        add_conn_id('failure', 'Live Data Team', 'live_msteams_webhook_url')
    if max_num_executors is not None:
        conf['spark.dynamicAllocation.maxExecutors'] = max_num_executors
    if executor_memory_overhead is not None:
        conf['spark.executor.memoryOverhead'] = str(executor_memory_overhead) + 'g'

    conf['spark.jars.packages'] = packages
    polling_interval = 0
    if wait:
        polling_interval = 30
    return LivyOperator(
        task_id=task_id,
        name=name + '_{{ run_id }}_' + str(datetime.now().timestamp()),
        file='file://' + application,
        files=['file://' + config_file_path],
        jars=jars,
        conf=conf,
        class_name=java_class_name,
        args=application_args,
        executor_memory=str(executor_memory) + 'g',
        driver_memory=str(driver_memory) + 'g',
        executor_cores=total_executor_cores,
        proxy_user='vagrant',
        dag=dag_param,
        polling_interval=polling_interval,
        execution_timeout=timedelta(days=1))


def hive_task_factory(task_id, script_path, dag_param, hive_cli_conn_id='beeline_default'):
    return HiveOperator(
        task_id='hive_' + task_id,
        hql=script_path,
        hive_cli_conn_id=hive_cli_conn_id,
        hiveconfs={
            'hive.support.concurrency': True,
            'hive.enforce.bucketing': True,
            'hive.exec.dynamic.partition.mode': 'nonstrict',
            'hive.txn.manager': 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager',
            'hive.compactor.initiator.on': True,
            'hive.compactor.worker.threads': 1,
            'hive.exec.max.dynamic.partitions': 1000,
            'hive.exec.max.dynamic.partitions.pernode': 1000
        },
        dag=dag_param
    )
