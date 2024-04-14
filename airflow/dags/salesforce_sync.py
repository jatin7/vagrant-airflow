from datetime import datetime
from airflow.models import DAG, TaskInstance
from airflow.utils.db import provide_session 
from defaults import combine_with_defaults, spark_task_factory, remote_one_package, remote_salesforce_package, environment_conf, default_packages

default_args = combine_with_defaults({
    'owner': 'jack.fawcett',
    'retries': 0
})

@provide_session
def previous_successful_dag_start_date(dag_param, task_id, execution_date, session=None):
    dag_run = dag_param.get_dagrun(execution_date)
    previous_successful_dag_run = dag_run.get_previous_dagrun(dag_run, state='success')

    if previous_successful_dag_run is None:
        return None

    previous_successful_ti = (  
        session.query(TaskInstance)  
        .filter(  
            TaskInstance.dag_id == dag_param.dag_id,  
            TaskInstance.task_id == task_id,  
            TaskInstance.state == 'success',  
            TaskInstance.execution_date < execution_date  
        )   
        .first()  
    )

    if previous_successful_ti is None:
        return None

    return previous_successful_dag_run.start_date

dag = DAG(
    dag_id='salesforce_sync_dag',
    default_args=default_args,
    template_searchpath=['/home/airflow/repos/airflow/hive-scripts/'],
    schedule_interval='59 * * * *',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 4, 12),
    user_defined_macros={
        'previous_successful_dag_start_date': previous_successful_dag_start_date
    }
)

package_list = default_packages +  ',com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.13.0,com.fasterxml.jackson.core:jackson-core:2.13.0,com.fasterxml.jackson.core:jackson-databind:2.13.0'

city_task = spark_task_factory(
    task_id='city_salesforce_spark',
    name='city_salesforce_spark',
    application=remote_one_package,
    java_class_name='com.ef.edtech.kt.GenericSalesforceJdbcDeltaSync',
    dag_param=dag,
    application_args=[environment_conf, 'salesforce.City__c', '{{ previous_successful_dag_start_date(dag, "city_salesforce_spark", execution_date) }}', '{{ dag_run.start_date }}'],
    packages=package_list,
    jars=['file://' + remote_salesforce_package],
    driver_memory=1,
    driver_memory_overhead=2,
    executor_memory=2,
    executor_memory_overhead=2)

employee_task = spark_task_factory(
    task_id='employee_salesforce_spark',
    name='employee_salesforce_spark',
    application=remote_one_package,
    java_class_name='com.ef.edtech.kt.GenericSalesforceJdbcDeltaSync',
    dag_param=dag,
    application_args=[environment_conf, 'salesforce.Employee__c', '{{ previous_successful_dag_start_date(dag, "employee_salesforce_spark", execution_date) }}', '{{ dag_run.start_date }}'],
    packages=package_list,
    jars=['file://' + remote_salesforce_package])