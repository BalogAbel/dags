from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
import os
import boto3
from botocore.exceptions import ClientError



def upload_to_s3(s3_endpoint,access_key,secretkey,source_folder,target_bucket, prefix=None):

    # initialize connection
    s3_client = boto3.client('s3', endpoint_url=s3_endpoint,aws_access_key_id=access_key , aws_secret_access_key=secretkey)

    
    for file in os.listdir(source_folder):
        try:
            filename_path = (os.path.join(source_folder,file))
            print(filename_path)
            response = s3_client.upload_file(filename_path, target_bucket,prefix+file)
            return True
        except ClientError as e:
            print(e)
            return False



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['ktomi88@gmail.com'],
    'email_on_failure':  False,
    'email_on_retry':  False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    'TestDAG',
    default_args=default_args,
    description='Test job',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['test'],
) as dag:

    ##upload_files = PythonOperator( 
    ##task_id='Pythpn_upload_files_local_to_s3',
    ##python_callable= upload_to_s3,
    ##provide_context=True,
    ##op_kwargs={"s3_endpoint":"{{ var.value.s3_endpoint }}","access_key":"{{ var.value.s3_access_key }}","secretkey": "{{ var.value.s3_secret_key }}","source_folder": "{{ var.value.tmp_folder }}","target_bucket":"spark", "prefix":"test_data/"},
    ##) 

    spark_1 = SparkSubmitOperator( 
    task_id='Run_yellow_cab_job' ,
    conn_id='Spark_conn',
    application="{{ conf.core.dags_folder }}/spark_codes/test_spark.py",
    packages="org.apache.hadoop:hadoop-aws:3.2.0",
    name='yellow_cab_test',
    executor_cores=4,
    executor_memory='5g',
    driver_memory='5g',
    conf={ "fs.s3a.access.key":"B2JDY11NHXLI77PHSX4D","fs.s3a.secret.key":"XBbgD4eM8Su2B7AZVyTe4hKY2IR1Oz05QYYEvCaD" ,"fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem","fs.s3a.endpoint":"http://s3-rook-ceph.apps.okdpres.alerant.org.uk","fs.s3a.connection.ssl.enabled":"false","fs.s3a.path.style.access":"true"},
    )
 
    ##spark_1 << upload_files
