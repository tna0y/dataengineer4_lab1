import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import dateutil.parser
import json
from datetime import datetime
import elasticsearch
import csv
import unicodedata
from urllib.parse import urlparse
import tempfile
import subprocess
import clickhouse_driver


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 13, 22, 0, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG('es-hdfs-ch-dag-2', default_args=default_args, schedule_interval=timedelta(minutes=15))


def load_from_esearch(**kwargs):
    ts_end = dateutil.parser.parse(kwargs['ts'])
    ts_start = ts_end - timedelta(minutes=15)
    es = elasticsearch.Elasticsearch(["node-1:9200"])
    res = es.search(index="maxim.kochukov", body={"query": {
         "range" : {
         "@timestamp" :{
             "gte" : ts_start.isoformat(),
             "lt" : ts_end.isoformat(),
        }
    }}
    })['hits']['hits']
    res = list(map(lambda x: json.loads(x['_source']['message']), res))
    return {'ts_start': ts_start, 'ts_end': ts_end, 'res': res}

def save_to_hdfs(**kwargs):
    ti = kwargs['ti']
    v = ti.xcom_pull(task_ids='load_from_esearch')
    tmpfile = tempfile.mktemp()
    with open(tmpfile, 'w') as f:
        f.write(json.dumps(v['res']))
    new_filename = '/click_data/{}_{}.json'.format(int(v['ts_start'].timestamp()), int(v['ts_end'].timestamp()))
    subprocess.check_call(['hdfs', 'dfs', '-put', '-f', tmpfile, new_filename])
    os.remove(tmpfile)
    return new_filename

def save_to_clickhouse(**kwargs):
    ti = kwargs['ti']
    j = ti.xcom_pull(task_ids='load_from_esearch')['res']
    j = [dict((k, int(v) if isinstance(v, bool) else v) for k, v in _.items()) for _ in j]    
    client = clickhouse_driver.Client('Localhost')
    client.execute('SET input_format_skip_unknown_fields=1;')
    client.execute('INSERT INTO clicks VALUES', j)


es = PythonOperator(
    task_id='load_from_esearch',
    provide_context=True,
    python_callable=load_from_esearch,
    dag=dag,
)

hdfs = PythonOperator(
    task_id='save_to_hdfs',
    provide_context=True,
    python_callable=save_to_hdfs,
    dag=dag
)

ch = PythonOperator(
    task_id='save_to_clickhouse',
    provide_context=True,
    python_callable=save_to_clickhouse,
    dag=dag
)

[hdfs, ch] << es
