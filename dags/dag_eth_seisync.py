from __future__ import print_function

import datetime

from airflow import models
from airflow.operators import python_operator

from scripts.eth_seisync import sync

default_dag_args = {
    'start_date': datetime.datetime(2019, 12, 30),
}

netfile_login = models.Variable.get('netfile_login')
netfile_password = models.Variable.get('netfile_password')
socrata_keyId = models.Variable.get('socrata_keyId')
socrata_keySecret = models.Variable.get('socrata_keySecret')
socrata_appToken = models.Variable.get('socrata_appToken')
credentials = {
    "netfile":{
        "login": netfile_login,
        "password": netfile_password
    },
    "socrata":{
        "appToken": socrata_appToken,
        "keyId": socrata_keyId,
        "keySecret": socrata_keySecret
    }
}
socrata_config_redacted = models.Variable.get('socrata_config_redacted', deserialize_json=True)
socrata_config_unredacted = models.Variable.get('socrata_config_unredacted', deserialize_json=True)
schema_defs = models.Variable.get('schema_defs', deserialize_json=True)

redacted_kwargs = {
    'credentials': credentials,
    'socrata_config': socrata_config_redacted,
    'schema_defs': schema_defs,
    'get_unredacted': False
    }
unredacted_kwargs = {
    'credentials': credentials,
    'socrata_config': socrata_config_unredacted,
    'schema_defs': schema_defs,
    'get_unredacted': True
    }

with models.DAG(
        'eth_update_sei',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    redacted = python_operator.PythonOperator(
        task_id='get_redacted',
        python_callable=sync.run_sync,
        op_kwargs=redacted_kwargs,
        )
    unredacted = python_operator.PythonOperator(
        task_id='get_unredacted',
        python_callable=sync.run_sync,
        op_kwargs=unredacted_kwargs,
        )

    redacted >> unredacted
