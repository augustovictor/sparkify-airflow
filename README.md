# sparkify-airflow

## Connection

Conn Id | Conn Type | Host | Schema | Login | Password | Port | Extra
--------|-----------|------|--------|-------|----------|------|-------
aws_credentials | Amazon Web Services | - | - | <aws_access_key_id> | <aws_secret_access_key> | - | -
redshift | Postgres | <cluster_endpoint> | <schema> | <master_user_name> | <password> | 5439 | -

OK The DAG does not have dependencies on past runs
OK On failure, the task are retried 3 times
OK Retries happen every 5 minutes
Catchup is turned off
OK Do not email on retry

## TODO
- [ ] Dag's doc