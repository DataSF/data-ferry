# form700
## This ETL extracts and loads the SF Ethics Commission Form 700 Statement of Economic Data from API source.

### What is Form 700? Every elected official and public employee who makes or influences governmental decisions is required to submit a Statement of Economic Interest. Form 700 is a statement of economic interest. This is the ETL that moves the source data to the SF Open Data portal.

### To read more specifics about form 700, click [here](http://www.fppc.ca.gov/Form700.html)

## links for the final redacted datasets that this ETL produces:
* cover: http://data.sfgov.org/d/pm9d-idh4
* scheduleA1: http://data.sfgov.org/d/mwqh-x2wn
* scheduleA2: http://data.sfgov.org/d/64rb-55bi
* scheduleB: http://data.sfgov.org/d/9dv8-3432
* scheduleC: http://data.sfgov.org/d/u5rm-p23y
* scheduleD: http://data.sfgov.org/d/y9be-fypm
* scheduleE: http://data.sfgov.org/d/e67f-ux3j
* comments: http://data.sfgov.org/d/2ycd-bb2c


currently, gets all the data and does an upsert to socrata

[swagger docs for api](https://netfile.com/Connect2/api/swagger-ui/#!/public/Types)

get the following properties for each filing from the cover page api call:

* filer_name
* department_name
* position_name
* offices
* period_end
* period_start
* filing_date

## Tests
`python3 -m unittest netfile_client_unittest.Tests`

not a lot of coverage

## Sync
~~run:
`$ python3 runsync.py`~~

## on airflow
create composer environment

add pypi packages:

* composer > environment details > pypi packages > edit > add 'sodapy'

set variables in airflow server:

* credentials (secrets.json)
* socrata_config_redacted (fourfours for redacted datasets)
* socrata_config_unredacted (fourfours for unredacted datasets)
* schema_defs (schemas.json)

upload to "dags" folder in storage bucket:

* dag_runsync.py

upload to "dags/dependencies" folder in storage bucket:

* netfile_client.py
* sync.py

todo:

* tests
* put sync.py in container to be called by airflow?
* set logging level in airflow env to debug? 
