import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator


def up():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
       CREATE SCHEMA IF NOT EXISTS cdm;
            """)

    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        CREATE TABLE IF NOT EXISTS cdm.up (
            app_isu_id integer, 
            on_check TEXT, 
            laboriousness integer, 
            year integer, 
            qualification varchar(20), 
            update_ts timestamp,
            unit TEXT);
            """)

    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
       TRUNCATE CDM.up RESTART IDENTITY CASCADE;
            """)

    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """ 
        INSERT INTO cdm.up
        SELECT DISTINCT
        app_isu_id,
        CASE
        WHEN on_check = 'verified' THEN 'одобрено'
        ELSE 'в работе'
        END AS on_check,
        laboriousness,
        year,
        CASE
        WHEN qualification = 'master' THEN 'магистратура'
        WHEN qualification = 'bachelor' THEN 'бакалавриат'
        WHEN qualification = 'specialist' THEN 'специалитет'
        ELSE qualification
        END AS qualification,
        update_ts,
        unit
        FROM dds.up
        WHERE year = 2023 or year = 2024;
            """)


def up_actual_verified():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        CREATE TABLE IF NOT EXISTS cdm.up_actual_verified (
            status TEXT, 
            qualification varchar(20),
            year integer,
            unit TEXT, 
            update_ts timestamp);
            """)

    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
       TRUNCATE CDM.up_actual_verified RESTART IDENTITY CASCADE;
            """)

    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """ 
        INSERT INTO cdm.up_actual_verified
        WITH LatestUpdate AS (
            SELECT MAX(DATE(update_ts)) AS latest_update_date
            FROM cdm.up
        ),
        Filtered AS (
            SELECT *
            FROM cdm.up, LatestUpdate
            WHERE DATE(cdm.up.update_ts) = LatestUpdate.latest_update_date
        )
        SELECT 
            on_check as status,
            qualification,
            year,
            unit, 
            update_ts
        FROM 
            Filtered
        GROUP BY
             on_check, qualification, update_ts, year, unit
        ORDER BY
            on_check ASC;
            """)


def up_actual_lab():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        CREATE TABLE IF NOT EXISTS cdm.up_actual_lab (
            laboriousness_status TEXT, 
            qualification varchar(20), 
            year integer,
            unit text, 
            update_ts timestamp);
            """)

    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
       TRUNCATE CDM.up_actual_lab RESTART IDENTITY CASCADE;
            """)

    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """ 
        INSERT INTO cdm.up_actual_lab
        WITH LatestUpdate AS (
            SELECT MAX(DATE(update_ts)) AS latest_update_date
            FROM cdm.up
        ),
        Filtered AS (
            SELECT *
            FROM cdm.up, LatestUpdate
            WHERE DATE(cdm.up.update_ts) = LatestUpdate.latest_update_date
        )
        SELECT 
            CASE
                WHEN (
                    laboriousness = 240 AND qualification = 'бакалавриат'
                )
                OR (
                    laboriousness = 120 AND qualification = 'магистратура'
                ) THEN 'Корректно'
                ELSE 'Некорректно'
            END AS laboriousness_status,
            qualification,
            year, 
            unit,
            update_ts
        FROM 
            Filtered
        GROUP BY
            laboriousness_status, qualification, update_ts, year, unit
        ORDER BY
            laboriousness_status ASC;
            """)


def wp():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        CREATE TABLE IF NOT EXISTS cdm.wp_statuses_aggregation (
            wp_id INTEGER,
            update_ts TIMESTAMP,
            state_name TEXT,
            wp_description TEXT,
            editor TEXT,
            unit TEXT  
        );
        """
    )

    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
       TRUNCATE CDM.wp_statuses_aggregation RESTART IDENTITY CASCADE;
            """)

    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
            INSERT INTO cdm.wp_statuses_aggregation
            SELECT
                wp_id,
                update_ts,
                st.state_name,
                wp_description,
                      COALESCE(
                    (
                        SELECT STRING_AGG(CONCAT(ed.first_name, ' ', ed.last_name, ' (', ed.isu_number, ')'), ', ')
                        FROM dds.wp_editor we
                        INNER JOIN dds.editors ed ON we.editor_id = ed.id
                        WHERE we.wp_id = wp.wp_id
                    ),
                    'неизвестно'
                ) AS editor,
                   (
                    SELECT u.unit_title
                    FROM dds.units u
                    WHERE wp.unit_id = u.id
                ) AS unit  
            FROM
                dds.wp
                INNER JOIN dds.states st ON dds.wp.wp_status = st.id;
        """)


with DAG(dag_id='dds_to_cdm', start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Moscow"),
         schedule_interval='0 4 * * *', catchup=False) as dag:
    t1 = PythonOperator(
        task_id='up',
        python_callable=up
    )

    t2 = PythonOperator(
        task_id='wp',
        python_callable=wp
    )

    t3 = PythonOperator(
        task_id='up_actual_verified',
        python_callable=up_actual_verified
    )

    t4 = PythonOperator(
        task_id='up_actual_lab',
        python_callable=up_actual_lab
    )

    t1 >> t2 >> t3 >> t4
