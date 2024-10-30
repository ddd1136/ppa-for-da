import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator


def editors():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.editors (id, username, first_name, last_name, email, isu_number)
        SELECT DISTINCT (json_array_elements(wp_list::json->'editors')::json->>'id')::integer as editor_id, 
            (json_array_elements(wp_list::json->'editors')::json->>'username') as username,
            (json_array_elements(wp_list::json->'editors')::json->>'first_name') as first_name,
            (json_array_elements(wp_list::json->'editors')::json->>'last_name') as last_name,
            (json_array_elements(wp_list::json->'editors')::json->>'email') as email,
            (json_array_elements(wp_list::json->'editors')::json->>'isu_number') as isu_number
        FROM stg.su_wp sw
        ON CONFLICT ON CONSTRAINT editors_uindex DO UPDATE 
        SET 
            username = EXCLUDED.username, 
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name, 
            email = EXCLUDED.email, 
            isu_number = EXCLUDED.isu_number;
        """)


def states():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        TRUNCATE dds.states RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.states (cop_state, state_name)
        WITH t AS (
            SELECT DISTINCT (json_array_elements(wp_in_academic_plan::json)->>'status') as cop_states FROM stg.work_programs wp
        )
        SELECT cop_states, 
            CASE 
                WHEN cop_states = 'AC' THEN 'одобрено' 
                WHEN cop_states = 'AR' THEN 'архив'
                WHEN cop_states = 'EX' THEN 'на экспертизе'
                WHEN cop_states = 'RE' THEN 'на доработке'
                ELSE 'в работе'
            END as state_name
        FROM t
        ON CONFLICT ON CONSTRAINT state_name_uindex DO UPDATE 
        SET 
            id = EXCLUDED.id, 
            cop_state = EXCLUDED.cop_state;
        """)


def units():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.units (id, unit_title, faculty_id)
        SELECT DISTINCT sw.fak_id, 
            sw.fak_title, 
            ud.faculty_id::integer
        FROM stg.su_wp sw 
        LEFT JOIN stg.up_description ud 
        ON sw.fak_title = ud.faculty_name 
        ON CONFLICT ON CONSTRAINT units_uindex DO UPDATE 
        SET 
            unit_title = EXCLUDED.unit_title, 
            faculty_id = EXCLUDED.faculty_id;
        """)


def up():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        TRUNCATE dds.up RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        INSERT INTO dds.up (app_isu_id, on_check, laboriousness, year, qualification, update_ts, unit)
        WITH up_desc AS (
            SELECT DISTINCT edu_program_name, faculty_name FROM stg.up_description
        ),
        up_temp AS (
            SELECT DISTINCT ON (up_det.ap_isu_id, DATE(up_det.update_ts)) up_det.ap_isu_id, 
                   up_det.on_check, 
                   up_det.laboriousness, 
                   element.value->>'year' AS year,
                   element.value->>'qualification' AS qualification,
                   element.value->'structural_unit'->>'title' AS unit,
                   element.value->>'title' AS title,
                   up_det.update_ts
            FROM stg.up_detail up_det
            CROSS JOIN LATERAL json_array_elements(up_det.academic_plan_in_field_of_study::json) AS element(value)
            WHERE up_det.ap_isu_id IS NOT NULL
        )
         SELECT DISTINCT ON (up_temp.ap_isu_id, DATE(up_temp.update_ts)) up_temp.ap_isu_id, 
               up_temp.on_check, 
               up_temp.laboriousness, 
               up_temp.year::integer, 
               up_temp.qualification, 
               up_temp.update_ts,
               COALESCE(up_temp.unit, up_desc.faculty_name) AS unit
        FROM up_temp
        LEFT JOIN up_desc ON up_temp.title = up_desc.edu_program_name;


        """
    )


def wp():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        TRUNCATE dds.wp RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.wp (wp_id, discipline_code, wp_title, wp_status, unit_id, wp_description, update_ts)
        WITH wp_desc AS (
            SELECT DISTINCT 
                (json_array_elements(wp_in_academic_plan::json)->>'id')::integer as wp_id,
                json_array_elements(wp_in_academic_plan::json)->>'discipline_code' as discipline_code,
                json_array_elements(wp_in_academic_plan::json)->>'description' as wp_description,
                json_array_elements(wp_in_academic_plan::json)->>'status' as wp_status,
                valid_from::date as update_date -- Используем только дату без учета времени
            FROM stg.work_programs wp
        ),
        wp_unit AS (
            SELECT 
                fak_id,
                (wp_list::json->>'id')::integer as wp_id,
                wp_list::json->>'title' as wp_title,
                wp_list::json->>'discipline_code' as discipline_code
            FROM stg.su_wp sw
        ),
        combined AS (
            SELECT DISTINCT 
                wp_desc.wp_id, 
                wp_desc.discipline_code,
                wp_unit.wp_title,
                s.id as wp_status, 
                wp_unit.fak_id as unit_id,
                wp_desc.wp_description,
                wp_desc.update_date,
                ROW_NUMBER() OVER (PARTITION BY wp_desc.wp_id, wp_desc.update_date ORDER BY wp_desc.update_date DESC) AS row_num
            FROM wp_desc
            LEFT JOIN wp_unit
            ON wp_desc.discipline_code = wp_unit.discipline_code
            LEFT JOIN dds.states s 
            ON wp_desc.wp_status = s.cop_state
        )
        SELECT wp_id, discipline_code, wp_title, wp_status, unit_id, wp_description, update_date AS update_ts
        FROM combined
        WHERE row_num = 1;
        """
    )



def wp_inter():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        TRUNCATE dds.wp_editor RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.wp_editor (wp_id, editor_id, update_ts)
        SELECT 
            (wp_list::json->>'id')::integer AS wp_id,
            (json_array_elements(wp_list::json->'editors')::json->>'id')::integer AS editor_id,
            wp.update_ts
        FROM 
            stg.su_wp
        JOIN 
            (
                SELECT wp_id, MAX(update_ts) AS update_ts
                FROM dds.wp
                GROUP BY wp_id
            ) AS wp
        ON 
            (wp_list::json->>'id')::integer = wp.wp_id
        WHERE 
            (wp_list::json->>'id')::integer IN (SELECT wp_id FROM dds.wp);
        """
    )

with DAG(dag_id='stg_to_dds', start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Moscow"), schedule_interval='0 4 * * *', catchup=False) as dag:
    t1 = PythonOperator(
        task_id='editors',
        python_callable=editors
    )

    t2 = PythonOperator(
        task_id='states',
        python_callable=states
    )

    t3 = PythonOperator(
        task_id='units',
        python_callable=units
    )

    t4 = PythonOperator(
        task_id='up',
        python_callable=up
    )

    t5 = PythonOperator(
        task_id='wp',
        python_callable=wp
    )

    t6 = PythonOperator(
        task_id='wp_inter',
        python_callable=wp_inter
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
