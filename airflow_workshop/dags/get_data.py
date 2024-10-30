import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

url = "https://op.itmo.ru/auth/token/login"
username = Variable.get ("username")
password = Variable.get ("password")
auth_data = {"username": username, "password": password}

token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {'Content-Type': "application/json", 'Authorization': "Token " + token}

def get_practice():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate stg.practice  restart identity cascade;
    """)
    url_down = 'https://op.itmo.ru/api/practice/?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)['count']
    for p in range(1,c//10+2):
        url_down = 'https://op.itmo.ru/api/practice/?format=json&page=' + str(p)
        # dt = pendulum.now().to_iso8601_string()
        page = requests.get(url_down, headers=headers)
        res = json.loads(page.text)['results']
        for r in res:
            df = pd.DataFrame([r], columns=r.keys())
            # df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
            # df['wp_in_academic_plan'] = df[~df['wp_in_academic_plan'].isna()]["wp_in_academic_plan"].apply(lambda st_dict: json.dumps(st_dict))
            # df.loc[:, 'update_ts'] = dt
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.practice', df.values, target_fields = df.columns.tolist())

def get_wp_descriptions():
    # нет учета времени, просто удаляем все записи
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate stg.work_programs  restart identity cascade;
    """)
    # target_fields = ['id', 'academic_plan_in_field_of_study', 'wp_in_academic_plan']
    url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)['count']
    for p in range(1,c//10+2):
        url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=' + str(p)
        dt = pendulum.now().to_iso8601_string()
        page = requests.get(url_down, headers=headers)
        if page.status_code != 200:
            print(f"Ошибка при получении данных с страницы {p}: {page.status_code}")
            continue
        try:
            data = page.json()  # Использовать page.json(), это более надежный способ работы с JSON
            if 'results' not in data:
                print(f"Нет данных на странице {p}.")
                continue
            res = data['results']
            for r in res:
                df = pd.DataFrame([r], columns=r.keys())
                df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
                df['wp_in_academic_plan'] = df[~df['wp_in_academic_plan'].isna()]["wp_in_academic_plan"].apply(lambda st_dict: json.dumps(st_dict))
                PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.work_programs', df.values, target_fields = df.columns.tolist())
        except ValueError:
            print(f"Ошибка JSON на странице {p}.")
            continue
        

def get_structural_units():
    # нет учета времени, просто удаляем все записи
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate stg.su_wp  restart identity cascade;
    """)
    url_down = 'https://op.itmo.ru/api/record/structural/workprogram'
    target_fields = ['fak_id', 'fak_title', 'wp_list']
    page = requests.get(url_down, headers=headers)
    res = list(json.loads(page.text))
    for su in res:
        df = pd.DataFrame.from_dict(su)
        # превращаем последний столбец в json
        df['work_programs'] = df[~df['work_programs'].isna()]["work_programs"].apply(lambda st_dict: json.dumps(st_dict))
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.su_wp', df.values, target_fields = target_fields)

def get_online_courses():
    # нет учета времени, просто удаляем все записи
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate stg.online_courses  restart identity cascade;
    """)
    target_fields = ['id', 'title', 'institution', 'topic_with_online_course']
    url_down = 'https://op.itmo.ru/api/course/onlinecourse/?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)['count']
    for p in range(1,c//10+2):
        print (p)
        url_down = 'https://op.itmo.ru/api/course/onlinecourse/?format=json&page=' + str(p)
        page = requests.get(url_down, headers=headers)
        res = json.loads(page.text)['results']
        for r in res:
            df = pd.DataFrame([r], columns=r.keys())
            df = df[['id', 'title', 'institution', 'topic_with_online_course']]
            df['institution'] = df[~df['institution'].isna()]["institution"].apply(lambda st_dict: json.dumps(st_dict))
            df['topic_with_online_course'] = df[~df['topic_with_online_course'].isna()]["topic_with_online_course"].apply(lambda st_dict: json.dumps(st_dict))
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.online_courses', df.values, target_fields = target_fields)

def get_disc_by_year():
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    with t as (
    select 
    (json_array_elements(academic_plan_in_field_of_study::json)->>'ap_isu_id') :: integer as isu_id,
    id
    from stg.work_programs wp)
    select id from t
    where isu_id in
    (
    select id from stg.up_description ud 
    where ((training_period = '2') and (selection_year > '2020'))
    or ((training_period = '4') and (selection_year > '2018'))
    ) and
    id < 7256
    order by id
    """)
    for up_id in ids:
        up_id = str(up_id[0])
        print (up_id)
        url = 'https://op.itmo.ru/api/record/academicplan/get_wp_by_year/' + up_id + '?year=2022/2023'
        page = requests.get(url, headers=headers)
        df = pd.DataFrame.from_dict(page.json())
        df['work_programs'] = df[~df['work_programs'].isna()]["work_programs"].apply(lambda st_dict: json.dumps(st_dict))
        if len(df)>0:
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.disc_by_year', df.values, target_fields=df.columns.tolist())

#id  >= 7220
def get_up_detail():
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
        """
        select id as op_id
        from stg.work_programs wp
        where id > 7220
        order by 1
        """)
    existing_data = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").get_records(
        """
        SELECT id, ap_isu_id, on_check, laboriousness, academic_plan_in_field_of_study, update_ts
            FROM stg.up_detail
        """)

    url_down = 'https://op.itmo.ru/api/academicplan/detail/'
    target_fields = ['id', 'ap_isu_id', 'on_check', 'laboriousness', 'academic_plan_in_field_of_study', 'update_ts']
    df_existing_data = pd.DataFrame(existing_data, columns=target_fields)
    for op_id in ids:
        op_id = str(op_id[0])
        print(op_id)
        dt = pendulum.now("Europe/Moscow").to_iso8601_string()
        url = url_down + op_id + '?format=json'
        page = requests.get(url, headers=headers)
        df = pd.DataFrame.from_dict(page.json(), orient='index')
        df = df.T
        df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()][
            "academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
        df.loc[:, "update_ts"] = dt
        df = df[target_fields]

        # Insert the new entry with an updated update_ts (SCD2)
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.up_detail',
                                                                             df.values,
                                                                             target_fields=target_fields)

# "id": 6796,
# "ap_isu_id": 10572,
# "on_check": "in_work",
# "laboriousness": 393,
# "academic_plan_in_field_of_study": [
#         {
#             "id": 6859,
#             "year": 2018,
#             "qualification": "bachelor",
#             "title": "Нанофотоника и квантовая оптика",
#             "field_of_study": [
#                 {
#                     "number": "16.03.01",
#                     "id": 15772,
#                     "title": "Техническая физика",
#                     "qualification": "bachelor",
#                     "educational_profile": null,
#                     "faculty": null
#                 }
#             ],
#             "plan_type": "base",
#             "training_period": 0,
#             "structural_unit": null,
#             "total_intensity": 0,
#             "military_department": false,
#             "university_partner": [],
#             "editors": []
#         }

with DAG(dag_id='get_data', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 1 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_practice',
    python_callable=get_practice
    )
    t2 = PythonOperator(
    task_id='get_wp_descriptions',
    python_callable=get_wp_descriptions
    )
    t3 = PythonOperator(
    task_id='get_structural_units',
    python_callable=get_structural_units
    )
    t4 = PythonOperator(
    task_id='get_online_courses',
    python_callable=get_online_courses
    )
    t5 = PythonOperator(
    task_id='get_disc_by_year',
    python_callable=get_disc_by_year
    )
    t6 = PythonOperator(
    task_id='get_up_detail',
    python_callable=get_up_detail
    )

t1 >> t2 >> t3 >> t4 >> t5 >> t6
