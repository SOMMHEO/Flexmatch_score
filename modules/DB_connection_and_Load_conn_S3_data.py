import pandas as pd
import os
from dotenv import load_dotenv
import boto3
import io
import pymysql
import json
from datetime import datetime, timedelta
from sshtunnel import SSHTunnelForwarder

from sshtunnel import SSHTunnelForwarder

class SSHMySQLConnector:
    def __init__(self):
        self.ssh_host = None
        self.ssh_username = None
        self.ssh_password = None
        self.db_username = None
        self.db_password = None
        self.db_name = None
        self.tunnel = None
        self.connection = None

    def load_config_from_json(self, json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.ssh_host = config['ssh_host']
                self.ssh_username = config['ssh_username']
                self.ssh_password = config['ssh_password']
                self.db_username = config['db_username']
                self.db_password = config['db_password']
                self.db_name = config['db_name']
        except Exception as e:
            print("설정 JSON 로딩 실패:", e)

    def connect(self, insert=False):
        try:
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, 22),
                ssh_username=self.ssh_username,
                ssh_password=self.ssh_password,
                remote_bind_address=('127.0.0.1', 3306),
            )
            self.tunnel.start()
            # insert 여부에 따라 cursorclass 설정
            connect_kwargs = {
                'host': '127.0.0.1',
                'port': self.tunnel.local_bind_port,
                'user': self.db_username,
                'password': self.db_password,
                'db': self.db_name,
            }
            if insert:
                connect_kwargs['cursorclass'] = pymysql.cursors.DictCursor
            self.connection = pymysql.connect(**connect_kwargs)
            print("DB 접속 성공")
        except Exception as e:
            print("SSH 또는 DB 연결 실패:", e)

    def execute_query(self, query):
        # 쿼리 실행 후 데이터를 DataFrame으로 반환
        return pd.read_sql_query(query, self.connection)

    def insert_query_with_lookup(self, table_name, data_list):
        try:
            with self.connection.cursor() as cursor:
                for data in data_list:
                    # 1. op_member에서 uid, user_id 조회
                    cursor.execute("""
                        SELECT uid, user_id, add1_connected FROM op_member
                        WHERE add1 = %s
                        LIMIT 1
                    """, (data['acnt_nm'],))
                    result = cursor.fetchone()
                    
                    if result:
                        data['member_uid'] = result['uid']
                        data['user_id'] = result['user_id']
                        data['is_connected'] = result['add1_connected']
                        # 향후에 ig_user_id가 추가가 된다면, 해당 부분도 확인해서 추가할 수 있게
                        # data['ig_user_id'] = result['ig_user_id']
                    else:
                        data['member_uid'] = 0
                        data['user_id'] = 'None'
                        data['is_connected'] = 'n'
                        # data['ig_user_id'] = 'None'
              

                    # 2. INSERT 쿼리 구성 및 실행
                    columns = ', '.join(data.keys())
                    placeholders = ', '.join([f"%({k})s" for k in data.keys()])
                    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    # print(insert_sql)
                    cursor.execute(insert_sql, data)

                    print(f"✅ inserted acnt_id: {data.get('acnt_id', 'N/A')}")

            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("INSERT 실패:", e)
    
    def close(self):
        if self.connection:
            self.connection.close()
        if self.tunnel:
            self.tunnel.stop()

def sendQuery(query):
        ssh = SSHMySQLConnector()
        ssh.load_config_from_json('C:/Users/flexmatch/Desktop/ssom/code/4.Flexmatch_score/config/accounts.json')
        ssh.connect()
        results = ssh.execute_query(query)
        # print(results)
        # print(results.head())
        ssh.close()

        return results
    
def get_all_infos(): 

    query_sales_info = """
        SELECT o.uid, o.add1, s.*
        FROM op_mem_seller_statistics s
        JOIN (
            SELECT member_uid, MAX(regdate) AS max_regdate
            FROM op_mem_seller_statistics
            GROUP BY member_uid
        ) latest ON s.member_uid = latest.member_uid AND s.regdate = latest.max_regdate
        JOIN op_member o ON o.uid = s.member_uid
        JOIN S3_RECENT_USER_INFO_MTR u ON o.add1 = u.acnt_nm
        ORDER BY s.uid DESC
    """
    sales_info = sendQuery(query_sales_info)

    # insterest category 
    query_seller_interest_info = """
        SELECT
        o.user_id, o.ig_user_id, o.add1, s.interestcategory
        FROM op_member o
        left join op_mem_seller s
        on o.user_id=s.user_id
        where (o.ig_user_id != '' and o.ig_user_id is not null) or (o.add1 != '' and o.add1 is not null)
    """
    seller_interest_info = sendQuery(query_seller_interest_info)

    # creator main category
    # query_conn_user_main_category_info = """
    #     SELECT
    #     o.user_id, o.ig_user_id, o.add1, s.main_category, s.top_3_category
    #     FROM op_member o
    #     left join INSTAGRAM_USER_CATEGORY_LABELING s
    #     on o.ig_user_id=s.acnt_id
    #     where (o.ig_user_id != '' and o.ig_user_id is not null) or (o.add1 != '' and o.add1 is not null)
    # """

    query_conn_user_main_category_info = """
        SELECT acnt_id, acnt_nm, main_category, top_3_category, is_connected
        FROM INSTAGRAM_USER_CATEGORY_LABELING
    """

    conn_user_main_category_info = sendQuery(query_conn_user_main_category_info)

    return sales_info, seller_interest_info, conn_user_main_category_info


### 실행 시점으로 부터 어제, 오늘 데이터 가져오는 함수
# def conn_load_weekly_instagram_data(bucket_name, table_list, target_filename='merged_data.parquet'):
#     # 환경 변수 로딩
#     load_dotenv()
#     aws_access_key = os.getenv("aws_accessKey")
#     aws_secret_key = os.getenv("aws_secretKey")

#     client = boto3.client(
#         's3',
#         aws_access_key_id=aws_access_key,
#         aws_secret_access_key=aws_secret_key,
#         region_name='ap-northeast-2'
#     )

#     today = datetime.now()
#     yesterday = (today - timedelta(days=1))

#     today_date = datetime.now().strftime('%Y-%m-%d')
#     yesterday_date = yesterday.strftime('%Y-%m-%d')

#     # 결과 저장용 딕셔너리 초기화
#     merged_data_by_table = {table_name: {} for table_name in table_list}

#     # 주차별로 데이터 로딩
#     recent_dates = [yesterday_date, today_date]
#     recent_data_by_table = {}

#     for table_name in table_list:
#         recent_data_by_table[table_name] = {}

#         for date_str in recent_dates:
#             prefix = f'instagram-data/tables/{table_name}/{date_str}/'
#             response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

#             if 'Contents' not in response:
#                 print(f"[Info] No files found under prefix: {prefix}")
#                 continue

#             target_files = [
#                 content['Key']
#                 for content in response['Contents']
#                 if content['Key'].endswith(target_filename)
#             ]

#             if not target_files:
#                 print(f"[Info] No {target_filename} found for {table_name} on date={date_str}")
#                 continue

#             for file_key in target_files:
#                 try:
#                     obj = client.get_object(Bucket=bucket_name, Key=file_key)
#                     df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
#                     recent_data_by_table[table_name][date_str] = df
#                     print(f"[Success] Loaded {file_key} for table {table_name}, date={date_str}")
#                 except Exception as e:
#                     print(f"[Error] Failed to read {file_key} for {table_name}, date={date_str}: {e}")

#     final_data = {}
#     for table_name, date_data in recent_data_by_table.items():
#         if today_date in date_data and yesterday_date in date_data:
#             final_data[table_name] = {
#                 'yesterday': date_data[yesterday_date],
#                 'today': date_data[today_date]
#             }
#         else:
#             print(f"[Warning] Missing yesterday or today data for table {table_name}")

#     return final_data


###############################################################################################################


### 실행 시점으로부터 일주일 전 데이터 로딩하는 함수
def load_last_weekly_instagram_data(bucket_name, table_list, target_filename='merged_data.parquet'):
    # 환경 변수 로딩
    load_dotenv()
    aws_access_key = os.getenv("aws_accessKey")
    aws_secret_key = os.getenv("aws_secretKey")

    # S3 클라이언트 초기화
    client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name='ap-northeast-2'
    )

    # 날짜 계산
    today = datetime.now()
    day_before_last_week = (today - timedelta(days=8)).strftime('%Y-%m-%d')
    last_week = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    recent_dates = [day_before_last_week, last_week]

    # 테이블별로 결과 저장할 딕셔너리
    recent_data_by_table = {table_name: {} for table_name in table_list}

    for table_name in table_list:
        for date_str in recent_dates:
            prefix = f'instagram-data/tables/{table_name}/{date_str}/'
            try:
                response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

                if 'Contents' not in response:
                    print(f"[Info] No files found under prefix: {prefix}")
                    continue

                target_files = [
                    content['Key']
                    for content in response['Contents']
                    if content['Key'].endswith(target_filename)
                ]

                if not target_files:
                    print(f"[Info] No {target_filename} found for {table_name} on {date_str}")
                    continue

                for file_key in target_files:
                    try:
                        obj = client.get_object(Bucket=bucket_name, Key=file_key)
                        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
                        recent_data_by_table[table_name][date_str] = df
                        print(f"[Success] Loaded {file_key} for table {table_name}, date={date_str}")
                    except Exception as e:
                        print(f"[Error] Failed to read {file_key} for {table_name} on {date_str}: {e}")

            except Exception as e:
                print(f"[Error] Failed to list objects for {prefix}: {e}")

    # 결과 구조 정리
    final_data = {}
    for table_name, date_data in recent_data_by_table.items():
        if day_before_last_week in date_data and last_week in date_data:
            final_data[table_name] = {
                'yesterday': date_data[day_before_last_week],
                'today': date_data[last_week]
            }
        else:
            print(f"[Warning] Missing data for {table_name} on either {day_before_last_week} or {last_week}")

    return final_data

## s3 -> db로 데이터 마이그레이션 진행 이후 수정 코드
def get_weekly_instagram_data_from_db(table_list): 
    # db 연결
    ssh = SSHMySQLConnector()
    ssh.load_config_from_json('C:/Users/flexmatch/Desktop/ssom/code/4.Flexmatch_score/config/accounts.json')
    ssh.connect()
    
    # 날짜 계산
    today = datetime.now()
    day_before_last_week = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    last_week = (today - timedelta(days=6)).strftime('%Y-%m-%d')
    recent_dates = [day_before_last_week, last_week]

    recent_data_by_table = {table_name: {} for table_name in table_list}

    for table_name in table_list:
        for date_str in recent_dates:
            try:
                query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE DATE(reg_date) = '{date_str}'
                """
                df = ssh.execute_query(query)
                if df.empty:
                    print(f"[Info] No data found for {table_name} on {date_str}")
                    continue
                recent_data_by_table[table_name][date_str] = df
                print(f"[Success] Loaded data for {table_name}, date={date_str}")
            except Exception as e:
                print(f"[Error] Failed to query {table_name} on {date_str}: {e}")

    # SSH 연결 종료
    ssh.close()

    # 결과 구조 정리
    final_data = {}
    for table_name, date_data in recent_data_by_table.items():
        if day_before_last_week in date_data and last_week in date_data:
            final_data[table_name] = {
                'day_before_last_week': date_data[day_before_last_week],
                'last_week': date_data[last_week]
            }
        else:
            print(f"[Warning] Missing data for {table_name} on either {day_before_last_week} or {last_week}")

    return final_data

