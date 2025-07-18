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
        ssh.load_config_from_json('C:/Users/ehddl/Desktop/업무/code/config/ssh_db_config.json')
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

    query_seller_info = """
        SELECT
        o.user_id, o.ig_user_id, o.add1, s.interestcategory
        FROM op_member o
        left join op_mem_seller s on o.user_id=s.user_id
        where (o.ig_user_id != '' and o.ig_user_id is not null) or (o.add1 != '' and o.add1 is not null)
    """
    seller_info = sendQuery(query_seller_info)

    return sales_info, seller_info


def load_weekly_instagram_data(bucket_name, table_list, weeks_back=2, target_filename='merged_data.parquet'):
    # 환경 변수 로딩
    load_dotenv()
    aws_access_key = os.getenv("aws_accessKey")
    aws_secret_key = os.getenv("aws_secretKey")

    client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name='ap-northeast-2'
    )

    # 주차 리스트 생성 (현재 주 포함하여 `weeks_back`만큼)
    today = datetime.now()
    week_year_pairs = [
        (today - timedelta(weeks=w)).isocalendar()[:2]
        for w in range(weeks_back)
    ]

    # 결과 저장용 딕셔너리 초기화
    merged_data_by_table = {table_name: {} for table_name in table_list}

    # 주차별로 데이터 로딩
    for year_val, week_val in week_year_pairs:
        for table_name in table_list:
            prefix = f'instagram-data/tables/{table_name}/year={year_val}/week={week_val}/'
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
                print(f"[Info] No {target_filename} found for {table_name} week={week_val}")
                continue

            for file_key in target_files:
                try:
                    obj = client.get_object(Bucket=bucket_name, Key=file_key)
                    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
                    merged_data_by_table[table_name][week_val] = df
                    print(f"[Success] Loaded {file_key} for table {table_name}, week {week_val}")
                except Exception as e:
                    print(f"[Error] Failed to read {file_key} for {table_name}, week {week_val}: {e}")

    recent_weeks_data = {}
    for table_name, week_data in merged_data_by_table.items():
        sorted_weeks = sorted(week_data.keys())
        if len(sorted_weeks) >= 2:
            prev_week, current_week = sorted_weeks[-2], sorted_weeks[-1]
            recent_weeks_data[table_name] = {
                'prev_week': week_data[prev_week],
                'current_week': week_data[current_week]
            }
        else:
            print(f"[Warning] Not enough data for table {table_name} to determine prev/current weeks.")

    
    return recent_weeks_data




