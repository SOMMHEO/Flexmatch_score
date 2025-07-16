import pandas as pd
import os
from dotenv import load_dotenv
import boto3
import io
import pymysql
import json
from datetime import datetime, timedelta
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

    def connect(self):
        try:
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, 22),
                ssh_username=self.ssh_username,
                ssh_password=self.ssh_password,
                remote_bind_address=('127.0.0.1', 3306),
            )
            self.tunnel.start()
            
            self.connection = pymysql.connect(
                host='127.0.0.1',
                port=self.tunnel.local_bind_port,
                user=self.db_username,
                password=self.db_password,
                db=self.db_name,
                cursorclass=pymysql.cursors.DictCursor  # 이 줄 추가
            )
            print("DB 접속 성공")
        except Exception as e:
            print("SSH 또는 DB 연결 실패:", e)

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
                    print(insert_sql)
                    cursor.execute(insert_sql, data)

                    print(f"inserted acnt_id: {data.get('acnt_id', 'N/A')}")

            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("INSERT 실패:", e)

    def get_all_infos():
        
        return 
    
    def close(self):
        if self.connection:
            self.connection.close()
        if self.tunnel:
            self.tunnel.stop()


def conn_load_weekly_instagram_data(bucket_name, table_list, target_filename='merged_data.parquet'):
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

    today = datetime.now()
    yesterday = (today - timedelta(days=1))

    today_date = datetime.now().strftime('%Y-%m-%d')
    yesterday_date = yesterday.strftime('%Y-%m-%d')

    # 결과 저장용 딕셔너리 초기화
    merged_data_by_table = {table_name: {} for table_name in table_list}

    # 주차별로 데이터 로딩
    recent_dates = [yesterday_date, today_date]
    recent_data_by_table = {}

    for table_name in table_list:
        recent_data_by_table[table_name] = {}

        for date_str in recent_dates:
            prefix = f'instagram-data/tables/{table_name}/{date_str}/'
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
                print(f"[Info] No {target_filename} found for {table_name} on date={date_str}")
                continue

            for file_key in target_files:
                try:
                    obj = client.get_object(Bucket=bucket_name, Key=file_key)
                    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
                    recent_data_by_table[table_name][date_str] = df
                    print(f"[Success] Loaded {file_key} for table {table_name}, date={date_str}")
                except Exception as e:
                    print(f"[Error] Failed to read {file_key} for {table_name}, date={date_str}: {e}")

    final_data = {}
    for table_name, date_data in recent_data_by_table.items():
        if today_date in date_data and yesterday_date in date_data:
            final_data[table_name] = {
                'yesterday': date_data[yesterday_date],
                'today': date_data[today_date]
            }
        else:
            print(f"[Warning] Missing yesterday or today data for table {table_name}")

    return final_data


