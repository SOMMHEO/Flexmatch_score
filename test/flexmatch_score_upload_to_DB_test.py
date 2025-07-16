import pymysql, json
from sshtunnel import SSHTunnelForwarder
import pandas as pd


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
            print("❌ 설정 JSON 로딩 실패:", e)


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
            print("✅ DB 접속 성공")
        except Exception as e:
            print("❌ SSH 또는 DB 연결 실패:", e)

    def insert_query_with_lookup(self, table_name, data_list):
        try:
            with self.connection.cursor() as cursor:
                for data in data_list:
                    # 1. op_member에서 uid, user_id 조회
                    cursor.execute("""
                        SELECT uid, user_id FROM op_member
                        WHERE add1 = %s
                        LIMIT 1
                    """, (data['acnt_nm'],))
                    result = cursor.fetchone()
                    if result:
                        data['member_uid'] = result['uid']
                        data['user_id'] = result['user_id']
                        # 향후에 ig_user_id가 추가가 된다면, 해당 부분도 확인해서 추가할 수 있게
                        # data['ig_user_id'] = result['ig_user_id']
                    else:
                        data['member_uid'] = 0
                        data['user_id'] = 'None'
                        # data['ig_user_id'] = 'None'

                    # 2. INSERT 쿼리 구성 및 실행
                    columns = ', '.join(data.keys())
                    placeholders = ', '.join([f"%({k})s" for k in data.keys()])
                    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(insert_sql, data)

                    print(f"✅ inserted acnt_id: {data.get('acnt_id', 'N/A')}")

            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("❌ INSERT 실패:", e)

    def close(self):
        if self.connection:
            self.connection.close()
        if self.tunnel:
            self.tunnel.stop()



ssh_ = SSHMySQLConnector()

ssh_.load_config_from_json('config/ssh_db_config.json')

ssh_.connect()

data_list = [
    {
        'activity_score': 0.18618008163528707,
        'trend_score': 1.2384506112322682,
        'follower_total_engagement': 0.9006925508419993,
        'follower_retention_rate': 4.84593837535013,
        'avg_post_efficiency': 1.4776093002849746,
        'acnt_id': '17841400070132367',
        'acnt_nm': 'gnuoyeatt',
        'influencer_scale_type': 'nano'
    }
]

ssh_.insert_query_with_lookup('op_mem_seller_score', data_list)
