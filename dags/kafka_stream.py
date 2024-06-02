import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# 기본 DAG 인자를 정의합니다.
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 4, 21, 10, 00)
}

def get_data():
    import requests

    # randomuser.me API를 호출하여 사용자 데이터를 가져옵니다.
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0] # 결과에서 첫 번째 사용자 데이터를 가져옵니다.

    return res

# 가져온 데이터를 특정 형식으로 변환합니다.
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

# 데이터를 Kafka로 스트리밍합니다.
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    res = get_data()
    res = format_data(res)


    # Kafka 프로듀서를 생성합니다.
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: # 1분 동안 데이터 스트리밍을 수행합니다.
            break
        try:
            res = get_data()
            res = format_data(res)

            # Kafka 토픽 'users_created'에 데이터를 전송합니다.
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

# Airflow DAG을 정의합니다.
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',    # DAG이 매일 실행되도록 설정합니다.
         catchup=False) as dag:

    # PythonOperator를 사용하여 stream_data 함수를 실행하는 태스크를 정의합니다.
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
