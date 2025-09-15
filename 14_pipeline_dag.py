# API를 활용하여 날씨를 불러오는 AirFlow를 DAG 파일 작성하기
import requests
from datetime import datetime, timedelta
import pendulum # python의 datetime을 좀더 편하게 사용할 수 있게 돕는 모델
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import dag, task

local_tz = pendulum.timezone("Asia/Seoul")


# 1. API 키 불러오기
def load_API(ti):
    API_KEY = '31563b64da6452ee368abb8f9e78d807'
    CITY = "Seoul"
    URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    print(URL)
    ti.xcom_push(key='URL', value=URL)

# 2. API 호출로 날씨 정보 가지고오기
def load_weather_info(ti):
    # 'load_api' 태스크에서 보낸 URL을 가져옵니다.
    API = ti.xcom_pull(task_ids='load_api', key='URL')
    
    # if API is None:
    #     raise ValueError("URL이 XCom에서 전달되지 않았습니다.")
        
    response = requests.get(API)
    # response.raise_for_status() # 오류 발생 시 예외 처리

    data = response.json()
    temp = data["main"]["temp"]
    weather = data["weather"][0]["description"]
    humidity = data["main"]["humidity"]

    return f"서울의 현재 날씨: {weather}, 온도: {temp}°C, 습도: {humidity}%"

# 3. 해당 날씨 정보로 README.md 업데이트 하기
def write_readme(ti):

    weather_info = ti.xcom_pull(task_ids='load_weather_info', key='return_value')

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    readme_content = f"""
# Weather API Status

이 리포지토리는 OpenWeather API를 사용하여 서울의 날씨 정보를 자동으로 업데이트합니다.

## 현재 서울 날씨
> {weather_info}

⏳ 업데이트 시간: {now} (UTC)

---
자동 업데이트 봇에 의해 관리됩니다.
"""
    print(readme_content)
    return readme_content
    
def update_readme(ti):
    content = ti.xcom_pull(task_ids='write_readme', key='return_value')
    README_PATH = "/opt/airflow/data/README.md"
    os.makedirs(os.path.dirname(README_PATH), exist_ok=True)

    with open(README_PATH, "w", encoding="utf-8") as file:
        file.write(content)

# ------------------------------------------------------------------------

# DAG 정의
with DAG(
    dag_id='weather_api',
    start_date=datetime(2025, 9, 13, tzinfo=local_tz),
    schedule='@once',
    catchup=True,
    tags=["fisaai"]
) as dag:
    
    api = PythonOperator(
        task_id='load_api',
        python_callable=load_API
    )

    weather_info = PythonOperator(
        task_id='load_weather_info',
        python_callable=load_weather_info
    )

    write = PythonOperator(
        task_id='write_readme',
        python_callable=write_readme
    )

    update = PythonOperator(
        task_id='update_readme',
        python_callable=update_readme
    )

    # 작업 순서 설정
    api >> weather_info >> write >> update

