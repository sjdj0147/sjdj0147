# 되는 코드 (아마 마지막 수정)
import psycopg2
from psycopg2 import sql
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import random
import time
import json
import re

# Slack 봇 설정
slack_token = "xoxb-6110359304097-6095073318517-dRfsQIdPUq3q1vJe0s82fn1U"
channel_id = "C0638ALULRF"


# Slack WebClient 초기화
client = WebClient(token=slack_token)

# PostgreSQL 데이터베이스 연결 정보
db_params = {
    "database": "study",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432",
}

# 연결 설정
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

yesterday_data_query = """
    SELECT nameKo
    FROM musinsa
    WHERE date(observedAt) = CURRENT_DATE - INTERVAL '1 day'
"""
# Selenium 웹 드라이버 설정
webdriver_path = "/Users/onthelook/ex1/chromedriver"
driver = webdriver.Chrome(executable_path=webdriver_path)

# 시작 페이지 설정
start_page = 1

# 웹페이지 열어서 숫자 부분 크롤링
driver.get("https://www.musinsa.com/brands?categoryCode=&type=&sortCode=BRAND_RANK")
WebDriverWait(driver, 10).until(
    EC.presence_of_element_located(
        (By.CSS_SELECTOR, "div.brand_popup div.brand_contents_popup > div")
    )
)

# 페이지의 HTML 내용 가져오기
html = driver.page_source

# BeautifulSoup를 사용하여 HTML 파싱
soup = BeautifulSoup(html, "html.parser")

# 숫자 부분 크롤링
span_text = soup.find("span", class_="totalPagingNum").text
end_page = int("".join(filter(str.isdigit, span_text)))

brands_data = []
brands_count = 0

try:
    for page in range(start_page, end_page + 1):
        url = f"https://www.musinsa.com/brands?categoryCode=&type=&sortCode=BRAND_RANK&page={page}&size=100"
        random_sleep_time = random.uniform(0.1, 0.5)

        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.brand_popup div.brand_contents_popup > div")
            )
        )

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        brand_descriptions = soup.select(
            "div.brand_popup div.brand_contents_popup > div"
        )

        if not brand_descriptions:
            print(f"페이지 {page}에서 브랜드 설명을 찾을 수 없습니다. CSS 선택기에 문제가 있을 수 있습니다.")
        else:
            brand_items = driver.find_elements(By.CSS_SELECTOR, "li.brand_li")

            for idx, description in enumerate(brand_descriptions, start=1):
                title_box = description.find_previous("span", class_="title-box")
                # "span" 요소 중에서 첫 번째 요소의 텍스트만 가져옵니다.
                brand_name = (
                    description.find_previous("span", class_="title-box").get_text(
                        strip=True
                    )
                    if description.find_previous("span", class_="title-box")
                    else "알 수 없는 브랜드"
                )

                # 영어 텍스트만 추출
                brand_name = re.sub(r"[^a-zA-Z\s]", "", brand_name)

                nameEn = brand_name
                description_text = description.get_text(strip=True)
                nameKo = (
                    brand_items[idx - 1]
                    .find_element(By.CSS_SELECTOR, "a.gtm-catch-click")
                    .text.strip()
                )
                url = (
                    brand_items[idx - 1]
                    .find_element(By.CSS_SELECTOR, "a.gtm-catch-click")
                    .get_attribute("href")
                )

                product_count_element = brand_items[idx - 1]
                product_count_text = product_count_element.text
                product_count_text = product_count_text.replace(",", "")
                # 숫자만 추출합니다.
                product_count_match = re.search(r"\d+", product_count_text)
                product_count = (
                    int(product_count_match.group()) if product_count_match else 0
                )

                observed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                brand_info = {
                    "nameKo": nameKo,
                    "nameEn": nameEn,
                    "url": url,
                    "description": description_text,
                    "product_count": product_count,
                    "observedAt": observed_at,
                    "createdAt": observed_at,  # observedAt 값을 createdAt에 저장
                    "inactive": False,
                }

                brands_data.append(brand_info)

        time.sleep(random_sleep_time)
finally:
    driver.quit()

for brand_info in brands_data:
    insert_query = sql.SQL(
        """
        INSERT INTO musinsa (nameKo, nameEn, url, description, product_count, observedAt, createdAt, inactive) 
        VALUES ({nameKo}, {nameEn}, {url}, {description}, {product_count}, {observedAt}, {createdAt}, {inactive})
        ON CONFLICT (url) DO UPDATE
        SET
            nameen = EXCLUDED.nameEn,
            nameKo = EXCLUDED.nameKo,
            description = EXCLUDED.description,
            product_count = EXCLUDED.product_count,
            observedAt = EXCLUDED.observedAt
    """
    ).format(
        nameKo=sql.Literal(brand_info["nameKo"]),
        nameEn=sql.Literal(brand_info["nameEn"]),
        url=sql.Literal(brand_info["url"]),
        description=sql.Literal(brand_info["description"]),
        product_count=sql.Literal(brand_info["product_count"]),
        observedAt=sql.Literal(brand_info["observedAt"]),
        createdAt=sql.Literal(brand_info["createdAt"]),
        inactive=sql.Literal(brand_info["inactive"]),
    )
    cursor.execute(insert_query)  # 각 브랜드 정보에 대한 INSERT 문 실행
    connection.commit()  # 트랜잭션 커밋
cursor.execute("SELECT setval('musinsays_id_seq', 1, false);")
# "musinsays" 테이블 업데이트 (Delta 및 상태 업데이트)


today_data_query = """
    SELECT nameKo
    FROM musinsa
    WHERE date(observedAt) = CURRENT_DATE
"""

# 증가분
insert_increase_query = """
    INSERT INTO musinsays (musinsa_id, changedAt, inactive)
    SELECT m.id, CURRENT_DATE, True
    FROM ({today_data_query}) AS t
    JOIN musinsa m ON m.nameKo = t.nameKo
    WHERE NOT EXISTS (
        SELECT 1
        FROM ({yesterday_data_query}) AS y
        WHERE y.nameKo <> t.nameKo
    )
    ON CONFLICT (musinsa_id) DO NOTHING;
"""

# 감소분
insert_decrease_query = """
    INSERT INTO musinsays (musinsa_id, changedAt, inactive)
    SELECT m.id, CURRENT_DATE, FALSE
    FROM ({yesterday_data_query}) AS y
    JOIN musinsa m ON m.nameKo = y.nameKo
    WHERE NOT EXISTS (
        SELECT 1
        FROM ({today_data_query}) AS t
        WHERE t.nameKo = y.nameKo
    )
    ON CONFLICT (musinsa_id) DO NOTHING;
"""

try:
    cursor.execute(
        insert_increase_query.format(
            today_data_query=today_data_query, yesterday_data_query=yesterday_data_query
        )
    )
    cursor.execute(
        insert_decrease_query.format(
            today_data_query=today_data_query, yesterday_data_query=yesterday_data_query
        )
    )
    connection.commit()
except psycopg2.Error as e:
    print(f"Error: {e}")
    connection.rollback()

# musinsa 테이블의 누적 수집 브랜드 수 계산
cursor.execute("SELECT COUNT(*) FROM musinsa")
total_brands_count = cursor.fetchone()[0]
# 증가분 수 조회
increase_query = """
SELECT COUNT(*)
FROM musinsays
WHERE changedAt = CURRENT_DATE AND inactive = True
"""

try:
    cursor.execute(increase_query)
    increase_count = cursor.fetchone()[0]
    print(f"증가분 수: {increase_count}")
except psycopg2.Error as e:
    print(f"Error: {e}")

# 감소분 수 조회
decrease_query = """
SELECT COUNT(*)
FROM musinsays
WHERE changedAt = CURRENT_DATE AND inactive = FALSE
"""

try:
    cursor.execute(decrease_query)
    decrease_count = cursor.fetchone()[0]
    print(f"감소분 수: {decrease_count}")
except psycopg2.Error as e:
    print(f"Error: {e}")


# 메인 메시지
main_message = (
    f"총 누적 수집 브랜드: {total_brands_count }개\n"
    f"어제 생성된 브랜드: {increase_count}개\n"
    f"어제 삭제된 브랜드: {decrease_count}개"
)

# Slack 봇을 통해 메인 메시지 게시
try:
    main_response = client.chat_postMessage(channel=channel_id, text=main_message)
    print("Slack 봇에게 메시지를 게시했습니다.")
    # 메인 메시지의 'ts' 속성을 thread_ts로 설정
    thread_ts = main_response.data["ts"]
except SlackApiError as e:
    # Slack 메시지 결과를 확인하려면 response 변수를 사용할 수 있습니다.
    print(f"Slack 오류: {e.response['error']}")

# 삭제된 브랜드 메시지
deleted_brands_query = """
    SELECT m.nameKo, m.nameEn
    FROM musinsa m
    JOIN musinsays s ON m.id = s.musinsa_id
    WHERE s.inactive = false
"""
created_brands_query = """
    SELECT m.nameKo, m.nameEn
    FROM musinsa m
    JOIN musinsays s ON m.id = s.musinsa_id
    WHERE s.inactive = true
"""
cursor.execute(deleted_brands_query)
deleted_brands = cursor.fetchall()

cursor.execute(created_brands_query)
created_brands = cursor.fetchall()

deleted_brands_message = "삭제된 브랜드\n"
for brand in deleted_brands:
    deleted_brands_message += f"• {brand[0]}\n"

created_brands_message = "추가된 브랜드\n"
for brand in created_brands:
    created_brands_message += f"• {brand[0]}\n"

# Slack 봇을 통해 삭제된 브랜드 메시지를 게시 (메인 메시지의 스레드로)
try:
    response = client.chat_postMessage(
        channel=channel_id, text=deleted_brands_message, thread_ts=thread_ts
    )
    print("Slack 봇에게 삭제 브랜드 메시지를 게시했습니다.")
except SlackApiError as e:
    # Slack 메시지 결과를 확인하려면 response 변수를 사용할 수 있습니다.
    print(f"Slack 오류: {e.response['error']}")

try:
    response = client.chat_postMessage(
        channel=channel_id, text=created_brands_message, thread_ts=thread_ts
    )
    print("Slack 봇에게 생성 메시지를 게시했습니다.")
except SlackApiError as e:
    # Slack 메시지 결과를 확인하려면 response 변수를 사용할 수 있습니다.
    print(f"Slack 오류: {e.response['error']}")

cursor.execute("SELECT setval('musinsays_id_seq', 1, false);")
cursor.execute("SELECT setval('musinsa_id_seq', 1, false);")
connection.close()
