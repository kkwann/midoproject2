# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
# import geopandas as gpd
# import pandas_gbq
from datetime import datetime, timedelta
import pytz
import json
from shapely import wkt
from google.cloud import bigquery
from google.oauth2 import service_account

import warnings
warnings.filterwarnings("ignore")

credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

def save_dataframe_to_bigquery(df, dataset_id, table_id):

    # 빅쿼리 클라이언트 객체 생성
    client = bigquery.Client(credentials=credentials)

    # 테이블 레퍼런스 생성
    table_ref = client.dataset(dataset_id).table(table_id)

    # 'bool' 타입 열을 제외한 나머지 열에 대한 처리
    non_bool_columns = df.select_dtypes(exclude=['bool']).columns
    df[non_bool_columns] = df[non_bool_columns].astype(str).replace('nan', '').replace('None', '').replace('', '')

    # 'bool' 타입 열에 대한 처리 (변환 없이 그대로 유지)
    bool_columns = df.select_dtypes(include=['bool']).columns
    df[bool_columns] = df[bool_columns]

    # 데이터프레임을 BigQuery 테이블에 적재
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"  # 기존 테이블 내용 삭제 후 삽입

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # 작업 완료 대기

    print(f"Data inserted into table {table_id} successfully.")

def get_dataframe_from_bigquery(dataset_id, table_id):

    # BigQuery 클라이언트 생성
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # 테이블 레퍼런스 생성
    table_ref = client.dataset(dataset_id).table(table_id)

    # 테이블 데이터를 DataFrame으로 변환
    df = client.list_rows(table_ref).to_dataframe()

    return df


def get_dataframe_from_bigquery_by_date(dataset_id, table_id, start_date, end_date):

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    start_date = pd.to_datetime(start_date, format='%Y%m%d').date().strftime('%Y-%m-%d')
    end_date = pd.to_datetime(end_date, format='%Y%m%d').date().strftime('%Y-%m-%d')

    query = f"""
    SELECT *
    FROM `{dataset_id}.{table_id}`
    WHERE collection_Date BETWEEN '{start_date}' AND '{end_date}'
    """

    # 쿼리 실행
    df = client.query(query).to_dataframe()

    return df

def get_geodataframe_from_bigquery(dataset_id, table_id):

    # 빅쿼리 클라이언트 객체 생성
    client = bigquery.Client(credentials=credentials)

    # 쿼리 작성
    query = f"SELECT * FROM `{dataset_id}.{table_id}`"

    # 쿼리 실행
    df = client.query(query).to_dataframe()

    # 'geometry' 열의 문자열을 다각형 객체로 변환
    df['geometry'] = df['geometry'].apply(wkt.loads)

    # GeoDataFrame으로 변환
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = "EPSG:5179"

    return gdf


def log_user_action(username, action, dataset_id, table_id):

    client = bigquery.Client(credentials=credentials)

    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    # 현재 시각을 한국 시간으로 설정
    kst = pytz.timezone('Asia/Seoul')
    timestamp_now = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

    rows_to_insert = [
        {
            "username": username,
            "timestamp": timestamp_now,  # 문자열로 변환된 시각
            "action": action
        }
    ]

    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

@st.cache_data
def load_users_data():
    users = get_dataframe_from_bigquery('SERVICE_DATA', 'users').sort_values(by='employeeNumber').reset_index(drop=True)
    users = users[['employeeName', 'jobTitle', 'password']]

    return users

@st.cache_data(ttl=300)
def load_list_up_data():
    list_up_budget_data = get_dataframe_from_bigquery('DATA_MARTS', 'list_up_budget_data')
    list_up_edu_budget_data = get_dataframe_from_bigquery('DATA_MARTS', 'list_up_edu_budget_data')

    list_up_budget_data = list_up_budget_data.sort_values(by=['지역명', '자치단체명'])
    list_up_edu_budget_data = list_up_edu_budget_data.sort_values(by=['도광역시', '시군구'])

    return list_up_budget_data, list_up_edu_budget_data

@st.cache_data(ttl=3600)
def load_budget_data():
    today = datetime.now().date()

    budget_df = get_dataframe_from_bigquery_by_date('DATA_WAREHOUSE', 'budget_data', today, today)

    numeric_columns = ['예산현액', '국비', '시도비', '시군구비', '기타', '지출액', '편성액']

    for column in numeric_columns:
        if column in budget_df.columns:
            budget_df[column] = budget_df[column].str.replace(',', '')
            budget_df[column] = pd.to_numeric(budget_df[column], errors='coerce')

    columns_to_view = [
        '지역명', '자치단체명', '세부사업명', '예산현액', '국비', '시도비', '시군구비', '기타', '지출액', '편성액'
    ]

    budget_df = budget_df[columns_to_view]
    budget_df = budget_df.sort_values(by='자치단체명')

    return budget_df

def load_latest_budget_data():

    new_budget_data = get_dataframe_from_bigquery('DATA_MARTS', 'new_budget_data')
    latest_budget_data = get_dataframe_from_bigquery('DATA_MARTS', 'latest_budget_data')

    numeric_columns = ['예산현액', '국비', '시도비', '시군구비', '기타', '지출액', '편성액']

    for column in numeric_columns:
        if column in new_budget_data.columns:
            new_budget_data[column] = new_budget_data[column].str.replace(',', '')
            new_budget_data[column] = pd.to_numeric(new_budget_data[column], errors='coerce')

    for column in numeric_columns:
        if column in latest_budget_data.columns:
            latest_budget_data[column] = latest_budget_data[column].str.replace(',', '')
            latest_budget_data[column] = pd.to_numeric(latest_budget_data[column], errors='coerce')

    columns_to_view = [
        '지역명', '자치단체명', '세부사업명', '예산현액', '국비', '시도비', '시군구비', '기타', '지출액', '편성액'
    ]

    new_budget_data = new_budget_data[columns_to_view]
    new_budget_data = new_budget_data.sort_values(by='자치단체명')

    latest_budget_data = latest_budget_data[columns_to_view]
    latest_budget_data = latest_budget_data.sort_values(by='자치단체명')

    return new_budget_data, latest_budget_data

@st.cache_data(ttl=3600)
def load_edu_budget_data():
    edu_budget_df = get_dataframe_from_bigquery('DATA_WAREHOUSE', 'edu_budget_data')

    numeric_columns = ['금액', '면적']

    for column in numeric_columns:
        if column in edu_budget_df.columns:
            edu_budget_df[column] = edu_budget_df[column].str.replace(',', '')
            edu_budget_df[column] = pd.to_numeric(edu_budget_df[column], errors='coerce')

    columns_to_view = [
        '도광역시', '시군구', '구분', '과업명', '금액', '면적', '예산집행'
    ]

    edu_budget_df = edu_budget_df[columns_to_view]
    edu_budget_df = edu_budget_df.sort_values(by=['도광역시', '시군구'])

    return edu_budget_df

@st.cache_data(ttl=3600)
def load_info_con_data():
    # 공사입찰/공사낙찰

    bir_con_df = get_dataframe_from_bigquery('DATA_MARTS', 'bid_con_data')

    bir_con_df['입력일'] = pd.to_datetime(bir_con_df['입력일']).dt.strftime('%Y-%m-%d')
    bir_con_df['투찰마감'] = pd.to_datetime(bir_con_df['투찰마감']).dt.strftime('%Y-%m-%d')
    bir_con_df['개찰일'] = pd.to_datetime(bir_con_df['개찰일']).dt.strftime('%Y-%m-%d')

    numeric_columns = ['추정가격', '기초금액']

    for column in numeric_columns:
        if column in bir_con_df.columns:
            bir_con_df[column] = bir_con_df[column].str.replace(',', '')
            bir_con_df[column] = pd.to_numeric(bir_con_df[column], errors='coerce')

    info_con_df = bir_con_df.sort_values(by='입력일', ascending=False)

    view_columns = [
        '입력일', '공고명', '발주기관', '추정가격', '기초금액', '투찰마감', '개찰일', '업종', '지역', '분류'
    ]

    info_con_df = info_con_df[view_columns]

    return info_con_df

@st.cache_data(ttl=3600)
def load_info_ser_data():
    # 용역입찰/용역낙찰

    bir_ser_df = get_dataframe_from_bigquery('DATA_MARTS', 'bid_ser_data')

    bir_ser_df['입력일'] = pd.to_datetime(bir_ser_df['입력일']).dt.strftime('%Y-%m-%d')
    bir_ser_df['투찰마감'] = pd.to_datetime(bir_ser_df['투찰마감']).dt.strftime('%Y-%m-%d')
    bir_ser_df['개찰일'] = pd.to_datetime(bir_ser_df['개찰일']).dt.strftime('%Y-%m-%d')

    numeric_columns = ['추정가격', '기초금액']

    for column in numeric_columns:
        if column in bir_ser_df.columns:
            bir_ser_df[column] = bir_ser_df[column].str.replace(',', '')
            bir_ser_df[column] = pd.to_numeric(bir_ser_df[column], errors='coerce')

    info_ser_df = bir_ser_df.sort_values(by='입력일', ascending=False)

    view_columns = [
        '입력일', '공고명', '발주기관', '추정가격', '기초금액', '투찰마감', '개찰일', '업종', '지역', '분류'
    ]

    info_ser_df = info_ser_df[view_columns]

    return info_ser_df

@st.cache_data(ttl=3600)
def load_info_pur_data():
    # 구매입찰/구매낙찰

    bir_pur_df = get_dataframe_from_bigquery('DATA_MARTS', 'bid_pur_data')

    bir_pur_df['참가마감'] = pd.to_datetime(bir_pur_df['참가마감']).dt.strftime('%Y-%m-%d')
    bir_pur_df['투찰마감'] = pd.to_datetime(bir_pur_df['투찰마감']).dt.strftime('%Y-%m-%d')
    bir_pur_df['개찰일'] = pd.to_datetime(bir_pur_df['개찰일']).dt.strftime('%Y-%m-%d')

    numeric_columns = ['기초금액']

    for column in numeric_columns:
        if column in bir_pur_df.columns:
            bir_pur_df[column] = bir_pur_df[column].str.replace(',', '')
            bir_pur_df[column] = pd.to_numeric(bir_pur_df[column], errors='coerce')

    info_pur_df = bir_pur_df.sort_values(by='투찰마감', ascending=False)

    view_columns = [
        '공고명', '기초금액', '업종', '참가마감', '투찰마감', '개찰일', '분류'
    ]

    info_pur_df = info_pur_df[view_columns]

    return info_pur_df

@st.cache_data(ttl=3600)
def load_current_year_g2b_data():
    g2b_df = get_dataframe_from_bigquery('DATA_MARTS', 'g2b_data')

    g2b_df['수요기관지역명'] = g2b_df['수요기관지역명'].replace({'강원도': '강원특별자치도', '전라북도': '전북특별자치도'}, regex=True)

    g2b_df['도광역시'] = g2b_df['수요기관지역명'].apply(lambda x: x.split(' ')[0])
    g2b_df['시군구'] = g2b_df['수요기관지역명'].apply(lambda x: x.split(' ')[1] if len(x.split(' ')) > 1 else None)

    columns_to_view = [
        '납품요구번호', '납품요구변경차수', '납품요구접수일자', '물품순번', '물품분류번호',
        '품명', '세부물품분류번호', '세부품명', '물품식별번호', '품목', '단가', '단위',
        '수량', '금액', '납품기한일자', '계약구분', '우수제품여부', '옵션구분', '수요기관코드',
        '수요기관명', '수요기관구분', '수요기관지역명', '업체명', '최종납품요구여부',
        '증감납품요구수량', '증감납품요구금액', '업체사업자등록번호', '납품요구건명',
        '계약번호', '계약변경차수', '다수공급자계약여부', '공사용자재직접구매대상여부',
        '최초납품요구접수일자', '납품요구수량', '납품요구금액', '중소기업자간경쟁제품여부',
        '업체기업구분명', '납품요구지청명', '도광역시', '시군구'
    ]

    g2b_df = g2b_df[columns_to_view]

    g2b_df['납품요구접수일자'] = pd.to_datetime(g2b_df['납품요구접수일자'], errors='coerce')

    g2b_df['단가'] = pd.to_numeric(g2b_df['단가'], errors='coerce')
    g2b_df['수량'] = pd.to_numeric(g2b_df['수량'], errors='coerce')
    g2b_df['금액'] = pd.to_numeric(g2b_df['금액'], errors='coerce')

    g2b_df = g2b_df.sort_values(by='납품요구접수일자', ascending=False)

    # Load the JSON data from the file
    with open('region.json', 'r', encoding='utf-8') as file:
        regions = json.load(file)

    def get_lat_long(row):
        if pd.isna(row['시군구']):
            region_key = f"{row['도광역시']}/"
        else:
            region_key = f"{row['도광역시']}/{row['시군구']}"

        if region_key in regions:
            return regions[region_key]["lat"], regions[region_key]["long"]
        else:
            return None, None

    g2b_df[['위도', '경도']] = g2b_df.apply(get_lat_long, axis=1, result_type='expand')

    return g2b_df

@st.cache_data(ttl=3600)
def load_g2b_data():
    g2b_df = get_dataframe_from_bigquery('DATA_WAREHOUSE', 'g2b_data')

    g2b_df['수요기관지역명'] = g2b_df['수요기관지역명'].replace({'강원도': '강원특별자치도', '전라북도': '전북특별자치도'}, regex=True)

    g2b_df['도광역시'] = g2b_df['수요기관지역명'].apply(lambda x: x.split(' ')[0])
    g2b_df['시군구'] = g2b_df['수요기관지역명'].apply(lambda x: x.split(' ')[1] if len(x.split(' ')) > 1 else None)

    columns_to_view = [
        '납품요구번호', '납품요구변경차수', '납품요구접수일자', '물품순번', '물품분류번호',
        '품명', '세부물품분류번호', '세부품명', '물품식별번호', '품목', '단가', '단위',
        '수량', '금액', '납품기한일자', '계약구분', '우수제품여부', '옵션구분', '수요기관코드',
        '수요기관명', '수요기관구분', '수요기관지역명', '업체명', '최종납품요구여부',
        '증감납품요구수량', '증감납품요구금액', '업체사업자등록번호', '납품요구건명',
        '계약번호', '계약변경차수', '다수공급자계약여부', '공사용자재직접구매대상여부',
        '최초납품요구접수일자', '납품요구수량', '납품요구금액', '중소기업자간경쟁제품여부',
        '업체기업구분명', '납품요구지청명', '도광역시', '시군구'
    ]

    g2b_df = g2b_df[columns_to_view]

    g2b_df['납품요구접수일자'] = pd.to_datetime(g2b_df['납품요구접수일자'], errors='coerce')

    g2b_df['단가'] = pd.to_numeric(g2b_df['단가'], errors='coerce')
    g2b_df['수량'] = pd.to_numeric(g2b_df['수량'], errors='coerce')
    g2b_df['금액'] = pd.to_numeric(g2b_df['금액'], errors='coerce')

    g2b_df = g2b_df.sort_values(by='납품요구접수일자', ascending=False)

    # Load the JSON data from the file
    with open('region.json', 'r', encoding='utf-8') as file:
        regions = json.load(file)

    def get_lat_long(row):
        if pd.isna(row['시군구']):
            region_key = f"{row['도광역시']}/"
        else:
            region_key = f"{row['도광역시']}/{row['시군구']}"

        if region_key in regions:
            return regions[region_key]["lat"], regions[region_key]["long"]
        else:
            return None, None

    g2b_df[['위도', '경도']] = g2b_df.apply(get_lat_long, axis=1, result_type='expand')

    return g2b_df

@st.cache_data(ttl=3600)
def load_news_data():
    news_df = get_dataframe_from_bigquery('DATA_MARTS', 'news_data').sort_values('기사날짜', ascending=False)

    today = datetime.now().date()
    latest = today - timedelta(days=3)

    news_df['기사날짜'] = pd.to_datetime(news_df['기사날짜'])
    news_df = news_df[news_df['기사날짜'].dt.date >= latest]

    # 키워드 중요도 리스트
    keywords = ['인조잔디','예산', '추경']
    keyword_importance = {keyword: i for i, keyword in enumerate(keywords)}

    def get_importance(name):
        if name is None:
            return float('inf')  # name이 None인 경우 맨 뒤로 정렬
        for keyword, importance in keyword_importance.items():
            if keyword in name:
                return importance
        return float('inf')  # 키워드가 없는 경우 맨 뒤로 정렬

    # 키워드 중요도 점수 컬럼 추가
    news_df['중요도'] = news_df['내용'].apply(get_importance)
    news_df = news_df.sort_values(by='중요도')
    news_df = news_df.drop(columns=['중요도'])

    columns_to_view = [
        '기사날짜', 'URL', '제목', '내용'
    ]

    news_df = news_df[columns_to_view]

    return news_df