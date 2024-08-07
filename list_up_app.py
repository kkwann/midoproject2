# -*- coding: utf-8 -*-
import streamlit as st

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import utils

def list_up_app():
    today = datetime.now().strftime('%Y%m%d_%H%M%S')

    list_up_budget_data, list_up_edu_budget_data = utils.load_list_up_data()

    st.header("예산 사업 현황")
    st.markdown("---")

    tab1, tab2 = st.tabs(["지자체 예산 현황", "교육청 예산 현황"])

    with tab1:
        st.markdown("---")
        st.subheader("지자체 예산 현황")
        st.markdown("---")

        # CSV 업로드
        uploaded_file = st.file_uploader("지자체 예산 CSV 파일 업로드", type="csv", key='list_up_budget_file_uploader')

        utils.save_dataframe_to_bigquery(list_up_budget_data, 'list_up_data_backup', f'list_up_budget_data_{today}')

        if uploaded_file is not None:
            # 업로드된 CSV 파일 읽기
            uploaded_df = pd.read_csv(uploaded_file)

            # 업로드된 데이터로 기존 데이터프레임 완전히 대체
            list_up_budget_data = uploaded_df
            list_up_budget_data = list_up_budget_data.sort_values(by=['지역명', '자치단체명'])

            utils.save_dataframe_to_bigquery(list_up_budget_data, 'DATA_MARTS', 'list_up_budget_data')
            utils.log_user_action(st.session_state['username'], "save list 지자체 현황 csv uploaded", "SERVICE_DATA", "logs")

        st.markdown("---")

        numeric_columns = ['예산현액', '국비', '시도비', '시군구비', '기타', '지출액', '편성액']

        for column in numeric_columns:
            list_up_budget_data[column] = list_up_budget_data[column].replace('None', np.nan)
            list_up_budget_data[column] = pd.to_numeric(list_up_budget_data[column].astype(str).str.replace(',', ''), errors='coerce')

        key_column = st.selectbox('필터링할 열 선택', list_up_budget_data.columns, index=list_up_budget_data.columns.get_loc('세부사업명'), key='list_up_budget_data_key_column')

        if key_column in numeric_columns:
            min_value = float(list_up_budget_data[key_column].min())
            max_value = float(list_up_budget_data[key_column].max())
            value_range = st.slider(f'{key_column}에서 검색할 범위 선택',
                                    min_value=min_value,
                                    max_value=max_value,
                                    value=(min_value, max_value),
                                    key='list_up_budget_data_value_range')

            list_up_budget_data_filtered = list_up_budget_data[(list_up_budget_data[key_column] >= value_range[0]) & (list_up_budget_data[key_column] <= value_range[1])]
        else:
            search_term = st.text_input(f'{key_column}에서 검색할 내용 입력', key='list_up_budget_data_search_term')
            if search_term:
                list_up_budget_data_filtered = list_up_budget_data[list_up_budget_data[key_column].str.contains(search_term, case=False, na=False)]
            else:
                list_up_budget_data_filtered = list_up_budget_data

        st.markdown("---")
        list_up_budget_data_display = list_up_budget_data_filtered[list_up_budget_data_filtered['삭제'] == False]
        st.write(f"{len(list_up_budget_data_display)} 건")

        list_up_budget_edited_data = st.data_editor(
            list_up_budget_data_display,
            hide_index=True,
        )

        # 원본 데이터 프레임 업데이트: 인덱스를 기준으로 편집된 데이터 반영
        list_up_budget_data.loc[list_up_budget_edited_data.index, :] = list_up_budget_edited_data

        if st.button('지자체 저장'):
            utils.save_dataframe_to_bigquery(list_up_budget_data, 'DATA_MARTS', 'list_up_budget_data')
            utils.log_user_action(st.session_state['username'], "save list 지자체 현황", "SERVICE_DATA", "logs")

            st.success('지자체 예산 현황이 성공적으로 저장되었습니다.')


    with tab2:
        st.markdown("---")
        st.subheader("교육청 예산 현황")
        st.markdown("---")

        # CSV 업로드
        uploaded_file = st.file_uploader("교육청 예산 CSV 파일 업로드", type="csv", key='list_up_edu_budget_file_uploader')

        utils.save_dataframe_to_bigquery(list_up_edu_budget_data, 'list_up_data_backup', f'list_up_edu_budget_data_{today}')

        if uploaded_file is not None:
            # 업로드된 CSV 파일 읽기
            uploaded_df = pd.read_csv(uploaded_file)

            # 업로드된 데이터로 기존 데이터프레임 완전히 대체
            list_up_edu_budget_data = uploaded_df
            list_up_edu_budget_data = list_up_edu_budget_data.sort_values(by=['도광역시', '시군구'])

            utils.save_dataframe_to_bigquery(list_up_edu_budget_data, 'DATA_MARTS', 'list_up_edu_budget_data')
            utils.log_user_action(st.session_state['username'], "save list 교육청 현황 csv uploaded", "SERVICE_DATA", "logs")

        st.markdown("---")

        numeric_columns = ['금액', '면적']

        for column in numeric_columns:
            list_up_edu_budget_data[column] = list_up_edu_budget_data[column].replace('None', np.nan)
            list_up_edu_budget_data[column] = pd.to_numeric(list_up_edu_budget_data[column].astype(str).str.replace(',', ''), errors='coerce')

        key_column = st.selectbox('필터링할 열 선택', list_up_edu_budget_data.columns, index=list_up_edu_budget_data.columns.get_loc('과업명'), key='list_up_edu_budget_data_key_column')

        if key_column in numeric_columns:
            min_value = float(list_up_edu_budget_data[key_column].min())
            max_value = float(list_up_edu_budget_data[key_column].max())
            value_range = st.slider(f'{key_column}에서 검색할 범위 선택',
                                    min_value=min_value,
                                    max_value=max_value,
                                    value=(min_value, max_value),
                                    key='list_up_edu_budget_data_value_range')

            list_up_edu_budget_data_filtered = list_up_edu_budget_data[(list_up_edu_budget_data[key_column] >= value_range[0]) & (list_up_edu_budget_data[key_column] <= value_range[1])]
        else:
            search_term = st.text_input(f'{key_column}에서 검색할 내용 입력', key='list_up_edu_budget_data_search_term')
            if search_term:
                list_up_edu_budget_data_filtered = list_up_edu_budget_data[list_up_edu_budget_data[key_column].str.contains(search_term, case=False, na=False)]
            else:
                list_up_edu_budget_data_filtered = list_up_edu_budget_data

        st.markdown("---")
        list_up_edu_budget_data_display = list_up_edu_budget_data_filtered[list_up_edu_budget_data_filtered['삭제'] == False]
        st.write(f"{len(list_up_edu_budget_data_display)} 건")

        list_up_edu_budget_edited_data = st.data_editor(
            list_up_edu_budget_data_display,
            hide_index=True,
        )

        list_up_edu_budget_data.loc[list_up_edu_budget_edited_data.index, :] = list_up_edu_budget_edited_data

        if st.button('교육청 저장'):
            utils.save_dataframe_to_bigquery(list_up_edu_budget_data, 'DATA_MARTS', 'list_up_edu_budget_data')
            utils.log_user_action(st.session_state['username'], "save list 교육청 현황", "SERVICE_DATA", "logs")

            st.success('교육청 예산 현황이 성공적으로 저장되었습니다.')