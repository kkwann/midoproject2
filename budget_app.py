# -*- coding: utf-8 -*-
import streamlit as st

import pandas as pd
from datetime import datetime, timedelta

import utils

def filter_data(df, key_prefix):
    key_column_index = df.columns.get_loc('세부사업명')
    key_column = st.selectbox(
        '필터링할 열 선택',
        df.columns,
        index=key_column_index,
        key=f'selectbox_{key_prefix}'
    )

    if pd.api.types.is_numeric_dtype(df[key_column]):
        min_value = float(df[key_column].min()) if not df[key_column].isna().all() else 0.0
        max_value = float(df[key_column].max()) if not df[key_column].isna().all() else 0.0
        value_range = (min_value, max_value) if min_value < max_value else (0.0, max_value)

        min_value, max_value = st.slider(f'{key_column}에서 검색할 범위 선택',
                                min_value=min_value,
                                max_value=max_value,
                                value=value_range,
                                key=f'slider_{key_prefix}')

        filtered_df = df[(df[key_column] >= min_value) & (df[key_column] <= max_value)]

    else:
        search_term = st.text_input(f'{key_column}에서 검색할 내용 입력', key=f'text_input_{key_prefix}')
        if search_term:
            filtered_df = df[df[key_column].str.contains(search_term, case=False, na=False)]
        else:
            filtered_df = df

    st.write(f"{len(filtered_df)} 건")
    st.dataframe(filtered_df, hide_index=True)

def budget_app():
    budget_df = utils.load_budget_data()
    new_budget_df, latest_budget_df = utils.load_latest_budget_data()

    today = datetime.now().date()
    date_range = today - timedelta(days=30)

    st.header("지자체 예산서")
    st.markdown("---")

    tab1, tab2 = st.tabs(["최근 등록된 지자체 예산서", "전체 지자체 예산서"])

    with tab1:
        st.markdown("---")
        st.subheader(f"금일 지자체 예산서 ({today})")
        st.markdown("---")
        filter_data(new_budget_df, 'new_budget_df')

        st.markdown("---")
        st.subheader(f"최근 등록된 지자체 예산서 ({date_range} ~ {today})")
        st.markdown("---")
        filter_data(latest_budget_df, 'latest_budget_df')


    with tab2:
        st.markdown("---")
        st.subheader("전체 지자체 예산서")
        st.markdown("---")
        filter_data(budget_df, 'budget_df')