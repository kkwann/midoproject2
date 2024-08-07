# -*- coding: utf-8 -*-
import streamlit as st

import pandas as pd

import utils

def edu_budget_app():
    edu_budget_df = utils.load_edu_budget_data()

    st.header("교육청 예산서")
    st.markdown("---")

    key_column_index = edu_budget_df.columns.get_loc('과업명')

    key_column = st.selectbox(
        '필터링할 열 선택',
        edu_budget_df.columns,
        index=key_column_index
    )
    if pd.api.types.is_numeric_dtype(edu_budget_df[key_column]):
        min_value = float(edu_budget_df[key_column].min())
        max_value = float(edu_budget_df[key_column].max())
        value_range = st.slider(f'{key_column}에서 검색할 범위 선택',
                                  min_value=min_value,
                                  max_value=max_value,
                                  value=(min_value, max_value),
                                  key='value_range')
        edu_budget_filtered_df = edu_budget_df[(edu_budget_df[key_column] >= value_range[0]) & (edu_budget_df[key_column] <= value_range[1])]

    else:
        search_term = st.text_input(f'{key_column}에서 검색할 내용 입력', key='search_term')
        if search_term:
            edu_budget_filtered_df = edu_budget_df[edu_budget_df[key_column].str.contains(search_term, case=False, na=False)]
        else:
            edu_budget_filtered_df = edu_budget_df

    st.write(f"{len(edu_budget_filtered_df)} 건")
    st.dataframe(
        edu_budget_filtered_df,
        hide_index=True
    )