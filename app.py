# -*- coding: utf-8 -*-
import streamlit as st
from streamlit_option_menu import option_menu

import utils
import list_up_app
import budget_app
import edu_budget_app


import time
import warnings
warnings.filterwarnings("ignore")

def load_users_data():
    users_data = utils.load_users_data()
    return users_data


def login(username, password):
    users_data = load_users_data()
    users = {row['employeeName']: {'jobTitle': row['jobTitle'], 'password': row['password']} for _, row in users_data.iterrows()}

    if username in users:
        stored_password_plain = users[username]['password']
        if password == stored_password_plain:
            st.session_state['logged_in'] = True
            st.session_state['username'] = username
            st.session_state['jobTitle'] = users[username]['jobTitle']

            utils.log_user_action(username, "login", "SERVICE_DATA", "logs")
            return True
    return False


def logout():
    utils.log_user_action(st.session_state['username'], "logout", "SERVICE_DATA", "logs")
    st.session_state['logged_in'] = False
    st.session_state['username'] = None
    st.session_state['jobTitle'] = None


def main():

    st.set_page_config(page_title="Mido_Plus",
                       page_icon=None,
                       layout="wide",
                       initial_sidebar_state="auto",
                       menu_items=None)

    st.markdown("""
        <style>
        .username-jobTitle {
            font-size: 22px;
            font-weight: bold;
        }
        </style>
        """, unsafe_allow_html=True)

    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        st.session_state['username'] = None
        st.session_state['jobTitle'] = None

    if st.session_state['logged_in']:
        with st.sidebar:

            col1, col2 = st.columns([1, 1])
            with col1:
                st.markdown(
                    f"""
                    <span class='username-jobTitle'>{st.session_state['username']} {st.session_state['jobTitle']}</span>님
                    """,
                    unsafe_allow_html=True
                )

            with col2:
                if st.button("로그아웃", key="logout_button"):
                    logout()
                    st.experimental_rerun()

            styles = {
                "container": {"padding": "5!important", "background-color": "#fafafa"},
                "icon": {"color": "orange", "font-size": "25px"},
                "nav-link": {
                    "font-size": "16px",
                    "text-align": "left",
                    "margin": "0px",
                    "--hover-color": "#eee",
                    "color": "#000",
                },
                "nav-link-selected": {"background-color": "#02ab21", "color": "#fff"},
            }

            selected = option_menu("Mido Plus", ["납품 현황", "사업 현황", "지자체 예산서", "교육청 예산서", "인포21C", "종합쇼핑몰 납품상세 내역", "뉴스", "STAT"],
                                   icons=["graph-up-arrow", "list-check", "building", "building", "info-square", "cart4", "pencil-square", "clipboard-data"],
                                   menu_icon="cast",
                                   default_index=0,
                                   orientation="vertical",
                                   key='main_option',
                                   styles=styles,
                                   )

        if selected == "납품 현황":
            utils.log_user_action(st.session_state['username'], "viewed HOME", "SERVICE_DATA", "logs")
            # home_app.home_app()
        elif selected == "사업 현황":
            utils.log_user_action(st.session_state['username'], "viewed list", "SERVICE_DATA", "logs")
            list_up_app.list_up_app()
        elif selected == "지자체 예산서":
            utils.log_user_action(st.session_state['username'], "viewed 지자체 예산서", "SERVICE_DATA", "logs")
            budget_app.budget_app()
        elif selected == "교육청 예산서":
            utils.log_user_action(st.session_state['username'], "viewed 교육청 예산서", "SERVICE_DATA", "logs")
            edu_budget_app.edu_budget_app()
        # elif selected == "인포21C":
        #     utils.log_user_action(st.session_state['username'], "viewed 인포21C", "SERVICE_DATA", "logs")
        #     info21C_app.info21C_app()
        # elif selected == "종합쇼핑몰 납품상세 내역":
        #     utils.log_user_action(st.session_state['username'], "viewed 종합쇼핑몰 납품상세 내역", "SERVICE_DATA", "logs")
        #     g2b_app.g2b_app()
        # elif selected == "뉴스":
        #     utils.log_user_action(st.session_state['username'], "viewed 뉴스", "SERVICE_DATA", "logs")
        #     news_app.news_app()
        # elif selected == "STAT":
        #     utils.log_user_action(st.session_state['username'], "viewed STAT", "SERVICE_DATA", "logs")
        #     stat_app.stat_app()

    else:
        st.write("계속하시려면 로그인하세요.")
        with st.form(key='login_form'):
            username = st.text_input("이름")
            password = st.text_input("비밀번호", type="password")
            login_button = st.form_submit_button("로그인")
            if login_button:
                if login(username, password):
                    st.success("로그인에 성공하였습니다.")
                    st.experimental_rerun()
                else:
                    st.error("이름 또는 비밀번호를 확인해주세요.")

if __name__ == "__main__":
    main()