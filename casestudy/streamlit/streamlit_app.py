import streamlit as st
import pandas as pd
from authentification import check_login
import json
from get_team_player import load_teams, load_players, load_aggregated_data

st.set_page_config(
    page_title="Football Data Explorer",
    layout="wide"
)

if not check_login():
    st.stop()

st.title("⚽ Football Data Explorer")
st.sidebar.header("Filters")

teams = sorted(load_teams()["team_name"])
players = sorted(load_players()["player_name"])

selected_team = st.sidebar.selectbox(
    "Select team",
    ["All"] + teams
)

selected_player = st.sidebar.selectbox(
    "Select player",
    ["All"] + players
)

filtered_df = load_aggregated_data(2019026).copy()

if selected_team != "All":
     filtered_df = filtered_df[
         filtered_df["team_name"] == selected_team
    ]

if selected_player != "All":
     filtered_df = filtered_df[
         filtered_df["player_name"] == selected_player
     ]

col1, col2 = st.columns(2)

with col1:
    st.metric("Number of rows", len(filtered_df))

with col2:
    st.metric("Unique players", len(filtered_df["player_name"].unique()))
st.dataframe(filtered_df, use_container_width=True)