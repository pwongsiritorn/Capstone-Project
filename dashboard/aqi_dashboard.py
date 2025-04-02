import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# -------------------- Connect PostgreSQL --------------------
conn = psycopg2.connect(
    host="db",
    database="postgres",
    user="postgres",
    password="postgres",
    port="5432"
)

query = "SELECT * FROM bangkok_aqi ORDER BY timestamp DESC"
df = pd.read_sql_query(query, conn)
df['timestamp'] = pd.to_datetime(df['timestamp'])

st.title("Bangkok AQI Dashboard")

# -------------------- Summary Section --------------------
st.header("Summary Report")

# Filter Data
today = datetime.now()
week_start = today - timedelta(days=today.weekday())
three_months_ago = today - timedelta(days=90)

df_week = df[df['timestamp'] >= week_start]
df_3months = df[df['timestamp'] >= three_months_ago]

# 1. Highest AQI this week
highest_aqi_week = df_week['aqi'].max()

# 2. Lowest AQI in last 3 months
lowest_aqi_3months = df_3months['aqi'].min()

# 3. Average AQI this week
average_aqi_week = df_week['aqi'].mean()

# Display
col1, col2, col3 = st.columns(3)
col1.metric("Highest AQI this week", f"{highest_aqi_week}")
col2.metric("Lowest AQI (last 3 months)", f"{lowest_aqi_3months}")
col3.metric("Average AQI this week", f"{average_aqi_week:.2f}")

# -------------------- Line Chart --------------------
st.subheader("Trend AQI / Temperature / Humidity")
st.line_chart(df.set_index("timestamp")[['aqi', 'temperature', 'humidity']])

# -------------------- Extra Chart --------------------
st.subheader("AQI Time Series (Past 3 Months)")
plt.figure(figsize=(10,5))
plt.plot(df_3months['timestamp'], df_3months['aqi'], marker='o')
plt.xlabel("Date")
plt.ylabel("AQI")
plt.grid(True)
st.pyplot(plt)

conn.close()
