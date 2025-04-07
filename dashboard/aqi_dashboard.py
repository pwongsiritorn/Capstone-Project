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

# -------------------- Business Insights --------------------
st.header("Business Questions Insights")

# 1.วันที่ AQI สูงสุด
max_aqi_row = df.loc[df['aqi'].idxmax()]
st.write(f"วันที่ค่า AQI สูงสุด: **{max_aqi_row['timestamp'].date()}** (AQI = {max_aqi_row['aqi']})")

# 2. ค่าความชื้นเฉลี่ย
avg_humidity = df['humidity'].mean()
st.write(f"ค่าความชื้นเฉลี่ย: **{avg_humidity:.2f}%**")

# 3. ค่า AQI เฉลี่ยรายวัน
st.subheader("ค่า AQI เฉลี่ยรายวัน")
df_daily_avg = df.groupby(df['timestamp'].dt.date)['aqi'].mean().reset_index()
df_daily_avg.columns = ['date', 'avg_aqi']
st.line_chart(df_daily_avg.set_index('date'))

# 4. อุณหภูมิสูงสุด
max_temp = df['temperature'].max()
st.write(f"อุณหภูมิสูงสุด: **{max_temp}°C**")

# 5. จำนวนวันที่ AQI > 80
df_high_aqi_days = df[df['aqi'] > 80]
num_high_aqi_days = df_high_aqi_days['timestamp'].dt.date.nunique()
st.write(f"จำนวนวันที่ AQI > 80: **{num_high_aqi_days} วัน**")


conn.close()
