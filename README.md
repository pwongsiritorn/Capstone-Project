# Capstone-Project
Build a data pipeline to extract Air Quality Index (AQI) data in Bangkok
โปรเจกต์นี้มีวัตถุประสงค์เพื่อสร้าง ระบบเก็บข้อมูลคุณภาพอากาศ (Air Quality Index - AQI) ของกรุงเทพแบบอัตโนมัติ โดยใช้ Apache Airflow 

ขั้นตอนการทำงานคือ ดึงข้อมูล AQI จาก AirVisual API ตรวจสอบและแปลงข้อมูล (Data Validation & Transformation) แล้วนำข้อมูลเก็บในฐานข้อมูล PostgreSQL โดย Pipeline นี้สามารถทำงานอัตโนมัติทุก ๆ 3 ชั่วโมง 
