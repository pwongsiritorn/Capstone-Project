-- 1.วันที่ไหน AQI สูงสุด
SELECT * FROM bangkok_aqi ORDER BY aqi DESC LIMIT 1;

-- 2.ค่าความชื้นเฉลี่ย
SELECT AVG(humidity) FROM bangkok_aqi;

-- 3.อุณหภูมิสูงสุด
SELECT MAX(temperature) FROM bangkok_aqi;

-- 4 จำนวนวัน AQI > 100
SELECT COUNT(*) FROM bangkok_aqi WHERE aqi > 100;

-- 5 ค่า AQI เฉลี่ยรายวัน
SELECT DATE(timestamp) AS day, AVG(aqi) FROM bangkok_aqi GROUP BY day ORDER BY day;