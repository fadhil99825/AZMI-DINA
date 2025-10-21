# 
Muhammad ‘Azmilfadhil S - 2042231003
Dina Nur Shadrina - 2042231026

Modbus RTU-Based Multi-Sensor and Multi-Actuator Integration for Distributed Control System using Embedded Rust

 **Anggota Kelompok**

Muhammad ‘Azmilfadhil Syamsudin — NRP 2042231003

Dina Nur Shadrina — NRP 2042231026

 **Deskripsi Umum Proyek**

Proyek ini merupakan implementasi nyata sistem kontrol terdistribusi (Distributed Control System / DCS) berbasis Embedded Rust yang mengintegrasikan multi-sensor dan multi-aktuator melalui protokol Modbus RTU, dengan dukungan integrasi simulasi DWSIM, penyimpanan data pada InfluxDB, serta konektivitas ThingsBoard Cloud melalui MQTT.

Sistem ini memanfaatkan ESP32-S3 sebagai edge gateway untuk membaca dan mengontrol perangkat lapangan (sensor dan aktuator) secara real-time. Data dari edge gateway dikirim ke komputer melalui komunikasi serial, diintegrasikan dengan data simulasi proses dari DWSIM, dan selanjutnya disimpan ke dalam InfluxDB. Pada tahap backend, program berbasis Rust bertugas untuk membaca data dari database dan mendistribusikannya ke ThingsBoard Cloud, sehingga pengguna dapat memantau kondisi sistem secara real-time melalui dashboard cloud.

**Spesifikasi Teknis dan Ketentuan Proyek**
 **Bahasa Pemrograman**
 
Semua kode ditulis dalam Rust dengan praktik terbaik: struktur modular, dokumentasi fungsi, dan error handling.
Integrasi eksternal menggunakan Python (dwsim.py) untuk komunikasi dengan simulator DWSIM.

 **Komponen dan Teknologi**
 
Hardware: ESP32-S3 (Edge Gateway)
Protocol: Modbus RTU (sensor & aktuator), Serial, MQTT (cloud)
Database: InfluxDB
Cloud Platform: ThingsBoard
Simulation Tool: DWSIM
Backend Language: Rust

**Tujuan utama proyek ini adalah agar mahasiswa mampu:**

Mendesain dan memprogram ESP32-S3 sebagai edge gateway menggunakan Embedded Rust.
Mengintegrasikan multi-sensor dan multi-aktuator berbasis Modbus RTU.
Melakukan streaming data sensor melalui komunikasi serial ke PC dan mengintegrasikannya dengan data simulasi DWSIM.
Menyimpan hasil integrasi ke InfluxDB untuk analisis lebih lanjut.
Mengembangkan backend Rust yang mengirimkan data ke ThingsBoard Cloud menggunakan protokol MQTT.
