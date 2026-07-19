**ETL Pipeline Design: E-Commerce Orders**

1. **Overview**
Pipeline ini memproses data transaksi e-commerce harian secara otomatis untuk menghasilkan data siap pakai bagi tim bisnis dan analis. Sistem ini mengonversi data mentah yang rentan terhadap duplikasi, missing values, dan anomali format menjadi tabel data bersih (clean data) terpusat, serta otomatis memproduksi summary report performa per kategori produk.

Pipeline ini dirancang tangguh (robust) dengan arsitektur orkestrasi yang mengutamakan keamanan data melalui mekanisme Validation Gate sebelum proses pemuatan data terjadi.

2. **Extract**
Sumber: File repositori penyimpanan lokal (data lakehouse simulasi/pendaratan awal).
Format: File .csv terpisah (raw_orders.csv untuk data transaksi dan raw_products.csv untuk data master produk).
Volume: Skala transaksi harian (daily batch processing) dengan rata-rata 130 baris data mentah per hari sebagai simulasi operasional.

3. **Transform**
Proses transformasi diimplementasikan menggunakan Python dan Pandas melalui langkah-langkah terstruktur berikut:

Langkah 1: Deduplikasi Data (drop_duplicates)

Penjelasan: Menghapus baris transaksi yang terduplikasi secara identik.

Kenapa: Menghindari penggandaan metrik finansial (over-reporting) saat menganalisis pendapatan (revenue).

Langkah 2: Pembersihan Anomali Finansial (total_harga >= 0)

Penjelasan: Memfilter dan membuang baris data yang memiliki nilai total_harga di bawah nol.

Kenapa: Nilai harga negatif merupakan system error atau korupsi data pada source yang dapat merusak akurasi perhitungan rata-rata pendapatan.

Langkah 3: Imputasi Nilai Hilang (fillna)

Penjelasan: Mengisi kolom customer_email yang kosong dengan unknown@placeholder.com dan kolom total_harga yang kosong dengan nilai median data.

Kenapa: Menjaga integritas skema data agar tidak error saat dianalisis, tanpa mendistorsi distribusi nilai asli secara ekstrem (menggunakan median lebih aman terhadap pencilan dibanding mean).

Langkah 4: Standarisasi Format Tanggal (to_datetime)

Penjelasan: Mengubah tipe data kolom tanggal_order menjadi format datetime64[ns] dengan parameter format='mixed'.

Kenapa: Menghindari kegagalan parsing akibat variasi format penulisan tanggal dari sistem source yang berbeda.

Langkah 5: Normalisasi Teks dan String (strip, title, lower)

Penjelasan: Menghapus spasi tak beraturan, mengubah kolom kota menjadi Title Case, dan kolom channel menjadi lowercase serta mengganti spasi menjadi underscore.

Kenapa: Menjamin konsistensi data kualitatif saat dilakukan proses pengelompokan (GROUP BY) di tahap analisis.

Langkah 6: Pengayaan Data / Feature Engineering

Penjelasan: Ekstraksi nama bulan dari tanggal order dan pembuatan kolom turunan kategori_harga (kecil, sedang, besar) berbasis pengondisian np.where.

Kenapa: Menyediakan dimensi analisis baru yang siap pakai bagi user bisnis tanpa perlu melakukan komputasi ulang di sisi dashboard.

4. **Load**
Tujuan: Disimpan ke dalam media penyimpanan terstruktur siap konsumsi (Data Warehouse simulasi). Pada level produksi sesungguhnya, data diarahkan langsung ke tabel enterprise cloud seperti Google BigQuery.

Format Output: Dua file .csv bersih, yaitu:

orders_clean.csv: Berisi data granular hasil pembersihan yang memuat 13 kolom utama terpilih.

summary_report.csv: Berisi ringkasan metrik agregat (total_orders, total_revenue, avg_revenue) yang dikelompokkan berdasarkan kategori produk.

5. **Orchestration**
Tool: Apache Airflow (diimplementasikan menggunakan komparasi Local Mini-Orchestrator untuk simulasi cepat dan Airflow DAG File siap produksi).

Schedule: Diatur berjalan secara otomatis menggunakan cron expression '0 6 * * *' (Setiap hari pukul 06:00 WIB).

DAG Flow:

[Start] >> [Extract Orders] >> [Transform & Clean] >> [Validate Quality] >> [Load to Warehouse] >> [Generate Report] >> [Send Notification] >> [End]


6. **Error Handling**
Skenario 1: File Sumber Data Mentah Tidak Ditemukan

Penjelasan & Solusi: Jika raw_orders.csv absen, sistem memicu FileNotFoundError. Apache Airflow dikonfigurasi dengan retries: 3 dan retry_delay: 5 menit dengan exponential backoff di tingkat lokal untuk memberi jeda waktu bagi sistem hulu mengunggah file.

Skenario 2: Kegagalan Validasi Integritas Data (Data Quality Gate)

Penjelasan & Solusi: Jika data hasil transformasi terdeteksi masih mengandung duplikat atau nilai kosong, task validate_quality sengaja melemparkan ValueError dan menghentikan seluruh sisa aliran pipeline secara paksa. Hal ini mencegah data kotor mengontaminasi Data Warehouse.

7. **Monitoring**
Bagaimana cara tahu pipeline sukses?

Sistem menuliskan log kronologis secara real-time ke file pipeline_log.txt dengan penanda status [RUNNING], [SUCCESS], atau [FATAL].

Di akhir eksekusi, fungsi task_notify akan mengirimkan laporan notifikasi ringkas (simulasi Slack/Email) yang menampilkan metrik volume data yang berhasil diproses dan total revenue yang masuk.

Bagaimana cara tahu data berkualitas?

Melalui implementasi Validation Gate otomatis yang melakukan pengujian ketat terhadap 4 indikator utama:

zero_duplicates (Harus True)

zero_nulls (Harus True)

zero_negative_price (Harus True)

datetime_type (Harus True)

Hanya data yang mendapatkan status ✅ Semua check PASSED yang diizinkan melangkah ke proses pengisian repositori akhir.


📂 Struktur Repositori Project

├── dags/
│   ├── __pycache__/
│   │   └── etl_ecommerce_dag.cpython-311.pyc  # Compiled Python file
│   └── etl_ecommerce_dag.py                   # File DAG Airflow Production
├── data/
│   └── raw_orders.csv                         # Backup / Source data mentah harian
├── screenshot/                                # Dokumentasi bukti eksekusi & UI Airflow
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   └── Screenshot.png
├── docker-compose.yaml                        # Konfigurasi container Apache Airflow lokal
├── orchestrator.py                            # Script Mini-Orchestrator Python (Simulasi Airflow)
├── orders_clean.csv                           # Hasil output data yang telah dibersihkan
├── pipeline_log.txt                           # Log kronologis dari jalannya pipeline
├── raw_orders.csv                             # Input data transaksi mentah utama
├── raw_products.csv                           # Input data master produk mentah
├── README.md                                  # Dokumentasi utama proyek ETL
└── summary_report.csv                         # Hasil output report agregat per kategori
