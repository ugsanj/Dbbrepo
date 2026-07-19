# 🚀 Automated ETL Pipeline: E-Commerce Transaction Orchestration

## 📊 1. Overview
Pipeline ini dirancang untuk memproses, membersihkan, dan merekap data transaksi e-commerce harian secara otomatis. Dengan mengimplementasikan arsitektur *Data Quality Gate*, sistem ini menjamin bahwa hanya data yang 100% valid dan memenuhi standar kualitas tinggi yang akan dimuat ke dalam *Data Warehouse* (simulasi). Pipeline ini mentransformasikan data mentah yang rentan anomali menjadi aset siap pakai bagi tim analis dan pengambil keputusan bisnis.

---

## 📥 2. Extract
*   **📂 Source System:** Repositori penyimpanan lokal (*Data Lake* pendaratan awal).
*   **📄 Data Format:** File `.csv` terpisah (`raw_orders.csv` dan `raw_products.csv`).
*   **📈 Data Volume:** Skala transaksi harian (*Daily batch processing*) dengan rata-rata 130 baris data mentah per hari.
*   **🔍 Initial Audit:** Proses inspeksi awal dilakukan dengan memeriksa jumlah baris, *missing values*, duplikasi data, nilai harga negatif, serta konsistensi entri teks pada kolom kritis (`channel` dan `kota`).

---

## ⚙️ 3. Transform
Proses transformasi diimplementasikan menggunakan Python dan Pandas untuk membersihkan serta memperkaya data (*feature engineering*) melalui langkah-langkah berikut:

*   **🧹 Langkah 1: Deduplikasi Data (`drop_duplicates`)**
    *   *Penjelasan:* Menghapus baris transaksi yang terduplikasi secara identik.
    *   *Kenapa:* Mencegah terjadinya penggandaan nilai finansial (*over-reporting*) saat menghitung total pendapatan (*revenue*).
*   **❌ Langkah 2: Eliminasi Anomali Finansial (`total_harga >= 0`)**
    *   *Penjelasan:* Memfilter dan membuang baris data transaksi yang memiliki nilai `total_harga` di bawah nol.
    *   *Kenapa:* Nilai harga negatif mengindikasikan adanya *system error* atau kerusakan data hulu yang dapat mendistorsi analisis statistik.
*   **🛠️ Langkah 3: Imputasi Nilai Hilang (*Missing Values*)**
    *   *Penjelasan:* Mengisi kolom `customer_email` yang kosong dengan `unknown@placeholder.com` dan kolom `total_harga` yang kosong dengan nilai median data.
    *   *Kenapa:* Menjaga kelengkapan data skema tanpa merusak distribusi nilai asli akibat *outlier* (median lebih aman dibanding *mean*).
*   **📅 Langkah 4: Standarisasi Tipe Tanggal (`to_datetime`)**
    *   *Penjelasan:* Mengonversi tipe data kolom `tanggal_order` menjadi `datetime64[ns]` dengan opsi `format='mixed'`.
    *   *Kenapa:* Menghindari kegagalan *parsing* akibat adanya variasi format penulisan tanggal dari sistem *source* yang berbeda.
*   **🔤 Langkah 5: Normalisasi & Konsistensi Teks (`strip`, `title`, `lower`)**
    *   *Penjelasan:* Menghapus spasi tidak beraturan, mengubah kolom `kota` menjadi *Title Case*, dan kolom `channel` menjadi *lowercase* serta mengganti spasi menjadi *underscore*.
    *   *Kenapa:* Menjamin konsistensi data kualitatif saat dilakukan proses pengelompokan (*aggregation*) di tahap akhir.
*   **✨ Langkah 6: Pengayaan Data (*Feature Engineering*)**
    *   *Penjelasan:* Mengekstrak nama bulan dari tanggal order serta membuat kolom klasifikasi baru `kategori_harga` (kecil, sedang, besar) menggunakan `np.where`.
    *   *Kenapa:* Menyediakan dimensi analisis siap pakai bagi tim bisnis untuk mempercepat pembuatan metrik performa di *dashboard*.

---

## 💾 4. Load
*   **🏛️ Target Destination:** Disimpan ke dalam media penyimpanan siap konsumsi (*Data Warehouse* / *Analytics Layer*). Pada lingkungan produksi nyata, data diarahkan ke tabel *enterprise cloud* seperti Google BigQuery.
*   **💾 Output Artifacts:** Menghasilkan dua berkas `.csv` bersih:
    1.  `orders_clean.csv`: Berisi data terperinci pasca-pembersihan yang memuat 13 kolom utama terpilih.
    2.  `summary_report.csv`: Berisi ringkasan metrik agregat (`total_orders`, `total_revenue`, `avg_revenue`) yang dikelompokkan berdasarkan kategori produk.

---

## 🔗 5. Orchestration
*   **🛠️ Tooling:** Apache Airflow.
*   **⏱️ Scheduling:** Dikonfigurasi untuk berjalan secara otomatis menggunakan *cron expression* `'0 6 * * *'` (Setiap hari pukul 06:00 WIB).
*   **⛓️ Directed Acyclic Graph (DAG) Flow:**
    ```text
    [Start] ──> [Extract Orders] ──> [Transform & Clean] ──> [Validate Quality] ──> [Load to Warehouse] ──> [Generate Report] ──> [Send Notification] ──> [End]
    ```

---

## 🚨 6. Error Handling
*   **⚠️ Skenario 1: Berkas Sumber Data Hilang / Terlambat**
    *   *Solusi:* Jika file `raw_orders.csv` absen, Apache Airflow memicu kebijakan otomatis `retries: 3` dengan jeda waktu `retry_delay: 5 menit`. Dilengkapi dengan algoritma *exponential backoff* pada *script* lokal untuk memberikan toleransi waktu bagi sistem hulu.
*   **🛡️ Skenario 2: Kegagalan Integritas pada Data Quality Gate**
    *   *Solusi:* Jika data hasil transformasi terdeteksi masih melanggar aturan kualitas (misal masih ada nilai kosong atau duplikat), task `validate_quality` sengaja melemparkan `ValueError` untuk menghentikan seluruh sisa aliran pipeline secara paksa. Hal ini menjaga agar data kotor tidak pernah masuk ke *Data Warehouse*.

---

## 🛡️ 7. Monitoring & Observability
*   **📈 Pipeline Success Status:** Sistem menuliskan log kronologis secara *real-time* ke file `pipeline_log.txt` dengan penanda status `[RUNNING]`, `[SUCCESS]`, atau `[FATAL]`. Di akhir eksekusi, fungsi `task_notify` akan mengirimkan laporan notifikasi ringkas (simulasi Slack/Email) berisi volume data yang diproses dan total *revenue*.
*   **🧪 Data Quality Indicators:** Sistem menguji secara ketat 4 indikator utama sebelum proses pemuatan data:
    *   `zero_duplicates` (Harus True)
    *   `zero_nulls` (Harus True)
    *   `zero_negative_price` (Harus True)
    *   `datetime_type` (Harus True)

---

## 📂 8. Project Repository Structure

```text
.
├── dags/                                      # 📜 Folder khusus untuk DAG Apache Airflow
│   ├── __pycache__/
│   │   └── etl_ecommerce_dag.cpython-311.pyc  # ⚙️ Compiled Python file
│   └── etl_ecommerce_dag.py                   # 🛠️ File DAG Airflow Production-Ready
├── data/                                      # 💾 Repositori backup sumber data mentah
│   └── raw_orders.csv                         
├── screenshot/                                # 📸 Bukti eksekusi pipeline & antarmuka Airflow UI
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   ├── Screenshot.png
│   └── Screenshot.png
├── docker-compose.yaml                        # 🐳 Konfigurasi container Apache Airflow lokal
├── orchestrator.py                            # 🚀 Script Mini-Orchestrator Python (Simulasi Airflow)
├── orders_clean.csv                           # ✨ Hasil akhir data utama yang telah dibersihkan
├── pipeline

```

---

**Dokumentasi**

Step 1: Extract
<img width="991" height="402" alt="Screenshot 2026-07-18 at 00 22 50" src="https://github.com/user-attachments/assets/9b4f843a-2188-40ad-b5eb-5d0f4eca5de9" />

<img width="988" height="397" alt="Screenshot 2026-07-18 at 00 23 07" src="https://github.com/user-attachments/assets/3a60fd00-8bf7-4f6a-bf76-1e7f2eaef6df" />

---
Step 2: Data Cleaning
<img width="990" height="275" alt="Screenshot 2026-07-18 at 00 23 22" src="https://github.com/user-attachments/assets/419e0244-c211-4a20-adc0-c32696428193" />

---
Step 3: Validate (All Success)
<img width="985" height="155" alt="Screenshot 2026-07-18 at 00 23 39" src="https://github.com/user-attachments/assets/ad533f55-666f-490c-9e7b-3ca521b2ab87" />

---
Step 4: Load
<img width="895" height="171" alt="Screenshot 2026-07-18 at 22 06 08" src="https://github.com/user-attachments/assets/40297c3a-019c-41f2-8204-39c566e538bc" />

---
Step 5: Orchestrator
<img width="983" height="528" alt="Screenshot 2026-07-18 at 00 24 27" src="https://github.com/user-attachments/assets/e44fa939-2806-4bf7-b7e0-1ec818ce4b04" />

<img width="985" height="220" alt="Screenshot 2026-07-18 at 00 25 26" src="https://github.com/user-attachments/assets/c14574d6-51f6-44a9-85b1-299a83dc3443" />

---
Tampilan DAG Airflow
<img width="1366" height="679" alt="Screenshot 2026-07-18 at 21 55 56" src="https://github.com/user-attachments/assets/4b89bb46-d3c1-4b06-a167-d180191bb458" />

---

**Diagram Arsitektur Pipeline**
```
==================================================================================================

  [ 📥 EXTRACT ]                [ ⚙️ TRANSFORM ]              [ 🛡️ VALIDATE ]          [ 💾 LOAD ]
  
  +------------+
  | raw_orders | --+
  |   (.csv)   |   |
  +------------+   |         +-------------------+         +-----------------+       +--------------+
                   +======>  |  Python / Pandas  | =======>  |  Quality Gate   | ====> | orders_clean |
  +------------+   |         |  - Deduplication  |         |  - Zero Nulls   | [OK]  |    (.csv)    |
  |raw_products| --+         |  - Data Cleansing |         |  - Zero Dupes   |       +--------------+
  |   (.csv)   |             |  - Normalization  |         |  - Valid Type   |              |
  +------------+             +-------------------+         +-----------------+              v
                                                                    |                +--------------+
                                                                 [ERROR]             |summary_report|
                                                                    |                |    (.csv)    |
                                                                    v                +--------------+
                                                           +-----------------+
                                                           | Pipeline Halts  |
                                                           | & Sends Alert   |
                                                           +-----------------+
```
