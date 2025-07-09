# 🏥 Ethiopian Medical Telegram Insights Pipeline

This project builds a robust, end-to-end data platform to extract and analyze insights from Ethiopian medical-related Telegram channels. It uses modern ELT principles to process text and images, enrich the data, and expose analytical insights via an API.

---

## 🚀 Features

- Scrape public Telegram channels using `Telethon`
- Extract messages, metadata, and images
- Load raw data into a PostgreSQL data warehouse
- Transform and model data using `dbt` (star schema)
- Enrich data using object detection (`YOLO`) on medical product images
- Expose cleaned insights via a RESTful API using `FastAPI`
- Orchestrate the entire pipeline using `Dagster`
- Containerized environment using Docker + `docker-compose`

---

## 🗂 Project Structure

├── app/ # Source code
│ ├── scraping/ # Telegram scraping logic
│ ├── db/ # DB models and dbt scripts
│ ├── api/ # FastAPI endpoints
│ ├── yolo/ # Object detection code
│ └── utils/ # Helper functions
├── data/ # (Ignored) raw/staged data
├── .env # Secrets and credentials (ignored)
├── .gitignore
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md

---

## ⚙️ Setup & Installation

### 1. Clone the repository
```bash
git clone https://github.com/jonyz1/telegram-medical-data-pipeline.gitgit
cd telegram-medical-data-pipeline

 add a .env file 
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
DB_HOST=db
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_db_password
DB_NAME=telegram_data

  then run this 
docker-compose up --build
