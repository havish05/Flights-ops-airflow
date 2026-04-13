# ✈️ Airflow Medallion Data Pipeline

This project demonstrates a **Medallion Architecture (Bronze → Silver → Gold)** pipeline using Apache Airflow.

## 🚀 Tech Stack

* Apache Airflow (Docker)
* PostgreSQL
* Python

## 📂 Project Structure

* `dags/` → Airflow DAGs
* `scripts/` → ETL logic
* `data/` → Bronze, Silver, Gold layers

## ⚙️ Setup

```bash
docker-compose up --build
```

Access Airflow UI:

```
http://localhost:8080
```

Login:

```
username: admin
password: admin
```

## 🔄 Pipeline

* Bronze: Data ingestion
* Silver: Transformation
* Gold: Aggregation

## 📌 Notes

* Uses XCom and file-based data flow
* Modular ETL scripts

---

Made for learning Data Engineering 🚀
