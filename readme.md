# 🧾 Privacy-First Expense Tracker — CIBC PDF Parser

A local, privacy-first financial analytics tool that extracts **Payments** and **New Charges** directly from **CIBC credit-card PDF statements**, cleans and normalizes the data, visualizes spend patterns, and optionally stores everything in **PostgreSQL** for long-term analysis.

This project demonstrates **end-to-end data engineering**:
- PDF parsing  
- Data cleaning & normalization  
- Categorization & location extraction  
- Data visualization (Streamlit UI)  
- Database ingestion (PostgreSQL upserts with natural keys)

---

## 🚀 Features

### **🔒 100% Local Parsing (Privacy-First)**
All PDF parsing and processing happens on your machine.  
Nothing is uploaded anywhere.

### **📄 Robust PDF Statement Parsing**
- Extracts **Payments**  
- Extracts **New charges & credits**  
- Cleans merchant names  
- Detects **city + province** when possible  
- Normalizes ambiguous dates and invalid calendar days

### **📊 Interactive Dashboard (Streamlit)**
- Filter by date range  
- Filter by spending categories  
- Search by description  
- Daily spend (line chart)  
- Spend by category (bar chart)  
- Top merchants (automatically derived)

### **🗃️ PostgreSQL Integration**
- Schema: `expense`  
- Tables: `transactions`, `payments`  
- Idempotent ingestion using:
  - Deterministic `natural_key`
  - `ON CONFLICT DO NOTHING`  
- Docker-friendly connection via environment variables

---

## 🧱 Project Structure

```
.
├── app.py                          # Streamlit application
├── requirements.txt
├── README.md
├── src
│   ├── parsing
│   │   └── cibc_pdf_parser.py      # PDF parsing, date normalization, location extraction
│   └── storage
│       └── db.py                   # Postgres DDL, connection helper, upsert logic
└── .gitignore
```

---

## 🐳 Running PostgreSQL in Docker

```bash
docker run -d \
  --name pg-finance \
  -e POSTGRES_USER=user \
  -e POSTGRES_PASSWORD=123 \
  -e POSTGRES_DB=postgres \
  -p 54321:5432 \
  postgres:16
```

### Create the application database

Connect via DBeaver or psql:

```sql
CREATE DATABASE personal_finance_tracker_db
    ENCODING 'UTF8'
    TEMPLATE template0;
```

The app will auto-initialize schema & tables when you click **Initialize DB**.

---

## ⚙️ Environment Variables

The app reads DB connection details from:

```
POSTGRES_HOST        (default: localhost)
POSTGRES_PORT        (default: 54321)
POSTGRES_DB          (default: personal_finance_tracker_db)
POSTGRES_USER        (default: user)
POSTGRES_PASSWORD    (default: 123)
```

Example setup:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=54321
export POSTGRES_DB=personal_finance_tracker_db
export POSTGRES_USER=user
export POSTGRES_PASSWORD=123
```

---

## 📦 Install Dependencies

From project root:

```bash
pip install -r requirements.txt
```

Minimal `requirements.txt`:

```
streamlit
pandas
pdfplumber
python-dateutil
psycopg2-binary
```

---

## ▶️ Run the App

```bash
streamlit run app.py
```

You'll see a link like:

```
Local URL: http://localhost:8501
```

---

## 🧠 How the ETL Pipeline Works

### 1. Extract

`pdfplumber` loads each PDF page → finds tables → normalizes multiline rows → extracts:
- transaction date
- posting date
- merchant description
- category
- amount

### 2. Transform

Custom logic handles:
- Fixing broken dates (e.g., "Feb 30")
- Normalizing merchant strings
- Detecting location from description (city + province)
- Generating a deterministic `natural_key`

### 3. Load

`src/storage/db.py` handles:
- Postgres schema + DDL
- Batch inserts using `execute_values`
- `ON CONFLICT DO NOTHING` to avoid duplicates

This ensures re-uploading the same PDF won't double-insert data.

---

## 🛣️ Roadmap

- Add merchant name normalization via ML or rule-based mapping
- Add monthly insights + budget planning
- Add OCR fallback for scanned PDFs
- Add API endpoints for programmatic ingestion
- Deploy via Docker Compose (app + DB)

---

## 📝 Disclaimer

- This project is not affiliated with CIBC.
- For personal use only. Always verify parsed statements manually.

---

## ⭐ If you like this project

Give the repo a star ⭐ — it helps others discover it and motivates future updates.

---

## ✅ FINAL STEP

Create the README file:

```bash
cd ~/Developer/finance-tracker
touch README.md
```

Paste the full content above into the file → save → then:

```bash
git add README.md
git commit -m "Add README.md"
git push
```