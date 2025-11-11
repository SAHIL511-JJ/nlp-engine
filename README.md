## NLP Query Engine

Natural language query system for mixed structured employee data and unstructured documents. Features dynamic schema discovery, hybrid (SQL + documents) results, caching, pagination, metrics, and export.

GitHub repository: [nlp-engine](https://github.com/SAHIL511-JJ/nlp-engine.git)
LOOM VIDEO DEMO OF THE PEROJECT - https://www.loom.com/share/041e060062e341e7960e7a53bbd14ef2?sid=8fc0625f-593e-4e5a-a73c-007a88a3fac8
### Features
- Dynamic schema discovery (no hard-coded tables/columns)
- Query classification and hybrid results
- Document ingestion (PDF/DOCX/TXT/CSV) with TF‑IDF ranking
- Caching, pagination, metrics, CSV/JSON export
- Minimal frontend to connect DB, upload docs, and run queries

---

## 1) Prerequisites
- Python 3.10+ (works on 3.13)
- Browser (to open `frontend/index.html`)
- Optional: PostgreSQL/MySQL if you want a non-SQLite DB

---

## 2) Quick Start (Ubuntu / WSL / Linux VM)

```bash
cd /home/sasuke/deep2   # or your cloned path
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

Open the UI by double-clicking `frontend/index.html` (or drag it into a browser tab).

In the UI:
- Paste a DB URL (e.g., `sqlite:///./company.db` or `postgresql://user:pass@host:5432/dbname`)
- Click “Connect & Get Schema”
- Upload your documents via “Upload Documents” (PDF/DOCX/TXT/CSV)
- Enter a query and click “Search”

Health checks (optional):
```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/schema
```

---

## 3) Quick Start (Windows 10/11)

1. Install Python 3.10+ from the Microsoft Store or python.org
2. Open Command Prompt or PowerShell and run:
```powershell
cd C:\path\to\deep2
py -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

3. Open `frontend\index.html` in your browser.
4. Paste DB URL, connect, upload docs, and query as above.

Notes for Windows:
- If using PostgreSQL/MySQL, ensure the client libraries are installed and the URL is reachable.

---

## 4) Quick Start (Ubuntu VM)

Inside the VM:
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
cd /path/to/deep2
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
sqlite:///./company.db

```

If accessing from host → VM, find the VM IP:
```bash
ip addr | grep inet
```
Set API base in the UI to `http://<vm-ip>:8000` if needed (default is `http://localhost:8000`).

---

## 5) Configuration
- Default DB: `sqlite:///./company.db` (auto-initialized with demo data)
- Documents DB: `documents.db` (auto-created when uploading/creating samples)
- Config file: `backend/config.yml` (pool size, cache, logging)

Environment example (Postgres):
```
DATABASE_URL=postgresql://user:pass@localhost:5432/company_db
```

---

## 6) API Overview
- POST `/api/connect-database` { connection_string }
- GET `/api/schema`
- POST `/api/ingest/documents` (multipart files) or without files to create samples
- GET `/api/ingestion-status/{job_id}`
- POST `/api/query` { query, page, page_size, use_cache }
- GET `/api/query/history`
- GET `/api/metrics`
- GET `/api/export/csv?q=...`, `/api/export/json?q=...`

Swagger UI: `http://localhost:8000/docs`

---

## 7) Example Queries
SQL-focused
- How many employees do we have?
- Average salary by department
- Show employees in Engineering
- Show the highest paid employees
- Show the most recent hires
- List all departments

Documents (after uploading your docs)
- Find resumes mentioning python
- Resumes mentioning java
- Documents mentioning kubernetes
- Show performance reviews for engineers
- Find documents mentioning SQL and React

Pagination (send in POST body)
```
{
    "query": "average salary by department",
    "page": 1,
    "page_size": 25
}
```

---

## 8) Development
Run tests:
```bash
. .venv/bin/activate   # Windows: .\.venv\Scripts\activate
pytest -q
```

Code structure:
```
backend/
  api/routes/
  services/
  main.py
frontend/
  index.html
```

---

## 9) Troubleshooting
- Backend won’t start: ensure venv is active and requirements installed
- DB connect fails: verify the connection string and that the DB is reachable
- Uploaded docs not appearing: re-run the query; uploads are synchronous (immediate). Check `documents.db` exists and has rows.
- CORS: UI is a static file; backend allows all origins by default.

---

## 10) License
MIT (or update per your needs)
