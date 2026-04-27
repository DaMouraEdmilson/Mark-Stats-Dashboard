# Market Stats Dashboard + Databricks Genie Spaces

**Production analytics platform** I built at Newmark (Global Commercial Real Estate) that became a **flagship tool** for the national research team.

### Impact
- Saved analysts **up to 80%** of their time on market reporting and analysis.
- Powers interactive dashboards (Sigma) and **Databricks Genie Spaces** (AI conversational analytics).
- Used nationally across Office and Industrial sectors.

### What This Project Does
- Automated ingestion of CoStar data from SharePoint (Industrial + Office + Leasing Activity).
- Built a clean, unified **Golden Record** and market stats table with complex market rollups, weighted rents, absorption, deliveries, and property size bucketing.
- Optimized data model specifically for **Databricks Genie** (natural language queries) and Excel/ODBC connections.
- Includes robust error handling, validation tables, and market mismatch warnings.

### Technologies Used
- **Databricks** (PySpark, Delta Lake, SQL, Notebooks, Genie Spaces)
- Python + Spark for ETL/ELT
- SharePoint automation
- Sigma + Power BI dashboards
- Advanced SQL, Data Modeling, Metrics Layer

### Key Technical Highlights
- Configurable market rollup logic (single source of truth)
- Smart handling of CoStar vs internal NDX data
- Leasing Activity integration with validation
- YTD calculations using Window functions
- Production-grade pipeline (idempotent, logged, validated)

### Files
- `01_market_stats_dashboard.py` ← Main pipeline (this is the core file)

### Screenshots
*(Add 2–4 screenshots here of the Sigma dashboard and Genie Spaces once you upload them)*

---

**This project demonstrates end-to-end ownership** of a complex analytics platform used by dozens of researchers daily.
