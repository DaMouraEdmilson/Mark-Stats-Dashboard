# Market Stats Dashboard + Databricks Genie Spaces

**End-to-end Databricks analytics pipeline** I built as Analytics Engineer at Newmark (Global Commercial Real Estate). 

This became one of the **flagship products** for the National Research team.

### 🎯 Business Impact
- Reduced analyst manual effort by **up to 80%**
- Powers national Market Stats reporting for Office and Industrial sectors
- Feeds interactive Sigma dashboards and **Databricks Genie Spaces** (AI conversational analytics)

### Screenshots

![Market Stats Dashboard - Office View](Market%20Stats%20Dashboard%20Image.png)  
*Aggregated Market Statistics (Office)*

![Frozen Market Stats Dashboard](Frozen%20Market%20Stats%20Dashboard%20Image.png)  
*Frozen / Report View*

### What The Pipeline Does
- Automates ingestion of CoStar data from SharePoint (Industrial + Office + Leasing Activity files)
- Combines internal NDX data with external sources into a clean unified market stats table
- Handles complex market rollups, weighted rents, absorption, deliveries, and property size bucketing
- Optimized data model for **Databricks Genie** natural language queries and Excel/ODBC connections

### Key Technical Features
- Configurable market normalization (single source of truth in Python + SQL)
- Robust SharePoint automation with error handling and validation tables
- Leasing Activity integration with market mismatch warnings
- Production-grade Delta Lake pipeline using Window functions for YTD calculations

### Tech Stack
- **Databricks** (PySpark, Delta Lake, SQL, Genie Spaces, Notebooks)
- Python + Spark for ETL/ELT
- SharePoint automation
- Sigma dashboards
- Advanced SQL & Data Modeling

---
**This project demonstrates full ownership** of a complex, business-critical analytics platform from raw data ingestion to AI-powered consumption.
