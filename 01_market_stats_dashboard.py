# Databricks notebook source
# DBTITLE 1,Market Stats Dashboard - Dynamic & Frozen (Portfolio Version)
# End-to-end pipeline powering National Market Stats + Databricks Genie Spaces

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, lower, trim, regexp_replace, regexp_extract, concat
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG        = "your_company.analytics"
TARGET_TABLE   = f"{CATALOG}.market_stats_dashboard"
PRE_STATS_VIEW = f"{CATALOG}.pre_market_stats"
COSTAR_IND     = f"{CATALOG}.costar_industrial"
COSTAR_OFF     = f"{CATALOG}.costar_office"
LSA_TABLE      = f"{CATALOG}.costar_leasing_activity"

VOLUME_PATH    = "/Volumes/your_company/volume/data/"
SP_COSTAR_IND  = "Costar/Industrial"
SP_COSTAR_OFF  = "Costar/Office"
SP_LSA_IND     = "Costar/Leasing Activity/Industrial"
SP_LSA_OFF     = "Costar/Leasing Activity/Office"

# CoStar column type casts
COSTAR_CASTS = {
    "RBA": "long", "DirectVacant": "long", "VacantSubLease": "long",
    "VacantSF": "long", "DirectAvailable": "long", "SubleaseAvailable": "long",
    "AvailableSF": "long", "OccupiedSF": "long", "Quarter": "int",
}

# COMMAND ----------

# DBTITLE 1,MARKET_ROLLUP_CONFIG — Single Source of Truth
MARKET_ROLLUP_CONFIG = {
    "Office": {
        "NY - New York City": [
            "NY - New York City - Downtown",
            "NY - New York City - Midtown",
            "NY - New York City - Midtown South"
        ],
        "CA - Los Angeles": [
            "CA - Los Angeles - Central",
            "CA - Los Angeles - East (San Gabriel)",
            "CA - Los Angeles - Mid-Wilshire/Park Mile",
            "CA - Los Angeles - North",
            "CA - Los Angeles - Tri Cities",
            "CA - Los Angeles - West",
            "CA - Los Angeles - South Bay"
        ],
        "CA - Northern CA - San Francisco Peninsula": [
            "CA - Northern CA - North Peninsula",
            "CA - Northern CA - South Peninsula"
        ],
        "DC - District of Columbia": [
            "DC - District of Columbia",
            "Washington DC"
        ]
    },
    "Industrial": {
        "CA - Los Angeles": [
            "CA - Los Angeles - Central",
            "CA - Los Angeles - North",
            "CA - Los Angeles - East",
            "CA - Los Angeles - South Bay",
            "CA - Los Angeles - Mid-Counties",
            "West Ventura"
        ],
        "PA - Philadelphia - Greater": [
            "PA - Philadelphia - Greater",
            "DE - Delaware",
            "NJ - Southern"
        ]
    },
    None: {
        "IL - Chicago": ["IL - Chicago", "Chicago"]
    }
}

def _config_to_sql_case_when():
    """Builds SQL CASE WHEN from config (for pre_stats_view)."""
    lines = ["      CASE"]
    for prop_type, mappings in MARKET_ROLLUP_CONFIG.items():
        for output_name, patterns in mappings.items():
            ilike_clauses = "\n          OR ".join(
                f"hist.MajorMarketArea ILIKE '%{p}%'" for p in patterns
            )
            if prop_type is None:
                condition = f"(\n          {ilike_clauses}\n        )"
            else:
                condition = f"hist.PropertyType = '{prop_type}' AND (\n          {ilike_clauses}\n        )"
            lines.append(f"        WHEN {condition} THEN '{output_name}'")
    lines.append("        ELSE hist.MajorMarketArea")
    lines.append("      END")
    return "\n".join(lines)

def normalize_market_area(df):
    """Applies market rollup logic in Python (used in final aggregation)."""
    df = (
        df
        .withColumn("_mkt",   lower(trim(regexp_replace(col("MajorMarketArea"), r"(?i)\s*-\s*costar$", ""))))
        .withColumn("_ptype", lower(trim(col("PropertyType"))))
    )

    expr = col("MajorMarketArea")
    ordered = [(pt, out, pats) for pt, mappings in MARKET_ROLLUP_CONFIG.items() for out, pats in mappings.items()]
    
    for prop_type, output_name, patterns in reversed(ordered):
        pattern_regex = "|".join(p.lower().replace("(", r"\(").replace(")", r"\)") for p in patterns)
        mkt_match = col("_mkt").rlike(pattern_regex)
        cond = mkt_match if prop_type is None else (col("_ptype") == prop_type.lower()) & mkt_match
        expr = when(cond, lit(output_name)).otherwise(expr)

    return df.withColumn("MajorMarketArea", expr).drop("_mkt", "_ptype")

# COMMAND ----------

# DBTITLE 1,Quarterly Stats View
_market_case = _config_to_sql_case_when()

spark.sql(f"""
CREATE OR REPLACE VIEW {PRE_STATS_VIEW} AS
SELECT
  hist.MajorMarketAreaID,
  {_market_case} AS MajorMarketArea,
  hist.Submarket,
  hist.city,
  hist.State,
  hist.Zip,
  CASE WHEN hist.PropertyType = 'Industrial' THEN hist.LogisticType ELSE hist.Class END AS Class,
  CASE WHEN hist.PropertyType = 'Industrial' THEN hist.Class ELSE NULL END AS LogisticType,
  CASE WHEN hist.PropertyType = 'Office' AND hist.Biotech = TRUE AND hist.OnReportLocal = TRUE 
       THEN 'Lab' ELSE hist.PropertyType END AS PropertyType,
  hist.year AS year,
  hist.Quarter,
  hist.Onreportnational,
  hist.ConstructionStatus,
  hist.period,
  hist.QuarterStartDate,
  hist.QuarterEndDate,

  CAST(SUM(hist.NumberBuilding) AS LONG) AS NumberBuilding,
  SUM(hist.RBA) AS RBA,
  SUM(hist.DirectVacant) AS DirectVacant,
  SUM(hist.VacantSubLease) AS VacantSubLease,
  SUM(hist.VacantSF) AS VacantSF,
  SUM(hist.DirectAvailable) AS DirectAvailable,
  SUM(hist.SubleaseAvailable) AS SubleaseAvailable,
  SUM(hist.AvailableSF) AS AvailableSF,
  SUM(hist.OccupiedSF) AS OccupiedSF,
  SUM(hist.QtrNetAbsorption) AS QtrNetAbsorption,
  SUM(hist.YTDNetAbsorption) AS YTDNetAbsorption,
  SUM(hist.Deliveries) AS Deliveries,
  SUM(hist.RBADeliveries) AS RBADeliveries,
  SUM(hist.NumberUnderConst) AS NumberUnderConst,
  SUM(hist.RBANumberUnderConst) AS RBANumberUnderConst,

  SUM(hist.WeightedRate) AS WeightedRate,
  SUM(hist.WeightBldgSize) AS WeightBldgSize,
  SUM(hist.SubleaseStandardizedRent) AS SubleaseStandardizedRent,
  SUM(hist.WeightedRateSubLet) AS WeightedRateSubLet,
  SUM(hist.WeightBldgSizeSubLet) AS WeightBldgSizeSubLet,
  SUM(hist.TotalWeightedRate) AS TotalWeightedRate,
  SUM(hist.TotalWeightedBldgSize) AS TotalWeightedBldgSize,

  CASE
    WHEN hist.PropertyType = 'Industrial' THEN
      CASE
        WHEN hist.RBA BETWEEN 0 AND 100000 THEN '0-100K'
        WHEN hist.RBA BETWEEN 100001 AND 300000 THEN '100K-300K'
        WHEN hist.RBA BETWEEN 300001 AND 500000 THEN '300K-500K'
        WHEN hist.RBA BETWEEN 500001 AND 700000 THEN '500K-700K'
        WHEN hist.RBA BETWEEN 700001 AND 999999 THEN '700K-1MM'
        ELSE '1MM+'
      END
    ELSE NULL
  END AS PropertySizeBucket

FROM your_company.raw.prop_hist hist
GROUP BY ...  -- (group by columns same as original)
""")

# COMMAND ----------

# DBTITLE 1,SharePoint Client & Helper Functions
# %run "/Workspace/Repos/.../Sharepoint_Folder_Download"   # ← Replace with your function path

# (Keep your existing helper functions: _clean_col_names, _null_string_cols, load_excel_files, load_lsa_files, etc.)

# COMMAND ----------

# DBTITLE 1,Main Pipeline Execution
# ... (All your ingestion, Costar Industrial/Office tables, LSA loading, market validation, etc.)

# COMMAND ----------

# DBTITLE 1,Final Aggregation & Dashboard Table
# Your full aggregation logic with filters, conditional sums, YTD window, etc.

final_df = normalize_market_area(final_df)

final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"✅ Market Stats Dashboard successfully updated → {TARGET_TABLE}")
