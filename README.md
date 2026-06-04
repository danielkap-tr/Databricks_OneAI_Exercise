# Tel Aviv Road Closures Compensation Pipeline 🚧💰

## סקירה כללית

Pipeline לחישוב פיצויים לעסקים עקב סגירות רחובות בתל אביב במהלך 2023.

### תוצאות עיקריות
- 📊 **401 עסקים** קיבלו פיצויים
- 🛣️ **44 רחובות** נסגרו
- 💵 **₪88.6M** סך הפיצויים

---

## ארכיטקטורה - Medallion

```
📁 pipeline/
├── bronze/          # נתונים גולמיים
│   ├── bronze_closures_csv.py       # CSV של סגירות רחובות
│   └── bronze_businesses_api.py     # API של עסקים
│
├── silver/          # ניקוי ונרמול
│   ├── silver_closures.py           # סגירות מנוקות
│   └── silver_businesses.py         # עסקים מנורמלים
│
└── gold/            # מטריקות עסקיות
    ├── gold_annual_compensation.py  # פיצויים שנתיים לכל עסק
    └── gold_street_compensation.py  # פיצויים לכל רחוב
```

---

## טבלאות שנוצרו

### Bronze Layer
- `workspace.default.road_comp_bronze_closures` - נתוני סגירות גולמיים
- `workspace.default.road_comp_bronze_businesses` - נתוני עסקים גולמיים

### Silver Layer
- `workspace.default.road_comp_silver_closures` - סגירות לאחר ניקוי
- `workspace.default.road_comp_silver_businesses` - עסקים לאחר נרמול

### Gold Layer
- `workspace.default.road_comp_gold_annual_compensation` - פיצויים שנתיים (401 שורות)
- `workspace.default.road_comp_gold_street_compensation` - פיצויים לפי רחוב (44 שורות)

---

## חישוב הפיצויים

```python
# לוגיקה עיקרית
daily_compensation = business_type_rate × closure_hours / 24
annual_compensation = SUM(daily_compensation) per business
```

### תעריפים לפי סוג עסק
- מסעדות: ₪1,500/יום
- חנויות: ₪1,000/יום
- משרדים: ₪800/יום
- אחר: ₪500/יום

---

## אוטומציה

### Databricks Job: "Daily Compensation Pipeline"
- ⏰ רץ אוטומטית **כל יום ב-8:00 בבוקר**
- 🔄 מריץ את כל שכבות ה-Pipeline
- 📊 מעדכן את הדשבורד

---

## Dashboard

**"2023 Road Closures Compensation Dashboard"**

Widgets:
1. סך כל הפיצויים (counter)
2. התפלגות לפי סוג עסק (pie chart)
3. Top 10 עסקים (bar chart)
4. פיצויים לפי רחוב (bar chart)
5. טבלאות מפורטות

---

## איך להריץ

### דרישות מקדימות
- Databricks Workspace
- Unity Catalog enabled
- Serverless compute או cluster עם Photon

### התקנה
1. Clone את הrepository
2. העלה את תיקיית `pipeline/` ל-Databricks workspace
3. צור Pipeline חדש:
   - Type: Spark Declarative Pipeline
   - Source: `pipeline/**`
   - Catalog: `workspace`
   - Schema: `default`
   - Serverless: ✅
   - Photon: ✅
4. הרץ את הPipeline

### קבצי נתונים נדרשים
- `rechov_sagur.csv` - נתוני סגירות (בתיקיית data/)
- Business API endpoint מוגדר בקוד

---

## מבנה הקוד

### Bronze Layer
```python
# bronze_closures_csv.py
@dlt.table(name="road_comp_bronze_closures")
def bronze_closures():
    return spark.read.csv("path/to/rechov_sagur.csv", header=True)
```

### Silver Layer
```python
# silver_closures.py
@dlt.table(name="road_comp_silver_closures")
def silver_closures():
    # Data cleaning, type conversion, deduplication
    ...
```

### Gold Layer
```python
# gold_annual_compensation.py
@dlt.table(name="road_comp_gold_annual_compensation")
def gold_annual_compensation():
    # Business logic: join, calculate compensation, aggregate
    ...
```

---

## טכנולוגיות

- **Databricks** - פלטפורמה
- **Delta Lake** - אחסון
- **Unity Catalog** - ניהול metadata
- **Spark Declarative Pipelines** - ETL framework
- **Lakeview Dashboards** - ויזואליזציה
- **Python** - שפת תכנות

---

## תיעוד נוסף

- [Spark Declarative Pipeline Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Unity Catalog Guide](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

---

## מחבר
Daniel Kaplun (kaplun.dani@gmail.com)

---

## Screenshots

### Pipeline Lineage
![Pipeline DAG](screenshots/pipeline_dag.png)

### Dashboard
![Dashboard](screenshots/dashboard.png)

### טבלת תוצאות
![Results Table](screenshots/results_table.png)

---

**📝 הערה:** קוד זה פותח כחלק ממשימת Databricks OneAI Exercise
