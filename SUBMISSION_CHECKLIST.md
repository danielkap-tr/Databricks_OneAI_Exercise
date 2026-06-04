# 📋 Databricks Exercise Submission Checklist

## מה לשלוח לבודק:

### 1️⃣ קישור ל-GitHub Repository ⭐
**URL:** https://github.com/danielkap-tr/Databricks_OneAI_Exercise

הבודק יכול לראות:
- ✅ כל קוד הPipeline (6 קבצים)
- ✅ README.md מפורט עם הסברים
- ✅ pipeline_config.json - הגדרות הפייפליין
- ✅ מבנה תיקיות מלא

---

### 2️⃣ Screenshots (לצרף לrepository)

צריך להוסיף תיקייה `screenshots/` עם:

**Pipeline:**
- `pipeline_dag.png` - תרשים lineage של הטבלאות
- `pipeline_success.png` - ריצה מוצלחת
- `pipeline_tables.png` - רשימת טבלאות שנוצרו

**Dashboard:**
- `dashboard_overview.png` - מבט כללי על הדשבורד
- `dashboard_business_breakdown.png` - פירוט לפי סוג עסק
- `dashboard_street_totals.png` - סיכום לפי רחוב

**Job:**
- `job_schedule.png` - הגדרות הJob והלוח זמנים

---

### 3️⃣ תיעוד נוסף (אופציונלי אבל מומלץ)

**קובץ SUBMISSION.md:**

```markdown
# משימה: Tel Aviv Road Closures Compensation

## מימוש
- ✅ Medallion Architecture (Bronze → Silver → Gold)
- ✅ 6 טבלאות Delta בUnity Catalog
- ✅ Spark Declarative Pipeline serverless
- ✅ Dashboard אינטראקטיבי עם 9 widgets
- ✅ Job אוטומטי יומי ב-8:00

## תוצאות
- 401 עסקים
- 44 רחובות
- ₪88,600,000 סך פיצויים

## קבצים
- `pipeline/bronze/` - 2 קבצי ingestion
- `pipeline/silver/` - 2 קבצי ניקוי
- `pipeline/gold/` - 2 קבצי aggregation
- `README.md` - תיעוד מלא
- `pipeline_config.json` - הגדרות Pipeline

## איך להריץ
1. Clone repository
2. העלה לDatabricks workspace
3. צור Pipeline עם ההגדרות מpipeline_config.json
4. הרץ Pipeline
5. צור Dashboard מהטבלאות בcatalog workspace.default
```

---

### 4️⃣ נתוני דוגמה (אם מבוקש)

אם הבודק רוצה לראות נתונים אמיתיים:
- Export של 10-20 שורות ראשונות מכל טבלה כCSV
- להוסיף לtיקייה `sample_data/`

---

## איך לשלוח:

### אופציה A: GitHub בלבד (הכי פשוט)
1. העלה הכל ל-GitHub
2. שלח לבודק: "הנה הlink: https://github.com/danielkap-tr/Databricks_OneAI_Exercise"
3. זהו!

### אופציה B: GitHub + ZIP
1. העלה ל-GitHub
2. יצא גם כZIP (Download → Download ZIP מGitHub)
3. שלח את שני הדברים

### אופציה C: Video Demo (ב��נוסף)
אם יש זמן - סרטון קצר (2-3 דקות) שמראה:
- Pipeline רץ
- Dashboard עובד
- Job מתוזמן

---

## 🎯 המינימום הנדרש:

1. ✅ GitHub repository עם כל הקוד
2. ✅ README.md מפורט
3. ✅ לפחות 3-4 screenshots

**זה מספיק כדי שהבודק יבין בדיוק מה עשית!** 🚀
