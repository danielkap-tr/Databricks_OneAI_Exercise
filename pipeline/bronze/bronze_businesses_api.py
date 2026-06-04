from pyspark import pipelines as dp
import requests
import pandas as pd

@dp.table(
    name="road_comp_bronze_businesses",
    comment="Business data from Tel Aviv Municipality API"
)
def road_comp_bronze_businesses():
    # API endpoint for Tel Aviv business licenses
    API_URL = "https://gisn.tel-aviv.gov.il/arcgis/rest/services/IView2/MapServer/925/query"
    API_PARAMS = {"where": "1=1", "outFields": "*", "f": "json"}
    JSON_PATH = "/Workspace/Users/kaplun.dani@gmail.com/csv_and_json_data_ingestion_17896f42/json_data/tel_aviv_road_closures.json"
    
    try:
        response = requests.get(API_URL, params=API_PARAMS, timeout=30)
        response.raise_for_status()
        api_data = response.json()
        features = api_data.get('features', [])
        
        if features:
            business_records = [feature['attributes'] for feature in features]
            pandas_df = pd.DataFrame(business_records)
            return spark.createDataFrame(pandas_df)
        else:
            raise Exception("No features in API response")
    except Exception:
        # Fallback to local JSON if API unavailable
        return spark.read.format("json").option("inferSchema", "true").load(JSON_PATH)
