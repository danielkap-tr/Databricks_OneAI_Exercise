from pyspark import pipelines as dp

@dp.table(
    name="road_comp_bronze_closures",
    comment="Raw road closure data from CSV file"
)
def road_comp_bronze_closures():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("inferSchema", "true")
        .load("/Workspace/Users/kaplun.dani@gmail.com/csv_and_json_data_ingestion_17896f42/rechov_sagur.csv")
    )
