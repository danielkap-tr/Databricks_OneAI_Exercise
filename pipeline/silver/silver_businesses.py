from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.table(
    name="road_comp_silver_businesses",
    comment="Cleaned and deduplicated business data"
)
def road_comp_silver_businesses():
    # Keep latest record per business and normalize street names
    window_spec = Window.partitionBy("id_esek").orderBy(F.col("date_import").desc())
    
    return (
        spark.read.table("road_comp_bronze_businesses")
        .filter(F.col("id_esek").isNotNull())
        .withColumn("street_normalized", F.lower(F.trim(F.col("shem_rechov"))))
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
