from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="road_comp_silver_closures",
    comment="Cleaned road closure data for 2023"
)
def road_comp_silver_closures():
    # Parse dates, normalize street names, and filter for 2023
    return (
        spark.read.table("road_comp_bronze_closures")
        .filter(F.col("ID").isNotNull())
        .withColumn("closure_start", 
            F.to_timestamp(F.to_date(F.split(F.col("tr_from"), " ")[0], "dd/MM/yyyy")))
        .withColumn("closure_end", 
            F.when(F.col("tr_to").isNotNull(), 
                F.to_timestamp(F.to_date(F.split(F.col("tr_to"), " ")[0], "dd/MM/yyyy")))
            .otherwise(F.col("closure_start")))
        .withColumn("closure_year", F.year(F.col("closure_start")))
        .withColumn("street_from_normalized", F.lower(F.trim(F.col("me_shem_rechov"))))
        .withColumn("street_to_normalized", F.lower(F.trim(F.col("ad_shem_rechov"))))
        .withColumn("closure_duration_days", 
            F.datediff(F.col("closure_end"), F.col("closure_start")))
        .filter(F.col("closure_year") == 2023)
    )
