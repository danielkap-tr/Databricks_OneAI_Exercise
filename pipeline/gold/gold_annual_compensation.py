from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="road_comp_gold_annual_compensation",
    comment="Annual compensation per business for 2023"
)
def road_comp_gold_annual_compensation():
    # Business requirement: calculate annual compensation totals per business
    # Formula: area (sqm) × 100 ILS per day, capped at 10,000 ILS per day
    
    closures = spark.read.table("road_comp_silver_closures")
    businesses = spark.read.table("road_comp_silver_businesses")
    
    # Match businesses to both origin and destination streets
    affected_from = (
        closures.alias("c")
        .join(businesses.alias("b"),
            F.col("b.street_normalized") == F.col("c.street_from_normalized"), "inner")
        .select(
            F.col("c.ID").alias("closure_id"),
            F.col("c.closure_start"),
            F.col("c.closure_end"),
            F.col("c.closure_duration_days"),
            F.col("b.id_esek").alias("business_id"),
            F.col("b.shimush").alias("business_type"),
            F.col("b.shem_rechov").alias("business_street"),
            F.col("b.ms_bayit").alias("house_number"),
            F.col("b.shetach").alias("area")
        )
    )
    
    affected_to = (
        closures.alias("c")
        .join(businesses.alias("b"),
            F.col("b.street_normalized") == F.col("c.street_to_normalized"), "inner")
        .select(
            F.col("c.ID").alias("closure_id"),
            F.col("c.closure_start"),
            F.col("c.closure_end"),
            F.col("c.closure_duration_days"),
            F.col("b.id_esek").alias("business_id"),
            F.col("b.shimush").alias("business_type"),
            F.col("b.shem_rechov").alias("business_street"),
            F.col("b.ms_bayit").alias("house_number"),
            F.col("b.shetach").alias("area")
        )
    )
    
    all_affected = affected_from.union(affected_to)
    
    # Expand multi-day closures into daily records
    daily_affected = (
        all_affected
        .withColumn("closure_days", 
            F.when(F.col("closure_duration_days") == 0, 1)
            .otherwise(F.col("closure_duration_days") + 1))
        .withColumn("day_sequence", F.expr("sequence(0, closure_days - 1)"))
        .withColumn("day_offset", F.explode("day_sequence"))
        .withColumn("affected_date", F.date_add(F.col("closure_start"), F.col("day_offset")))
    )
    
    # Calculate daily compensation and handle multiple closures on same day
    gold_daily = (
        daily_affected
        .withColumn("daily_compensation", F.least(F.col("area") * 100, F.lit(10000)))
        .groupBy("business_id", "affected_date", "business_type", "business_street", "house_number", "area")
        .agg(
            F.max("daily_compensation").alias("daily_compensation"),
            F.count("*").alias("num_closures")
        )
    )
    
    # Aggregate to annual totals
    return (
        gold_daily
        .groupBy("business_id", "business_type", "business_street", "house_number", "area")
        .agg(
            F.sum("daily_compensation").alias("annual_compensation_ils"),
            F.count("*").alias("total_affected_days"),
            F.min("affected_date").alias("first_affected_date"),
            F.max("affected_date").alias("last_affected_date")
        )
        .orderBy(F.desc("annual_compensation_ils"))
    )
