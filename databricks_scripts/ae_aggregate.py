from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.databricks_adam_loader import DatabricksAdamLoader


def setup_widgets(dbutils):
    dbutils.widgets.text("input_catalog", "main", "Input Catalog")
    dbutils.widgets.text("input_schema", "results", "Input Schema")
    dbutils.widgets.text("input_table", "ae_base", "Input Table Name")
    dbutils.widgets.text("output_catalog", "main", "Output Catalog")
    dbutils.widgets.text("output_schema", "results", "Output Schema")
    dbutils.widgets.text("output_prefix", "ae_analysis_", "Output Table Prefix")
    dbutils.widgets.text("filter_pct_threshold", "2", "Minimum % for Analysis A")


def aggregate_ae_by_term(df: DataFrame, severity_filter: str = None, 
                          serious_filter: bool = False) -> DataFrame:
    if serious_filter and "AESER" in df.columns:
        df = df.filter(F.col("AESER") == "Y")
    
    if severity_filter and "AESEV" in df.columns:
        df = df.filter(F.col("AESEV") == severity_filter)
    
    arm_totals = df.groupBy("ARM_NUM", "ARM").agg(
        F.countDistinct("USUBJID").alias("ARM_TOTAL")
    )
    
    ae_counts = df.groupBy("AEBODSYS", "AEDECOD", "ARM_NUM", "ARM").agg(
        F.countDistinct("USUBJID").alias("N_SUBJECTS"),
        F.count("*").alias("N_EVENTS")
    )
    
    ae_with_totals = ae_counts.join(arm_totals, on=["ARM_NUM", "ARM"], how="left")
    
    ae_with_totals = ae_with_totals.withColumn(
        "PCT",
        F.round((F.col("N_SUBJECTS") / F.col("ARM_TOTAL")) * 100, 2)
    )
    
    return ae_with_totals


def pivot_by_arm(df: DataFrame) -> DataFrame:
    arms = df.select("ARM_NUM").distinct().orderBy("ARM_NUM").collect()
    arm_nums = [row["ARM_NUM"] for row in arms]
    
    pivoted = df.groupBy("AEBODSYS", "AEDECOD").pivot("ARM_NUM", arm_nums).agg(
        F.first("N_SUBJECTS"),
        F.first("PCT"),
        F.first("ARM_TOTAL")
    )
    
    return pivoted


def aggregate_by_severity(df: DataFrame) -> DataFrame:
    if "AESEV" not in df.columns:
        return None
    
    severity_levels = ["MILD", "MODERATE", "SEVERE", "LIFE THREATENING", "FATAL"]
    
    ae_severity = df.groupBy("AEBODSYS", "AEDECOD", "ARM_NUM", "ARM", "AESEV").agg(
        F.countDistinct("USUBJID").alias("N_SUBJECTS")
    )
    
    arm_totals = df.groupBy("ARM_NUM", "ARM").agg(
        F.countDistinct("USUBJID").alias("ARM_TOTAL")
    )
    
    ae_severity = ae_severity.join(arm_totals, on=["ARM_NUM", "ARM"], how="left")
    
    ae_severity = ae_severity.withColumn(
        "PCT",
        F.round((F.col("N_SUBJECTS") / F.col("ARM_TOTAL")) * 100, 2)
    )
    
    return ae_severity


def filter_by_threshold(df: DataFrame, threshold: float) -> DataFrame:
    max_pct = df.groupBy("AEBODSYS", "AEDECOD").agg(
        F.max("PCT").alias("MAX_PCT")
    )
    
    df_filtered = df.join(max_pct, on=["AEBODSYS", "AEDECOD"], how="left")
    df_filtered = df_filtered.filter(F.col("MAX_PCT") >= threshold)
    df_filtered = df_filtered.drop("MAX_PCT")
    
    return df_filtered


def main(dbutils=None):
    spark = SparkSession.builder.appName("AE_Aggregate").getOrCreate()
    
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    
    setup_widgets(dbutils)
    
    input_catalog = dbutils.widgets.get("input_catalog")
    input_schema = dbutils.widgets.get("input_schema")
    input_table = dbutils.widgets.get("input_table")
    output_catalog = dbutils.widgets.get("output_catalog")
    output_schema = dbutils.widgets.get("output_schema")
    output_prefix = dbutils.widgets.get("output_prefix")
    filter_pct = float(dbutils.widgets.get("filter_pct_threshold"))
    
    input_table_path = f"{input_catalog}.{input_schema}.{input_table}"
    ae_base = spark.read.table(input_table_path)
    
    analysis_a = aggregate_ae_by_term(ae_base)
    analysis_a = filter_by_threshold(analysis_a, filter_pct)
    output_a = f"{output_catalog}.{output_schema}.{output_prefix}all_aes"
    analysis_a.write.format("delta").mode("overwrite").saveAsTable(output_a)
    print(f"Analysis A (All AEs > {filter_pct}%): {output_a}")
    
    if "AESER" in ae_base.columns:
        analysis_b = aggregate_ae_by_term(ae_base, serious_filter=True)
        output_b = f"{output_catalog}.{output_schema}.{output_prefix}serious_aes"
        analysis_b.write.format("delta").mode("overwrite").saveAsTable(output_b)
        print(f"Analysis B (Serious AEs): {output_b}")
    
    if "AESEV" in ae_base.columns:
        analysis_c = aggregate_by_severity(ae_base)
        if analysis_c is not None:
            output_c = f"{output_catalog}.{output_schema}.{output_prefix}aes_by_severity"
            analysis_c.write.format("delta").mode("overwrite").saveAsTable(output_c)
            print(f"Analysis C (AEs by Severity): {output_c}")
        
        if "AESER" in ae_base.columns:
            ae_serious = ae_base.filter(F.col("AESER") == "Y")
            analysis_d = aggregate_by_severity(ae_serious)
            if analysis_d is not None:
                output_d = f"{output_catalog}.{output_schema}.{output_prefix}serious_aes_by_severity"
                analysis_d.write.format("delta").mode("overwrite").saveAsTable(output_d)
                print(f"Analysis D (Serious AEs by Severity): {output_d}")
    
    print("\nSummary Statistics:")
    print(f"Total AE records: {ae_base.count()}")
    print(f"Unique subjects with AEs: {ae_base.select('USUBJID').distinct().count()}")
    print(f"Unique AE terms: {ae_base.select('AEDECOD').distinct().count()}")
    print(f"Body systems: {ae_base.select('AEBODSYS').distinct().count()}")


if __name__ == "__main__":
    main()
