from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType
from pyspark.sql.window import Window
from utils.databricks_adam_loader import DatabricksAdamLoader


def setup_widgets(dbutils):
    dbutils.widgets.text("catalog", "main", "Delta Lake Catalog")
    dbutils.widgets.text("schema", "sdtm", "Delta Lake Schema")
    dbutils.widgets.text("output_catalog", "main", "Output Catalog")
    dbutils.widgets.text("output_schema", "results", "Output Schema")
    dbutils.widgets.text("output_table", "ae_base", "Output Table Name")
    dbutils.widgets.text("study_lag", "30", "Study Lag (days after treatment end)")
    dbutils.widgets.dropdown("source_type", "delta", ["delta", "xpt"], "Data Source Type")
    dbutils.widgets.text("ae_path", "", "AE XPT Path (if source_type=xpt)")
    dbutils.widgets.text("dm_path", "", "DM XPT Path (if source_type=xpt)")
    dbutils.widgets.text("ex_path", "", "EX XPT Path (if source_type=xpt)")


def validate_required_columns(df: DataFrame, domain: str, required_cols: list) -> DataFrame:
    missing_cols = [col for col in required_cols if col.upper() not in [c.upper() for c in df.columns]]
    
    if missing_cols:
        raise ValueError(f"Missing required columns in {domain}: {missing_cols}")
    
    return df


def convert_to_date(df: DataFrame, date_col: str) -> DataFrame:
    df = df.withColumn(
        f"{date_col}_DATE",
        F.when(F.col(date_col).isNotNull(), 
               F.to_date(F.col(date_col), "yyyy-MM-dd"))
        .otherwise(None)
    )
    return df


def merge_domains(ae: DataFrame, dm: DataFrame, ex: DataFrame, study_lag: int) -> DataFrame:
    ae_cols = [c.upper() for c in ae.columns]
    dm_cols = [c.upper() for c in dm.columns]
    ex_cols = [c.upper() for c in ex.columns]
    
    ae = ae.select([F.col(c).alias(c.upper()) for c in ae.columns])
    dm = dm.select([F.col(c).alias(c.upper()) for c in dm.columns])
    ex = ex.select([F.col(c).alias(c.upper()) for c in ex.columns])
    
    ae = convert_to_date(ae, "AESTDTC")
    ae = convert_to_date(ae, "AEENDTC")
    dm = convert_to_date(dm, "RFSTDTC")
    dm = convert_to_date(dm, "RFENDTC")
    ex = convert_to_date(ex, "EXSTDTC")
    ex = convert_to_date(ex, "EXENDTC")
    
    ex_agg = ex.groupBy("USUBJID").agg(
        F.min("EXSTDTC_DATE").alias("EXTRT_START"),
        F.max("EXENDTC_DATE").alias("EXTRT_END")
    )
    
    dm_ex = dm.join(ex_agg, on="USUBJID", how="left")
    
    dm_ex = dm_ex.withColumn(
        "TRT_START",
        F.coalesce(F.col("EXTRT_START"), F.col("RFSTDTC_DATE"))
    )
    
    dm_ex = dm_ex.withColumn(
        "TRT_END",
        F.coalesce(F.col("EXTRT_END"), F.col("RFENDTC_DATE"))
    )
    
    dm_ex = dm_ex.withColumn(
        "TRT_END_LAG",
        F.expr(f"date_add(TRT_END, {study_lag})")
    )
    
    arm_col = "ACTARM" if "ACTARM" in dm_cols else "ARM"
    
    ae_merged = ae.join(
        dm_ex.select(
            "USUBJID", 
            F.col(arm_col).alias("ARM"),
            "TRT_START", 
            "TRT_END", 
            "TRT_END_LAG",
            *([col for col in dm_cols if col in ["SEX", "AGE", "RACE", "ETHNIC"]])
        ),
        on="USUBJID",
        how="inner"
    )
    
    ae_merged = ae_merged.filter(
        (F.col("AESTDTC_DATE") >= F.col("TRT_START")) &
        (F.col("AESTDTC_DATE") <= F.col("TRT_END_LAG"))
    )
    
    arm_window = Window.orderBy("ARM")
    arm_mapping = ae_merged.select("ARM").distinct().withColumn("ARM_NUM", F.row_number().over(arm_window))
    
    ae_merged = ae_merged.join(arm_mapping, on="ARM", how="left")
    
    select_cols = [
        "USUBJID", "ARM", "ARM_NUM", "AEBODSYS", "AEDECOD", 
        "AESTDTC_DATE", "AEENDTC_DATE", "TRT_START", "TRT_END"
    ]
    
    optional_cols = ["AESER", "AESEV", "AEREL", "AETOXGR", "SEX", "AGE", "RACE"]
    for col in optional_cols:
        if col in [c.upper() for c in ae_merged.columns]:
            select_cols.append(col)
    
    ae_merged = ae_merged.select(*select_cols)
    
    return ae_merged


def main(dbutils=None):
    spark = SparkSession.builder.appName("AE_Setup").getOrCreate()
    
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    
    setup_widgets(dbutils)
    
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    output_catalog = dbutils.widgets.get("output_catalog")
    output_schema = dbutils.widgets.get("output_schema")
    output_table = dbutils.widgets.get("output_table")
    study_lag = int(dbutils.widgets.get("study_lag"))
    source_type = dbutils.widgets.get("source_type")
    
    loader = DatabricksAdamLoader(spark, catalog, schema)
    
    if source_type == "delta":
        ae = loader.load_ae()
        dm = loader.load_dm()
        ex = loader.load_ex()
    else:
        ae_path = dbutils.widgets.get("ae_path")
        dm_path = dbutils.widgets.get("dm_path")
        ex_path = dbutils.widgets.get("ex_path")
        ae = loader.load_ae(source_path=ae_path, source_type="xpt")
        dm = loader.load_dm(source_path=dm_path, source_type="xpt")
        ex = loader.load_ex(source_path=ex_path, source_type="xpt")
    
    ae = validate_required_columns(ae, "AE", ["USUBJID", "AEBODSYS", "AEDECOD", "AESTDTC"])
    dm = validate_required_columns(dm, "DM", ["USUBJID"])
    ex = validate_required_columns(ex, "EX", ["USUBJID"])
    
    ae_merged = merge_domains(ae, dm, ex, study_lag)
    
    output_table_path = f"{output_catalog}.{output_schema}.{output_table}"
    ae_merged.write.format("delta").mode("overwrite").saveAsTable(output_table_path)
    
    print(f"Successfully created merged AE dataset: {output_table_path}")
    print(f"Total records: {ae_merged.count()}")
    print(f"Unique subjects: {ae_merged.select('USUBJID').distinct().count()}")
    print(f"Treatment arms: {ae_merged.select('ARM').distinct().count()}")
    
    ae_merged.groupBy("ARM", "ARM_NUM").count().orderBy("ARM_NUM").show()


if __name__ == "__main__":
    main()
