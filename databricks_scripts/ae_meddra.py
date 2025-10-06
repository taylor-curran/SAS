from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.databricks_adam_loader import DatabricksAdamLoader
from utils.statistical_functions import calculate_fisher_exact


def setup_widgets(dbutils):
    dbutils.widgets.text("input_catalog", "main", "Input Catalog")
    dbutils.widgets.text("input_schema", "results", "Input Schema")
    dbutils.widgets.text("input_table", "ae_base", "Input Table Name")
    dbutils.widgets.text("meddra_catalog", "main", "MedDRA Catalog")
    dbutils.widgets.text("meddra_schema", "sdtm", "MedDRA Schema")
    dbutils.widgets.text("meddra_table", "mdhier_14_0", "MedDRA Table Name")
    dbutils.widgets.text("output_catalog", "main", "Output Catalog")
    dbutils.widgets.text("output_schema", "results", "Output Schema")
    dbutils.widgets.text("output_prefix", "ae_meddra_", "Output Table Prefix")
    dbutils.widgets.text("dme_table", "", "DME Table Name (optional)")
    dbutils.widgets.text("exp_arm", "1", "Experimental Arm Number")
    dbutils.widgets.text("ctl_arm", "2", "Control Arm Number")


def load_meddra_hierarchy(spark: SparkSession, catalog: str, schema: str, 
                           table: str) -> DataFrame:
    table_path = f"{catalog}.{schema}.{table}"
    meddra = spark.read.table(table_path)
    
    required_cols = ["SOC_NAME", "HLGT_NAME", "HLT_NAME", "PT_NAME"]
    missing_cols = [col for col in required_cols if col not in [c.upper() for c in meddra.columns]]
    
    if missing_cols:
        raise ValueError(f"Missing required MedDRA columns: {missing_cols}")
    
    return meddra


def match_ae_to_meddra(ae: DataFrame, meddra: DataFrame) -> DataFrame:
    meddra_primary = meddra.filter(F.col("PRIMARY_SOC_FG") == "Y")
    
    ae_upper = ae.withColumn("AEDECOD_UPPER", F.upper(F.col("AEDECOD")))
    meddra_upper = meddra_primary.withColumn("PT_NAME_UPPER", F.upper(F.col("PT_NAME")))
    
    ae_with_meddra = ae_upper.join(
        meddra_upper,
        ae_upper.AEDECOD_UPPER == meddra_upper.PT_NAME_UPPER,
        how="left"
    )
    
    ae_with_meddra = ae_with_meddra.select(
        ae.columns +
        ["SOC_NAME", "HLGT_NAME", "HLT_NAME", "PT_NAME", "LLT_NAME"]
    )
    
    return ae_with_meddra


def aggregate_by_meddra_level(df: DataFrame, level: str) -> DataFrame:
    if level not in ["SOC", "HLGT", "HLT", "PT"]:
        raise ValueError(f"Invalid MedDRA level: {level}")
    
    level_col = f"{level}_NAME"
    
    if level_col not in df.columns:
        raise ValueError(f"Column {level_col} not found in dataset")
    
    arm_totals = df.groupBy("ARM_NUM", "ARM").agg(
        F.countDistinct("USUBJID").alias("ARM_TOTAL")
    )
    
    level_counts = df.groupBy(level_col, "ARM_NUM", "ARM").agg(
        F.countDistinct("USUBJID").alias("N_SUBJECTS"),
        F.count("*").alias("N_EVENTS")
    )
    
    level_with_totals = level_counts.join(arm_totals, on=["ARM_NUM", "ARM"], how="left")
    
    level_with_totals = level_with_totals.withColumn(
        "PCT",
        F.round((F.col("N_SUBJECTS") / F.col("ARM_TOTAL")) * 100, 2)
    )
    
    level_with_totals = level_with_totals.withColumn("MEDDRA_LEVEL", F.lit(level))
    
    return level_with_totals


def calculate_meddra_statistics(df: DataFrame, level: str, exp_arm: int, 
                                  ctl_arm: int) -> DataFrame:
    level_col = f"{level}_NAME"
    
    exp_data = df.filter(F.col("ARM_NUM") == exp_arm).select(
        level_col,
        F.col("N_SUBJECTS").alias("EXP_N"),
        F.col("ARM_TOTAL").alias("EXP_TOTAL")
    )
    
    ctl_data = df.filter(F.col("ARM_NUM") == ctl_arm).select(
        level_col,
        F.col("N_SUBJECTS").alias("CTL_N"),
        F.col("ARM_TOTAL").alias("CTL_TOTAL")
    )
    
    comparison = exp_data.join(
        ctl_data,
        on=level_col,
        how="outer"
    ).fillna(0, subset=["EXP_N", "CTL_N", "EXP_TOTAL", "CTL_TOTAL"])
    
    def calc_fisher_pvalue(a, b, c, d):
        try:
            a, b, c, d = int(a), int(b), int(c), int(d)
            _, pvalue = calculate_fisher_exact(a, b, c, d)
            return pvalue
        except:
            return None
    
    fisher_udf = F.udf(calc_fisher_pvalue, "double")
    
    comparison = comparison.withColumn(
        "FISHER_PVALUE",
        fisher_udf(
            F.col("EXP_N"),
            F.col("EXP_TOTAL") - F.col("EXP_N"),
            F.col("CTL_N"),
            F.col("CTL_TOTAL") - F.col("CTL_N")
        )
    )
    
    comparison = comparison.withColumn(
        "EXP_PCT",
        F.round((F.col("EXP_N") / F.col("EXP_TOTAL")) * 100, 2)
    )
    
    comparison = comparison.withColumn(
        "CTL_PCT",
        F.round((F.col("CTL_N") / F.col("CTL_TOTAL")) * 100, 2)
    )
    
    comparison = comparison.withColumn(
        "RISK_DIFF",
        F.round(F.col("EXP_PCT") - F.col("CTL_PCT"), 2)
    )
    
    comparison = comparison.withColumn("MEDDRA_LEVEL", F.lit(level))
    
    return comparison


def add_dme_flags(df: DataFrame, dme_table: str, spark: SparkSession) -> DataFrame:
    if not dme_table:
        return df.withColumn("DME_FLAG", F.lit(None))
    
    try:
        dme = spark.read.table(dme_table)
        
        df_with_dme = df.join(
            dme.select("PT_NAME", F.lit("Y").alias("DME_FLAG")),
            on="PT_NAME",
            how="left"
        )
        
        df_with_dme = df_with_dme.fillna({"DME_FLAG": "N"})
        
        return df_with_dme
    except:
        return df.withColumn("DME_FLAG", F.lit(None))


def main(dbutils=None):
    spark = SparkSession.builder.appName("AE_MedDRA").getOrCreate()
    
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    
    setup_widgets(dbutils)
    
    input_catalog = dbutils.widgets.get("input_catalog")
    input_schema = dbutils.widgets.get("input_schema")
    input_table = dbutils.widgets.get("input_table")
    meddra_catalog = dbutils.widgets.get("meddra_catalog")
    meddra_schema = dbutils.widgets.get("meddra_schema")
    meddra_table = dbutils.widgets.get("meddra_table")
    output_catalog = dbutils.widgets.get("output_catalog")
    output_schema = dbutils.widgets.get("output_schema")
    output_prefix = dbutils.widgets.get("output_prefix")
    dme_table = dbutils.widgets.get("dme_table")
    exp_arm = int(dbutils.widgets.get("exp_arm"))
    ctl_arm = int(dbutils.widgets.get("ctl_arm"))
    
    input_table_path = f"{input_catalog}.{input_schema}.{input_table}"
    ae_base = spark.read.table(input_table_path)
    
    meddra = load_meddra_hierarchy(spark, meddra_catalog, meddra_schema, meddra_table)
    
    ae_with_meddra = match_ae_to_meddra(ae_base, meddra)
    
    if dme_table:
        ae_with_meddra = add_dme_flags(ae_with_meddra, dme_table, spark)
    
    meddra_levels = ["SOC", "HLGT", "HLT", "PT"]
    
    for level in meddra_levels:
        level_agg = aggregate_by_meddra_level(ae_with_meddra, level)
        
        output_table = f"{output_catalog}.{output_schema}.{output_prefix}{level.lower()}_counts"
        level_agg.write.format("delta").mode("overwrite").saveAsTable(output_table)
        print(f"Created {level} level aggregation: {output_table}")
        
        level_stats = calculate_meddra_statistics(level_agg, level, exp_arm, ctl_arm)
        
        output_stats = f"{output_catalog}.{output_schema}.{output_prefix}{level.lower()}_stats"
        level_stats.write.format("delta").mode("overwrite").saveAsTable(output_stats)
        print(f"Created {level} level statistics: {output_stats}")
    
    output_full = f"{output_catalog}.{output_schema}.{output_prefix}full_hierarchy"
    ae_with_meddra.write.format("delta").mode("overwrite").saveAsTable(output_full)
    print(f"\nCreated full MedDRA hierarchy dataset: {output_full}")
    
    print("\nSummary:")
    print(f"Total AEs matched to MedDRA: {ae_with_meddra.filter(F.col('PT_NAME').isNotNull()).count()}")
    print(f"Total AEs not matched: {ae_with_meddra.filter(F.col('PT_NAME').isNull()).count()}")
    
    for level in meddra_levels:
        level_col = f"{level}_NAME"
        count = ae_with_meddra.select(level_col).distinct().count()
        print(f"Unique {level} terms: {count}")


if __name__ == "__main__":
    main()
