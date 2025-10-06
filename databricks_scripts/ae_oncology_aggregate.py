from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.databricks_adam_loader import DatabricksAdamLoader
from utils.statistical_functions import (
    calculate_fisher_exact, calculate_risk_difference,
    calculate_relative_risk, calculate_odds_ratio,
    apply_continuity_correction
)


def setup_widgets(dbutils):
    dbutils.widgets.text("input_catalog", "main", "Input Catalog")
    dbutils.widgets.text("input_schema", "results", "Input Schema")
    dbutils.widgets.text("input_table", "ae_base", "Input Table Name")
    dbutils.widgets.text("output_catalog", "main", "Output Catalog")
    dbutils.widgets.text("output_schema", "results", "Output Schema")
    dbutils.widgets.text("output_table", "ae_oncology_analysis", "Output Table Name")
    dbutils.widgets.dropdown("continuity_correction", "0.5", ["0", "0.5", "1", "arm"], "Continuity Correction")
    dbutils.widgets.text("exp_arm", "1", "Experimental Arm Number")
    dbutils.widgets.text("ctl_arm", "2", "Control Arm Number")


def get_max_toxicity_grade(df: DataFrame) -> DataFrame:
    if "AETOXGR" not in df.columns:
        raise ValueError("AETOXGR column not found in dataset")
    
    df = df.withColumn(
        "TOXGR_NUM",
        F.when(F.col("AETOXGR").cast("int").isNotNull(), F.col("AETOXGR").cast("int"))
        .otherwise(0)
    )
    
    window_spec = Window.partitionBy("USUBJID", "AEBODSYS", "AEDECOD").orderBy(F.desc("TOXGR_NUM"))
    
    df_max = df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")
    
    return df_max


def aggregate_by_toxicity_grade(df: DataFrame) -> DataFrame:
    arm_totals = df.groupBy("ARM_NUM", "ARM").agg(
        F.countDistinct("USUBJID").alias("ARM_TOTAL")
    )
    
    toxgr_counts = df.groupBy("AEBODSYS", "AEDECOD", "ARM_NUM", "ARM", "AETOXGR").agg(
        F.countDistinct("USUBJID").alias("N_SUBJECTS")
    )
    
    toxgr_with_totals = toxgr_counts.join(arm_totals, on=["ARM_NUM", "ARM"], how="left")
    
    toxgr_with_totals = toxgr_with_totals.withColumn(
        "PCT",
        F.round((F.col("N_SUBJECTS") / F.col("ARM_TOTAL")) * 100, 2)
    )
    
    return toxgr_with_totals


def aggregate_grade_combinations(df: DataFrame) -> DataFrame:
    df_grade34 = df.filter(F.col("TOXGR_NUM").isin([3, 4]))
    agg_34 = aggregate_by_toxicity_grade(df_grade34)
    agg_34 = agg_34.withColumn("GRADE_GROUP", F.lit("Grade 3/4"))
    
    df_grade345 = df.filter(F.col("TOXGR_NUM").isin([3, 4, 5]))
    agg_345 = aggregate_by_toxicity_grade(df_grade345)
    agg_345 = agg_345.withColumn("GRADE_GROUP", F.lit("Grade 3/4/5"))
    
    return agg_34.union(agg_345)


def calculate_statistics_for_comparison(df: DataFrame, exp_arm: int, ctl_arm: int, 
                                          cc_method: str = "0.5") -> DataFrame:
    exp_data = df.filter(F.col("ARM_NUM") == exp_arm).select(
        "AEBODSYS", "AEDECOD", "AETOXGR",
        F.col("N_SUBJECTS").alias("EXP_N"),
        F.col("ARM_TOTAL").alias("EXP_TOTAL")
    )
    
    ctl_data = df.filter(F.col("ARM_NUM") == ctl_arm).select(
        "AEBODSYS", "AEDECOD", "AETOXGR",
        F.col("N_SUBJECTS").alias("CTL_N"),
        F.col("ARM_TOTAL").alias("CTL_TOTAL")
    )
    
    comparison = exp_data.join(
        ctl_data,
        on=["AEBODSYS", "AEDECOD", "AETOXGR"],
        how="outer"
    ).fillna(0, subset=["EXP_N", "CTL_N", "EXP_TOTAL", "CTL_TOTAL"])
    
    def calc_stats(a, b, c, d):
        a, b, c, d = int(a), int(b), int(c), int(d)
        
        cc_value = 0.5 if cc_method == "0.5" else (1 if cc_method == "1" else 0)
        if cc_method == "arm":
            cc_value = 1 / (a + b + c + d) if (a + b + c + d) > 0 else 0
        
        rd, rd_lower, rd_upper = calculate_risk_difference(a, b, c, d, cc_value)
        rr, rr_lower, rr_upper = calculate_relative_risk(a, b, c, d, cc_value)
        or_val, or_lower, or_upper = calculate_odds_ratio(a, b, c, d, cc_value)
        
        try:
            _, pvalue = calculate_fisher_exact(a, b, c, d)
        except:
            pvalue = None
        
        return (rd, rd_lower, rd_upper, rr, rr_lower, rr_upper, 
                or_val, or_lower, or_upper, pvalue)
    
    stats_udf = F.udf(calc_stats, "struct<rd:double,rd_lower:double,rd_upper:double," +
                      "rr:double,rr_lower:double,rr_upper:double," +
                      "or:double,or_lower:double,or_upper:double,pvalue:double>")
    
    comparison = comparison.withColumn(
        "STATS",
        stats_udf(
            F.col("EXP_N"),
            F.col("EXP_TOTAL") - F.col("EXP_N"),
            F.col("CTL_N"),
            F.col("CTL_TOTAL") - F.col("CTL_N")
        )
    )
    
    comparison = comparison.select(
        "*",
        F.col("STATS.rd").alias("RISK_DIFF"),
        F.col("STATS.rd_lower").alias("RD_CI_LOWER"),
        F.col("STATS.rd_upper").alias("RD_CI_UPPER"),
        F.col("STATS.rr").alias("RELATIVE_RISK"),
        F.col("STATS.rr_lower").alias("RR_CI_LOWER"),
        F.col("STATS.rr_upper").alias("RR_CI_UPPER"),
        F.col("STATS.or").alias("ODDS_RATIO"),
        F.col("STATS.or_lower").alias("OR_CI_LOWER"),
        F.col("STATS.or_upper").alias("OR_CI_UPPER"),
        F.col("STATS.pvalue").alias("FISHER_PVALUE")
    ).drop("STATS")
    
    return comparison


def main(dbutils=None):
    spark = SparkSession.builder.appName("AE_Oncology_Aggregate").getOrCreate()
    
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    
    setup_widgets(dbutils)
    
    input_catalog = dbutils.widgets.get("input_catalog")
    input_schema = dbutils.widgets.get("input_schema")
    input_table = dbutils.widgets.get("input_table")
    output_catalog = dbutils.widgets.get("output_catalog")
    output_schema = dbutils.widgets.get("output_schema")
    output_table = dbutils.widgets.get("output_table")
    cc_method = dbutils.widgets.get("continuity_correction")
    exp_arm = int(dbutils.widgets.get("exp_arm"))
    ctl_arm = int(dbutils.widgets.get("ctl_arm"))
    
    input_table_path = f"{input_catalog}.{input_schema}.{input_table}"
    ae_base = spark.read.table(input_table_path)
    
    if "AETOXGR" not in ae_base.columns:
        raise ValueError("AETOXGR column required for oncology analysis")
    
    ae_max_tox = get_max_toxicity_grade(ae_base)
    
    ae_agg = aggregate_by_toxicity_grade(ae_max_tox)
    
    ae_grade_combos = aggregate_grade_combinations(ae_max_tox)
    
    ae_all = ae_agg.union(ae_grade_combos.select(ae_agg.columns))
    
    ae_stats = calculate_statistics_for_comparison(ae_agg, exp_arm, ctl_arm, cc_method)
    
    output_table_path = f"{output_catalog}.{output_schema}.{output_table}"
    ae_all.write.format("delta").mode("overwrite").saveAsTable(output_table_path)
    
    output_stats_path = f"{output_catalog}.{output_schema}.{output_table}_stats"
    ae_stats.write.format("delta").mode("overwrite").saveAsTable(output_stats_path)
    
    print(f"Successfully created oncology AE analysis: {output_table_path}")
    print(f"Statistical comparisons: {output_stats_path}")
    print(f"\nRecords by toxicity grade:")
    ae_all.groupBy("AETOXGR").count().orderBy("AETOXGR").show()


if __name__ == "__main__":
    main()
