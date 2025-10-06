"""
Utility functions for adverse events processing in Databricks.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from typing import Tuple, Optional


def get_arm_variable(dm_df: DataFrame) -> str:
    """
    Determine which arm variable to use (ACTARM or ARM).
    
    Equivalent to SAS ae_setup.sas lines 61-79.
    
    Args:
        dm_df: Demographics DataFrame
        
    Returns:
        Name of arm variable to use ("ACTARM" or "ARM")
    """
    if "ACTARM" in dm_df.columns:
        return "ACTARM"
    elif "ARM" in dm_df.columns:
        return "ARM"
    else:
        raise ValueError("Neither ACTARM nor ARM found in DM dataset")


def create_safety_population(
    dm_df: DataFrame,
    ex_df: DataFrame,
    arm_var: str
) -> Tuple[DataFrame, DataFrame]:
    """
    Create safety population by merging DM and EX domains.
    
    Equivalent to SAS ae_setup.sas lines 120-300.
    
    Args:
        dm_df: Demographics DataFrame
        ex_df: Exposure DataFrame  
        arm_var: Arm variable name ("ACTARM" or "ARM")
        
    Returns:
        Tuple of (safety_population_df, arm_mapping_df)
    """
    dm_clean = dm_df.select(
        "USUBJID",
        F.col(arm_var).alias("ARM"),
        *([F.col("ARMCD")] if "ARMCD" in dm_df.columns else [])
    )
    
    if "ARMCD" in dm_clean.columns:
        dm_clean = dm_clean.filter(
            ~F.upper(F.col("ARMCD")).isin(["SCRNFAIL", "NOTASSGN"])
        )
    
    ex_subj = ex_df.select("USUBJID").distinct()
    
    safety_pop = dm_clean.join(ex_subj, on="USUBJID", how="inner")
    
    arm_counts = safety_pop.groupBy("ARM").agg(
        F.count("USUBJID").alias("COUNT")
    ).orderBy("ARM")
    
    window_spec = Window.orderBy("ARM")
    arm_mapping = arm_counts.withColumn("ARM_NUM", F.row_number().over(window_spec))
    
    safety_pop = safety_pop.join(
        arm_mapping.select("ARM", "ARM_NUM"),
        on="ARM",
        how="left"
    )
    
    return safety_pop, arm_mapping


def map_severity_to_numeric(aesev_col: str) -> F.Column:
    """
    Map AESEV text values to numeric codes.
    
    Equivalent to SAS ae_aggregate.sas severity mapping logic.
    
    Args:
        aesev_col: Column name containing severity values
        
    Returns:
        Column expression with numeric severity codes
    """
    return (
        F.when(F.upper(F.col(aesev_col)) == "MILD", 1)
        .when(F.upper(F.col(aesev_col)) == "MODERATE", 2)
        .when(F.upper(F.col(aesev_col)) == "SEVERE", 3)
        .when(F.upper(F.col(aesev_col)) == "LIFE THREATENING", 4)
        .when(F.upper(F.col(aesev_col)) == "FATAL", 5)
        .otherwise(100)
    )


def map_toxicity_to_numeric(aetoxgr_col: str) -> F.Column:
    """
    Map AETOXGR (toxicity grade) text values to numeric codes.
    
    Args:
        aetoxgr_col: Column name containing toxicity grade values
        
    Returns:
        Column expression with numeric toxicity codes
    """
    return (
        F.when(F.col(aetoxgr_col).cast("int").isNotNull(), F.col(aetoxgr_col).cast("int"))
        .when(F.upper(F.col(aetoxgr_col)) == "1", 1)
        .when(F.upper(F.col(aetoxgr_col)) == "2", 2)
        .when(F.upper(F.col(aetoxgr_col)) == "3", 3)
        .when(F.upper(F.col(aetoxgr_col)) == "4", 4)
        .when(F.upper(F.col(aetoxgr_col)) == "5", 5)
        .otherwise(0)
    )


def calculate_risk_difference(
    arm1_count: int,
    arm1_total: int,
    arm2_count: int,
    arm2_total: int
) -> float:
    """
    Calculate risk difference between two arms.
    
    Args:
        arm1_count: Event count in arm 1
        arm1_total: Total subjects in arm 1
        arm2_count: Event count in arm 2
        arm2_total: Total subjects in arm 2
        
    Returns:
        Risk difference as percentage
    """
    risk1 = 100.0 * arm1_count / arm1_total if arm1_total > 0 else 0.0
    risk2 = 100.0 * arm2_count / arm2_total if arm2_total > 0 else 0.0
    return risk1 - risk2


def calculate_relative_risk(
    arm1_count: int,
    arm1_total: int,
    arm2_count: int,
    arm2_total: int,
    continuity_correction: float = 0.5
) -> Optional[float]:
    """
    Calculate relative risk between two arms with continuity correction.
    
    Args:
        arm1_count: Event count in arm 1
        arm1_total: Total subjects in arm 1
        arm2_count: Event count in arm 2
        arm2_total: Total subjects in arm 2
        continuity_correction: Value to add when counts are zero
        
    Returns:
        Relative risk or None if calculation not possible
    """
    if arm1_count == 0 or arm2_count == 0:
        arm1_count += continuity_correction
        arm2_count += continuity_correction
    
    risk1 = arm1_count / arm1_total if arm1_total > 0 else 0.0
    risk2 = arm2_count / arm2_total if arm2_total > 0 else 0.0
    
    return risk1 / risk2 if risk2 > 0 else None
