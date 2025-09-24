"""
PhUSE Utility Functions - Python Implementation

This module provides Python equivalents of SAS PhUSE utility macros used in the original
SAS clinical data analysis pipeline. These functions support the PySpark-based implementation
of the clinical data analysis workflow.
"""

from typing import Dict, List, Optional, Tuple, Union
import math
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import DoubleType


def labels_from_var(df: DataFrame, code_col: str, label_col: str) -> Dict[str, str]:
    """
    Extract unique parameter codes and their labels from a DataFrame.
    
    Equivalent to SAS macro: %util_labels_from_var
    
    Args:
        df: PySpark DataFrame containing the data
        code_col: Column name containing parameter codes
        label_col: Column name containing parameter labels
    
    Returns:
        Dictionary mapping parameter codes to their labels
    """
    param_df = df.select(code_col, label_col).distinct()
    
    param_dict = {row[code_col]: row[label_col] for row in param_df.collect()}
    
    return param_dict


def count_unique_values(df: DataFrame, column: str) -> int:
    """
    Count distinct values in a DataFrame column.
    
    Equivalent to SAS macro: %util_count_unique_values
    
    Args:
        df: PySpark DataFrame containing the data
        column: Column name to count distinct values
    
    Returns:
        Count of distinct values in the column
    """
    return df.select(column).distinct().count()


def get_reference_lines(
    df: DataFrame, 
    low_var: str, 
    high_var: str, 
    ref_lines_type: str = "UNIFORM"
) -> List[float]:
    """
    Calculate normal range reference lines for plotting.
    
    Equivalent to SAS macro: %util_get_reference_lines
    
    Args:
        df: PySpark DataFrame containing the data
        low_var: Column name for lower limit of normal range
        high_var: Column name for upper limit of normal range
        ref_lines_type: Type of reference lines to generate:
            - "NONE": No reference lines
            - "UNIFORM": Only plot reference lines if uniform for all observations
            - "NARROW": Display only the narrow normal limits (max LOW, min HIGH)
            - "ALL": Display all reference lines (discouraged)
            - numeric values: Space-delimited list of reference line values
    
    Returns:
        List of reference line values
    """
    ref_lines = []
    
    if ref_lines_type not in ["NONE", "UNIFORM", "NARROW", "ALL"]:
        try:
            ref_lines = [float(x) for x in ref_lines_type.split()]
            return ref_lines
        except ValueError:
            ref_lines_type = "UNIFORM"
    
    if ref_lines_type == "NONE":
        return []
    
    if ref_lines_type in ["UNIFORM", "NARROW", "ALL"]:
        low_values = [row[0] for row in df.select(low_var).distinct().filter(F.col(low_var).isNotNull()).collect()]
        high_values = [row[0] for row in df.select(high_var).distinct().filter(F.col(high_var).isNotNull()).collect()]
        
        if ref_lines_type == "UNIFORM":
            if len(low_values) == 1:
                ref_lines.append(low_values[0])
            if len(high_values) == 1:
                ref_lines.append(high_values[0])
        
        elif ref_lines_type == "NARROW":
            if low_values:
                ref_lines.append(max(low_values))
            if high_values:
                ref_lines.append(min(high_values))
        
        elif ref_lines_type == "ALL":
            ref_lines.extend(low_values)
            ref_lines.extend(high_values)
    
    return sorted(list(set(ref_lines)))


def get_var_min_max(
    df: DataFrame, 
    var: str, 
    extra_values: Optional[List[float]] = None
) -> Tuple[float, float]:
    """
    Determine axis ranges for plotting based on data and reference lines.
    
    Equivalent to SAS macro: %util_get_var_min_max
    
    Args:
        df: PySpark DataFrame containing the data
        var: Column name for the measurement variable
        extra_values: Additional values (like reference lines) to consider
    
    Returns:
        Tuple of (min_value, max_value) for axis range
    """
    stats = df.select(
        F.min(F.col(var)).alias("min_val"),
        F.max(F.col(var)).alias("max_val")
    ).collect()[0]
    
    min_val = stats["min_val"]
    max_val = stats["max_val"]
    
    if extra_values:
        all_values = [min_val, max_val] + extra_values
        min_val = min(v for v in all_values if v is not None)
        max_val = max(v for v in all_values if v is not None)
    
    range_val = max_val - min_val
    padding = range_val * 0.05
    
    return (min_val - padding, max_val + padding)


def value_format(df: DataFrame, var: str) -> Dict[str, str]:
    """
    Format statistical values for display with appropriate precision.
    
    Equivalent to SAS macro: %util_value_format
    
    Args:
        df: PySpark DataFrame containing the data
        var: Column name for the measurement variable
    
    Returns:
        Dictionary with format strings for different statistics
    """
    numeric_data = df.select(var).filter(F.col(var).isNotNull())
    
    if numeric_data.count() == 0:
        return {
            "mean": "%.1f",
            "std": "%.2f",
            "median": "%.1f",
            "q1": "%.1f",
            "q3": "%.1f"
        }
    
    sample_values = [row[0] for row in numeric_data.limit(100).collect()]
    
    has_decimals = any(isinstance(val, float) and val != int(val) for val in sample_values)
    
    if has_decimals:
        return {
            "mean": "%.1f",
            "std": "%.2f",
            "median": "%.1f",
            "q1": "%.1f",
            "q3": "%.1f"
        }
    else:
        return {
            "mean": "%.0f",
            "std": "%.1f",
            "median": "%.0f",
            "q1": "%.0f",
            "q3": "%.0f"
        }


def boxplot_block_ranges(
    df: DataFrame, 
    block_var: str, 
    cat_vars: List[str], 
    max_boxes_per_page: int = 20
) -> List[str]:
    """
    Paginate visits for multiple plot pages.
    
    Equivalent to SAS macro: %util_boxplot_block_ranges
    
    Args:
        df: PySpark DataFrame containing the data
        block_var: Column name for the block variable (e.g., visit)
        cat_vars: List of category variables (e.g., treatment)
        max_boxes_per_page: Maximum number of boxes per page
    
    Returns:
        List of filter expressions for each page
    """
    block_values = sorted([row[0] for row in df.select(block_var).distinct().collect()])
    
    cat_counts = {}
    for block_val in block_values:
        cat_count = df.filter(F.col(block_var) == block_val).select(*cat_vars).distinct().count()
        cat_counts[block_val] = cat_count
    
    boxes_per_block = {block_val: count for block_val, count in cat_counts.items()}
    
    pages = []
    current_page = []
    current_page_boxes = 0
    
    for block_val in block_values:
        block_boxes = boxes_per_block[block_val]
        
        if current_page_boxes + block_boxes > max_boxes_per_page and current_page:
            pages.append(current_page)
            current_page = [block_val]
            current_page_boxes = block_boxes
        else:
            current_page.append(block_val)
            current_page_boxes += block_boxes
    
    if current_page:
        pages.append(current_page)
    
    filter_expressions = []
    for page in pages:
        expr = f"{block_var} IN ({', '.join(str(val) for val in page)})"
        filter_expressions.append(expr)
    
    return filter_expressions


def axis_order(min_val: float, max_val: float) -> Tuple[float, float, float]:
    """
    Calculate axis tick intervals for plotting.
    
    Equivalent to SAS macro: %util_axis_order
    
    Args:
        min_val: Minimum value for the axis
        max_val: Maximum value for the axis
    
    Returns:
        Tuple of (min_val, max_val, tick_interval)
    """
    range_val = max_val - min_val
    
    if range_val <= 1:
        tick_interval = 0.1
    elif range_val <= 5:
        tick_interval = 0.5
    elif range_val <= 10:
        tick_interval = 1
    elif range_val <= 50:
        tick_interval = 5
    elif range_val <= 100:
        tick_interval = 10
    elif range_val <= 500:
        tick_interval = 50
    else:
        tick_interval = 100
    
    min_val_rounded = math.floor(min_val / tick_interval) * tick_interval
    max_val_rounded = math.ceil(max_val / tick_interval) * tick_interval
    
    return (min_val_rounded, max_val_rounded, tick_interval)


def detect_outliers(
    df: DataFrame, 
    measure_var: str, 
    low_var: str, 
    high_var: str
) -> DataFrame:
    """
    Detect outliers based on normal range limits.
    
    Args:
        df: PySpark DataFrame containing the data
        measure_var: Column name for the measurement variable
        low_var: Column name for lower limit of normal range
        high_var: Column name for upper limit of normal range
    
    Returns:
        DataFrame with outlier column added
    """
    return df.withColumn(
        f"{measure_var}_outlier",
        F.when(
            (F.col(measure_var).isNotNull() & F.col(low_var).isNotNull() & (F.col(measure_var) < F.col(low_var))) |
            (F.col(measure_var).isNotNull() & F.col(high_var).isNotNull() & (F.col(measure_var) > F.col(high_var))),
            F.col(measure_var)
        ).otherwise(None)
    )
