"""
PhUSE Utility Functions
Replaces PhUSE macro functionality from the original SAS scripts
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, min as spark_min, max as spark_max
from typing import List, Dict, Tuple, Any
import math

logger = logging.getLogger(__name__)

def util_labels_from_var(df: DataFrame, code_var: str, label_var: str) -> Dict[str, Any]:
    """
    Extract unique values and labels from variables.
    Replaces %util_labels_from_var macro functionality
    
    Args:
        df: Input DataFrame
        code_var: Variable containing codes
        label_var: Variable containing labels
    
    Returns:
        Dictionary with counts, values, and labels
    """
    logger.info(f"Extracting labels from {code_var} and {label_var}")
    
    unique_pairs = df.select(code_var, label_var).distinct().collect()
    
    unique_pairs = sorted(unique_pairs, key=lambda x: x[0])
    
    result = {
        'count': len(unique_pairs),
        'values': [row[0] for row in unique_pairs],
        'labels': [row[1] for row in unique_pairs]
    }
    
    logger.info(f"Found {result['count']} unique {code_var} values")
    return result

def util_count_unique_values(df: DataFrame, var_name: str) -> int:
    """
    Count unique values in a variable.
    Replaces %util_count_unique_values macro functionality
    
    Args:
        df: Input DataFrame
        var_name: Variable name to count
    
    Returns:
        Count of unique values
    """
    count = df.select(var_name).distinct().count()
    logger.info(f"Unique values in {var_name}: {count}")
    return count

def util_get_reference_lines(df: DataFrame, low_var: str, high_var: str, ref_lines: str) -> List[float]:
    """
    Get reference line values based on configuration.
    Replaces %util_get_reference_lines macro functionality
    
    Args:
        df: Input DataFrame
        low_var: Lower reference limit variable
        high_var: Upper reference limit variable
        ref_lines: Reference lines configuration ('NONE', 'UNIFORM', 'NARROW', 'ALL')
    
    Returns:
        List of reference line values
    """
    logger.info(f"Getting reference lines with config: {ref_lines}")
    
    if ref_lines == 'NONE':
        return []
    
    low_values = df.select(low_var).distinct().rdd.flatMap(lambda x: x).collect()
    high_values = df.select(high_var).distinct().rdd.flatMap(lambda x: x).collect()
    
    low_values = [v for v in low_values if v is not None]
    high_values = [v for v in high_values if v is not None]
    
    ref_line_values = []
    
    if ref_lines == 'UNIFORM':
        if len(set(low_values)) == 1:
            ref_line_values.extend(low_values[:1])
        if len(set(high_values)) == 1:
            ref_line_values.extend(high_values[:1])
    
    elif ref_lines == 'NARROW':
        if low_values:
            ref_line_values.append(max(low_values))
        if high_values:
            ref_line_values.append(min(high_values))
    
    elif ref_lines == 'ALL':
        ref_line_values.extend(set(low_values + high_values))
    
    logger.info(f"Reference lines: {ref_line_values}")
    return sorted(ref_line_values)

def util_value_format(df: DataFrame, var_name: str) -> Tuple[str, str]:
    """
    Determine appropriate formatting for statistical values.
    Replaces %util_value_format macro functionality
    
    Args:
        df: Input DataFrame
        var_name: Variable to analyze for formatting
    
    Returns:
        Tuple of (mean_format, std_format)
    """
    logger.info(f"Determining value format for {var_name}")
    
    sample_values = df.select(var_name).filter(col(var_name).isNotNull()).limit(100).collect()
    
    if not sample_values:
        return ("8.1", "8.2")  # Default formats
    
    max_decimals = 0
    for row in sample_values:
        value = row[0]
        if value is not None:
            str_val = str(float(value))
            if '.' in str_val:
                decimals = len(str_val.split('.')[1])
                max_decimals = max(max_decimals, decimals)
    
    mean_decimals = min(max_decimals + 1, 3)
    std_decimals = min(max_decimals + 2, 4)
    
    mean_format = f"8.{mean_decimals}"
    std_format = f"8.{std_decimals}"
    
    logger.info(f"Value formats: mean={mean_format}, std={std_format}")
    return (mean_format, std_format)

def util_boxplot_block_ranges(df: DataFrame, blockvar: str, catvars: str, max_boxes: int = 20) -> List[str]:
    """
    Create block ranges for box plot pagination.
    Replaces %util_boxplot_block_ranges macro functionality
    
    Args:
        df: Input DataFrame
        blockvar: Variable to block on (e.g., visit number)
        catvars: Category variables (e.g., treatment)
        max_boxes: Maximum boxes per page
    
    Returns:
        List of filter expressions for each page
    """
    logger.info(f"Creating boxplot block ranges for {blockvar}")
    
    block_values = df.select(blockvar).distinct().orderBy(blockvar).collect()
    block_values = [row[0] for row in block_values]
    
    cats_per_block = df.groupBy(blockvar).agg(countDistinct(catvars).alias("cat_count")).collect()
    cats_dict = {row[0]: row[1] for row in cats_per_block}
    
    ranges = []
    current_boxes = 0
    current_range = []
    
    for block_val in block_values:
        boxes_in_block = cats_dict.get(block_val, 1)
        
        if current_boxes + boxes_in_block > max_boxes and current_range:
            ranges.append(f"{blockvar} in ({','.join(map(str, current_range))})")
            current_range = [block_val]
            current_boxes = boxes_in_block
        else:
            current_range.append(block_val)
            current_boxes += boxes_in_block
    
    if current_range:
        ranges.append(f"{blockvar} in ({','.join(map(str, current_range))})")
    
    logger.info(f"Created {len(ranges)} page ranges")
    return ranges

def util_get_var_min_max(df: DataFrame, var_name: str, extra_values: List[float] = None) -> Tuple[float, float]:
    """
    Get minimum and maximum values for axis scaling.
    Replaces %util_get_var_min_max macro functionality
    
    Args:
        df: Input DataFrame
        var_name: Variable to analyze
        extra_values: Additional values to consider (e.g., reference lines)
    
    Returns:
        Tuple of (min_value, max_value)
    """
    logger.info(f"Getting min/max values for {var_name}")
    
    result = df.agg(
        spark_min(var_name).alias("min_val"),
        spark_max(var_name).alias("max_val")
    ).collect()[0]
    
    min_val = result["min_val"]
    max_val = result["max_val"]
    
    if extra_values:
        all_values = [v for v in [min_val, max_val] + extra_values if v is not None]
        min_val = min(all_values)
        max_val = max(all_values)
    
    logger.info(f"Variable {var_name} range: {min_val} to {max_val}")
    return (min_val, max_val)

def util_axis_order(min_val: float, max_val: float) -> Tuple[float, float, float]:
    """
    Calculate axis ordering with nice increments.
    Replaces %util_axis_order macro functionality
    
    Args:
        min_val: Minimum value
        max_val: Maximum value
    
    Returns:
        Tuple of (axis_min, axis_max, increment)
    """
    logger.info(f"Calculating axis order for range {min_val} to {max_val}")
    
    if min_val is None or max_val is None:
        return (0, 10, 1)
    
    data_range = max_val - min_val
    
    if data_range == 0:
        center = min_val
        axis_min = center - 1
        axis_max = center + 1
        increment = 0.5
    else:
        log_range = math.log10(data_range)
        power = math.floor(log_range)
        normalized = data_range / (10 ** power)
        
        if normalized <= 1:
            nice_increment = 0.2 * (10 ** power)
        elif normalized <= 2:
            nice_increment = 0.5 * (10 ** power)
        elif normalized <= 5:
            nice_increment = 1 * (10 ** power)
        else:
            nice_increment = 2 * (10 ** power)
        
        axis_min = math.floor(min_val / nice_increment) * nice_increment
        axis_max = math.ceil(max_val / nice_increment) * nice_increment
        increment = nice_increment
    
    logger.info(f"Axis order: min={axis_min}, max={axis_max}, increment={increment}")
    return (axis_min, axis_max, increment)

def util_delete_dsets(*dataset_names: str):
    """
    Placeholder for dataset cleanup.
    Replaces %util_delete_dsets macro functionality
    
    In Spark/Databricks environment, temporary tables are automatically cleaned up.
    """
    logger.info(f"Cleanup requested for datasets: {dataset_names}")
    pass
