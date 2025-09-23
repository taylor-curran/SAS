"""
PhUSE Utility Functions
Python implementations of PhUSE utility macros referenced in SAS scripts

This module provides Python equivalents of the PhUSE macros:
- util_labels_from_var
- util_count_unique_values  
- util_get_reference_lines
- util_value_format
- util_axis_order
"""

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, min as spark_min, max as spark_max
import numpy as np
import logging

logger = logging.getLogger(__name__)

def util_labels_from_var(df, value_var, label_var):
    """
    Extract unique values and labels from variables
    
    Replaces SAS macro: %util_labels_from_var(css_anadata, paramcd, param)
    
    Args:
        df: Spark DataFrame or Pandas DataFrame
        value_var (str): Variable containing values
        label_var (str): Variable containing labels
        
    Returns:
        dict: Dictionary with value/label mappings and counts
    """
    if isinstance(df, DataFrame):  # Spark DataFrame
        unique_pairs = df.select(value_var, label_var).distinct().collect()
        
        value_label_map = {}
        values = []
        labels = []
        
        for row in unique_pairs:
            value = row[value_var]
            label = row[label_var]
            value_label_map[value] = label
            values.append(value)
            labels.append(label)
            
    else:  # Pandas DataFrame
        unique_pairs = df[[value_var, label_var]].drop_duplicates()
        
        value_label_map = dict(zip(unique_pairs[value_var], unique_pairs[label_var]))
        values = unique_pairs[value_var].tolist()
        labels = unique_pairs[label_var].tolist()
    
    result = {
        'count': len(values),
        'values': values,
        'labels': labels,
        'value_label_map': value_label_map
    }
    
    logger.info(f"Found {result['count']} unique {value_var} values")
    return result

def util_count_unique_values(df, variable, result_name):
    """
    Count unique values in a variable
    
    Replaces SAS macro: %util_count_unique_values(css_anadata, &t_var, trtn)
    
    Args:
        df: Spark DataFrame or Pandas DataFrame
        variable (str): Variable to count unique values for
        result_name (str): Name for the result
        
    Returns:
        int: Count of unique values
    """
    if isinstance(df, DataFrame):  # Spark DataFrame
        unique_count = df.select(variable).distinct().count()
    else:  # Pandas DataFrame
        unique_count = df[variable].nunique()
    
    logger.info(f"{result_name}: {unique_count} unique values in {variable}")
    return unique_count

def util_get_reference_lines(df, low_var, high_var, ref_lines_type='UNIFORM'):
    """
    Get reference lines for normal range
    
    Replaces SAS macro: %util_get_reference_lines(css_nextparam, nxt_reflines,
                                                  low_var=&lo_var, high_var=&hi_var, ref_lines=&ref_lines)
    
    Args:
        df: Spark DataFrame or Pandas DataFrame
        low_var (str): Variable containing lower limits
        high_var (str): Variable containing upper limits  
        ref_lines_type (str): Type of reference lines ('UNIFORM', 'NARROW', 'ALL', 'NONE')
        
    Returns:
        list: Reference line values
    """
    if ref_lines_type == 'NONE':
        return []
    
    if isinstance(df, DataFrame):  # Spark DataFrame
        low_values = df.select(low_var).distinct().collect()
        high_values = df.select(high_var).distinct().collect()
        low_values = [row[low_var] for row in low_values if row[low_var] is not None]
        high_values = [row[high_var] for row in high_values if row[high_var] is not None]
    else:  # Pandas DataFrame
        low_values = df[low_var].dropna().unique().tolist()
        high_values = df[high_var].dropna().unique().tolist()
    
    ref_lines = []
    
    if ref_lines_type == 'UNIFORM':
        if len(set(low_values)) == 1:
            ref_lines.append(low_values[0])
        if len(set(high_values)) == 1:
            ref_lines.append(high_values[0])
            
    elif ref_lines_type == 'NARROW':
        if low_values:
            ref_lines.append(max(low_values))
        if high_values:
            ref_lines.append(min(high_values))
            
    elif ref_lines_type == 'ALL':
        ref_lines.extend(low_values)
        ref_lines.extend(high_values)
        ref_lines = list(set(ref_lines))  # Remove duplicates
    
    logger.info(f"Generated {len(ref_lines)} reference lines using {ref_lines_type} method")
    return ref_lines

def util_value_format(df, measurement_var):
    """
    Create format string for displaying values with appropriate precision
    
    Replaces SAS macro: %util_value_format(css_nexttimept, &m_var)
    
    Args:
        df: Spark DataFrame or Pandas DataFrame
        measurement_var (str): Measurement variable
        
    Returns:
        dict: Format specifications for mean and std
    """
    if isinstance(df, DataFrame):  # Spark DataFrame
        sample_values = df.select(measurement_var).limit(1000).toPandas()[measurement_var]
    else:  # Pandas DataFrame
        sample_values = df[measurement_var]
    
    sample_values = sample_values.dropna()
    
    if len(sample_values) == 0:
        return {'mean_format': '%.1f', 'std_format': '%.2f'}
    
    data_range = sample_values.max() - sample_values.min()
    
    if data_range > 100:
        mean_decimals = 1
        std_decimals = 2
    elif data_range > 10:
        mean_decimals = 2
        std_decimals = 3
    else:
        mean_decimals = 3
        std_decimals = 4
    
    format_spec = {
        'mean_format': f'%.{mean_decimals}f',
        'std_format': f'%.{std_decimals}f'
    }
    
    logger.info(f"Value format: mean={format_spec['mean_format']}, std={format_spec['std_format']}")
    return format_spec

def util_axis_order(min_val, max_val, num_ticks=5):
    """
    Calculate axis order and tick marks
    
    Replaces SAS macro: %util_axis_order(%scan(&aval_min_max,1,%str( )), %scan(&aval_min_max,2,%str( )))
    
    Args:
        min_val (float): Minimum value
        max_val (float): Maximum value
        num_ticks (int): Desired number of tick marks
        
    Returns:
        dict: Axis configuration with min, max, and increment
    """
    if min_val is None or max_val is None:
        return {'min': 0, 'max': 100, 'increment': 20}
    
    data_range = max_val - min_val
    
    if data_range == 0:
        axis_min = min_val - 1
        axis_max = max_val + 1
        increment = 0.5
    else:
        padding = data_range * 0.1
        axis_min = min_val - padding
        axis_max = max_val + padding
        
        raw_increment = (axis_max - axis_min) / num_ticks
        magnitude = 10 ** np.floor(np.log10(raw_increment))
        normalized = raw_increment / magnitude
        
        if normalized <= 1:
            nice_increment = 1 * magnitude
        elif normalized <= 2:
            nice_increment = 2 * magnitude
        elif normalized <= 5:
            nice_increment = 5 * magnitude
        else:
            nice_increment = 10 * magnitude
        
        axis_min = np.floor(axis_min / nice_increment) * nice_increment
        axis_max = np.ceil(axis_max / nice_increment) * nice_increment
        increment = nice_increment
    
    result = {
        'min': axis_min,
        'max': axis_max,
        'increment': increment
    }
    
    logger.info(f"Axis order: min={result['min']}, max={result['max']}, increment={result['increment']}")
    return result

def util_boxplot_block_ranges(df, blockvar, catvars, max_per_page=20):
    """
    Create block ranges for pagination of box plots
    
    Replaces SAS macro: %util_boxplot_block_ranges(css_nexttimept, blockvar=avisitn, catvars=&tn_var)
    
    Args:
        df: Spark DataFrame or Pandas DataFrame
        blockvar (str): Variable to block on (e.g., visit)
        catvars (str or list): Categorical variables
        max_per_page (int): Maximum boxes per page
        
    Returns:
        list: List of filter conditions for each page
    """
    if isinstance(df, DataFrame):  # Spark DataFrame
        unique_blocks = df.select(blockvar).distinct().collect()
        block_values = sorted([row[blockvar] for row in unique_blocks])
    else:  # Pandas DataFrame
        block_values = sorted(df[blockvar].unique())
    
    pages = []
    current_page = []
    
    for block_val in block_values:
        current_page.append(block_val)
        
        if len(current_page) >= max_per_page:
            pages.append(current_page)
            current_page = []
    
    if current_page:
        pages.append(current_page)
    
    filter_conditions = []
    for page in pages:
        if len(page) == 1:
            condition = f"{blockvar} = {page[0]}"
        else:
            condition = f"{blockvar} in ({', '.join(map(str, page))})"
        filter_conditions.append(condition)
    
    logger.info(f"Created {len(filter_conditions)} page blocks for {len(block_values)} {blockvar} values")
    return filter_conditions
