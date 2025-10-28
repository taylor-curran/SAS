"""
PhUSE Utility Functions
Reimplementation of PhUSE utility macros used in the original SAS code
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_list, first, min as spark_min, max as spark_max
import logging

def get_spark_session():
    """Get or create Spark session"""
    return SparkSession.builder.getOrCreate()

def util_labels_from_var(df, code_var, label_var):
    """
    Extract parameter labels from variables
    Replicates %util_labels_from_var(css_anadata, paramcd, param) from WPCT-F.07.01.sas line 266
    
    Args:
        df: PySpark DataFrame
        code_var: Variable containing codes (e.g., PARAMCD)
        label_var: Variable containing labels (e.g., PARAM)
    
    Returns:
        dict: Mapping of codes to labels
    """
    try:
        labels_df = df.select(code_var, label_var).distinct().collect()
        
        labels_dict = {}
        for row in labels_df:
            code = row[code_var]
            label = row[label_var]
            if code is not None and label is not None:
                labels_dict[code] = label
        
        logging.info(f"Extracted {len(labels_dict)} labels from {code_var}/{label_var}")
        return labels_dict
        
    except Exception as e:
        logging.error(f"Error extracting labels: {str(e)}")
        return {}

def util_count_unique_values(df, variable, result_name=None):
    """
    Count unique values in a variable
    Replicates %util_count_unique_values(css_anadata, &t_var, trtn) from WPCT-F.07.01.sas line 269
    
    Args:
        df: PySpark DataFrame
        variable: Variable name to count unique values
        result_name: Name for the result (optional)
    
    Returns:
        int: Count of unique values
    """
    try:
        unique_count = df.select(variable).distinct().count()
        
        if result_name:
            logging.info(f"{result_name}: {unique_count} unique values in {variable}")
        else:
            logging.info(f"Found {unique_count} unique values in {variable}")
            
        return unique_count
        
    except Exception as e:
        logging.error(f"Error counting unique values in {variable}: {str(e)}")
        return 0

def util_get_reference_lines(df, low_var, high_var, ref_lines_type):
    """
    Get reference lines for plotting
    Replicates %util_get_reference_lines functionality from WPCT-F.07.01.sas lines 310-311
    
    Args:
        df: PySpark DataFrame
        low_var: Variable containing lower reference limits
        high_var: Variable containing upper reference limits  
        ref_lines_type: Type of reference lines (NONE, UNIFORM, NARROW, ALL)
    
    Returns:
        list: Reference line values
    """
    try:
        if ref_lines_type == "NONE":
            return []
        
        elif ref_lines_type == "UNIFORM":
            lo_values = df.select(low_var).distinct().collect()
            hi_values = df.select(high_var).distinct().collect()
            
            if len(lo_values) == 1 and len(hi_values) == 1:
                lo_val = lo_values[0][low_var]
                hi_val = hi_values[0][high_var]
                if lo_val is not None and hi_val is not None:
                    return [lo_val, hi_val]
        
        elif ref_lines_type == "NARROW":
            lo_val = df.agg(spark_max(low_var)).collect()[0][0]
            hi_val = df.agg(spark_min(high_var)).collect()[0][0]
            if lo_val is not None and hi_val is not None:
                return [lo_val, hi_val]
        
        elif ref_lines_type == "ALL":
            lo_values = [row[low_var] for row in df.select(low_var).distinct().collect() if row[low_var] is not None]
            hi_values = [row[high_var] for row in df.select(high_var).distinct().collect() if row[high_var] is not None]
            return sorted(set(lo_values + hi_values))
        
        return []
        
    except Exception as e:
        logging.error(f"Error getting reference lines: {str(e)}")
        return []

def util_get_var_min_max(df, variable, extra_values=None):
    """
    Get variable min/max values for axis scaling
    Replicates %util_get_var_min_max functionality from WPCT-F.07.01.sas line 331
    
    Args:
        df: PySpark DataFrame
        variable: Variable to get min/max for
        extra_values: Additional values to consider (e.g., reference lines)
    
    Returns:
        dict: Min, max, and range information
    """
    try:
        min_val = df.agg(spark_min(variable)).collect()[0][0]
        max_val = df.agg(spark_max(variable)).collect()[0][0]
        
        if extra_values:
            all_values = [min_val, max_val] + extra_values
            min_val = min([v for v in all_values if v is not None])
            max_val = max([v for v in all_values if v is not None])
        
        range_val = max_val - min_val if min_val is not None and max_val is not None else 0
        
        return {
            'min': min_val,
            'max': max_val,
            'range': range_val
        }
        
    except Exception as e:
        logging.error(f"Error getting min/max for {variable}: {str(e)}")
        return {'min': None, 'max': None, 'range': 0}

def util_value_format(df, variable):
    """
    Create format string for displaying values
    Replicates %util_value_format functionality from WPCT-F.07.01.sas line 337
    
    Args:
        df: PySpark DataFrame
        variable: Variable to determine formatting for
    
    Returns:
        dict: Formatting specifications
    """
    try:
        sample_values = df.select(variable).limit(100).collect()
        values = [row[variable] for row in sample_values if row[variable] is not None]
        
        if not values:
            return {'mean_format': '%.2f', 'std_format': '%.3f'}
        
        max_val = max(abs(v) for v in values)
        
        if max_val >= 1000:
            mean_decimals = 1
            std_decimals = 2
        elif max_val >= 100:
            mean_decimals = 2
            std_decimals = 3
        else:
            mean_decimals = 3
            std_decimals = 4
        
        return {
            'mean_format': f'%.{mean_decimals}f',
            'std_format': f'%.{std_decimals}f'
        }
        
    except Exception as e:
        logging.error(f"Error determining value format: {str(e)}")
        return {'mean_format': '%.2f', 'std_format': '%.3f'}

def util_boxplot_block_ranges(df, blockvar, catvars, max_per_page=20):
    """
    Create block ranges for pagination of box plots
    Replicates %util_boxplot_block_ranges functionality from WPCT-F.07.01.sas line 340
    
    Args:
        df: PySpark DataFrame
        blockvar: Variable to use for blocking (e.g., AVISITN)
        catvars: Categorical variables for grouping
        max_per_page: Maximum boxes per page
    
    Returns:
        list: List of block range specifications
    """
    try:
        if isinstance(catvars, str):
            catvars = [catvars]
        
        unique_blocks = df.select(blockvar).distinct().orderBy(blockvar).collect()
        block_values = [row[blockvar] for row in unique_blocks]
        
        unique_cats = df.select(*catvars).distinct().count()
        
        boxes_per_block = unique_cats
        blocks_per_page = max(1, max_per_page // boxes_per_block)
        
        block_ranges = []
        for i in range(0, len(block_values), blocks_per_page):
            end_idx = min(i + blocks_per_page, len(block_values))
            block_subset = block_values[i:end_idx]
            
            if len(block_subset) == 1:
                range_spec = f"{blockvar} = {block_subset[0]}"
            else:
                range_spec = f"{blockvar} IN ({','.join(map(str, block_subset))})"
            
            block_ranges.append(range_spec)
        
        logging.info(f"Created {len(block_ranges)} block ranges for pagination")
        return block_ranges
        
    except Exception as e:
        logging.error(f"Error creating block ranges: {str(e)}")
        return [f"{blockvar} IS NOT NULL"]

def util_axis_order(min_val, max_val, increment=None):
    """
    Calculate axis ordering for plots
    Replicates %util_axis_order functionality from WPCT-F.07.01.sas line 377
    
    Args:
        min_val: Minimum value
        max_val: Maximum value
        increment: Optional increment value
    
    Returns:
        dict: Axis specification
    """
    try:
        if min_val is None or max_val is None:
            return {'min': 0, 'max': 100, 'increment': 10}
        
        range_val = max_val - min_val
        
        if increment is None:
            if range_val <= 1:
                increment = 0.1
            elif range_val <= 10:
                increment = 1
            elif range_val <= 100:
                increment = 10
            else:
                increment = range_val / 10
        
        axis_min = (min_val // increment) * increment
        axis_max = ((max_val // increment) + 1) * increment
        
        return {
            'min': axis_min,
            'max': axis_max,
            'increment': increment
        }
        
    except Exception as e:
        logging.error(f"Error calculating axis order: {str(e)}")
        return {'min': min_val or 0, 'max': max_val or 100, 'increment': increment or 10}

def util_delete_dsets(*dataset_names):
    """
    Utility function to clean up temporary datasets
    Replicates %util_delete_dsets functionality from WPCT-F.07.01.sas line 438
    
    Args:
        dataset_names: Names of datasets to clean up
    
    Returns:
        bool: Success status
    """
    try:
        spark = get_spark_session()
        
        for dataset_name in dataset_names:
            try:
                spark.catalog.dropTempView(dataset_name)
                logging.info(f"Dropped temporary view: {dataset_name}")
            except:
                pass
        
        return True
        
    except Exception as e:
        logging.error(f"Error cleaning up datasets: {str(e)}")
        return False

if __name__ == "__main__":
    print("PhUSE utilities module loaded successfully")
    
    spark = get_spark_session()
    
    test_data = spark.createDataFrame([
        ("ALB", "Albumin", 35.0, 30.0, 50.0, "P", 1),
        ("ALB", "Albumin", 42.0, 30.0, 50.0, "X-high", 2),
        ("ALB", "Albumin", 38.0, 30.0, 50.0, "X-low", 3),
    ], ["PARAMCD", "PARAM", "AVAL", "A1LO", "A1HI", "TRTP", "AVISITN"])
    
    labels = util_labels_from_var(test_data, "PARAMCD", "PARAM")
    print(f"Labels: {labels}")
    
    count = util_count_unique_values(test_data, "TRTP")
    print(f"Unique treatments: {count}")
    
    ref_lines = util_get_reference_lines(test_data, "A1LO", "A1HI", "UNIFORM")
    print(f"Reference lines: {ref_lines}")
