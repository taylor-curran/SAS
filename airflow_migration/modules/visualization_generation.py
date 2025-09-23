"""
Visualization Generation Module
Replaces SAS SGRENDER box plot generation

This module creates box plots equivalent to the SGRENDER template
operations in WPCT-F.07.01.sas (lines 398-422)
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from pyspark.sql import SparkSession
import logging
import os

logger = logging.getLogger(__name__)

def generate_box_plots(config, **context):
    """
    Generate box plot visualizations for clinical data
    
    Replaces SAS SGRENDER from WPCT-F.07.01.sas lines 398-422:
    proc sgrender data=css_plot template=PhUSEboxplot;
      dynamic _MARKERS="&t_var" _XVAR='avisitn' _BLOCKLABEL='avisit' 
              _YVAR="&m_var" _YOUTLIERS='m_var_outlier'
              _YLABEL="&&paramcd_lab&pdx" _N='n' _MEAN='mean' _STD='std'
              _DATAMIN='datamin' _Q1='q1' _MEDIAN='median' _Q3='q3' _DATAMAX='datamax';
    run;
    
    Args:
        config (dict): Pipeline configuration parameters
        **context: Airflow context
        
    Returns:
        str: Path to generated visualizations
    """
    logger.info("Starting box plot generation")
    
    spark = SparkSession.builder \
        .appName("ClinicalVisualization") \
        .getOrCreate()
    
    try:
        plot_data_path = context['task_instance'].xcom_pull(
            task_ids='statistical_calculations', 
            key='plot_data_path'
        )
        
        raw_df = spark.read.parquet(f"{plot_data_path}/raw_data")
        stats_df = spark.read.parquet(f"{plot_data_path}/stats_data")
        
        raw_pandas = raw_df.toPandas()
        stats_pandas = stats_df.toPandas()
        
        logger.info(f"Loaded {len(raw_pandas)} raw records and {len(stats_pandas)} statistics for visualization")
        
        output_dir = f"{config['output_path']}/visualizations"
        os.makedirs(output_dir, exist_ok=True)
        
        parameters = raw_pandas['PARAMCD'].unique()
        timepoints = sorted(raw_pandas['AVISITN'].unique())
        
        visualization_files = []
        
        for param in parameters:
            for timepoint in timepoints:
                param_data = raw_pandas[
                    (raw_pandas['PARAMCD'] == param) & 
                    (raw_pandas['AVISITN'] == timepoint)
                ]
                
                if len(param_data) == 0:
                    continue
                
                param_stats = stats_pandas[
                    (stats_pandas['PARAMCD'] == param) & 
                    (stats_pandas['AVISITN'] == timepoint)
                ]
                
                fig = create_clinical_box_plot(
                    param_data, 
                    param_stats, 
                    config, 
                    param, 
                    timepoint
                )
                
                filename = f"WPCT-F.07.01_Box_plot_{param}_by_visit_for_timepoint_{timepoint}.html"
                filepath = os.path.join(output_dir, filename)
                fig.write_html(filepath)
                visualization_files.append(filepath)
                
                logger.info(f"Generated box plot: {filename}")
        
        logger.info(f"Box plot generation completed. {len(visualization_files)} files created in: {output_dir}")
        
        context['task_instance'].xcom_push(key='visualization_files', value=visualization_files)
        context['task_instance'].xcom_push(key='visualization_count', value=len(visualization_files))
        
        return output_dir
        
    except Exception as e:
        logger.error(f"Error during visualization generation: {str(e)}")
        raise
    finally:
        spark.stop()

def create_clinical_box_plot(raw_data, stats_data, config, parameter, timepoint):
    """
    Create a single box plot for clinical data
    
    Equivalent to SAS PhUSEboxplot template with dynamic variables
    
    Args:
        raw_data (pd.DataFrame): Raw measurement data
        stats_data (pd.DataFrame): Statistical summaries
        config (dict): Pipeline configuration
        parameter (str): Parameter code (e.g., 'ALB')
        timepoint: Analysis timepoint
        
    Returns:
        plotly.graph_objects.Figure: Box plot figure
    """
    fig = make_subplots(
        rows=1, cols=1,
        subplot_titles=[f"Box Plot - {parameter} Observed Values by Visit, Analysis Timepoint: {timepoint}"]
    )
    
    visits = sorted(raw_data['AVISITN'].unique())
    treatments = sorted(raw_data[config['treatment_var']].unique())
    
    colors = px.colors.qualitative.Set1[:len(treatments)]
    
    for i, treatment in enumerate(treatments):
        treatment_data = raw_data[raw_data[config['treatment_var']] == treatment]
        
        fig.add_trace(
            go.Box(
                x=treatment_data['AVISITN'],
                y=treatment_data[config['measurement_var']],
                name=treatment,
                boxpoints='outliers',  # Show outliers
                marker_color=colors[i],
                line=dict(color=colors[i]),
                showlegend=True
            )
        )
        
        outliers = treatment_data[treatment_data['m_var_outlier'].notna()]
        if len(outliers) > 0:
            fig.add_trace(
                go.Scatter(
                    x=outliers['AVISITN'],
                    y=outliers['m_var_outlier'],
                    mode='markers',
                    marker=dict(color='red', size=6, symbol='circle'),
                    name=f'{treatment} - Outside Normal Range',
                    showlegend=False
                )
            )
    
    ref_lines = get_reference_lines_for_plot(raw_data, config)
    for ref_value in ref_lines:
        fig.add_hline(
            y=ref_value,
            line_dash="dash",
            line_color="gray",
            annotation_text=f"Reference: {ref_value}"
        )
    
    fig.update_layout(
        title=dict(
            text=f"Box Plot - {parameter} Observed Values by Visit, Analysis Timepoint: {timepoint}",
            x=0.02,
            font=dict(size=14)
        ),
        xaxis_title="Analysis Visit Number",
        yaxis_title=f"{parameter} Value",
        width=1000,
        height=600,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        )
    )
    
    footnote_text = (
        "Box plot type is schematic: the box shows median and interquartile range (IQR, the box height); "
        "the whiskers extend to the minimum and maximum data points within 1.5 IQR of the lower and upper quartiles, respectively. "
        "Values outside the whiskers are shown as outliers. "
        "Means are marked with a different symbol for each treatment. Red dots indicate measures outside the normal reference range."
    )
    
    fig.add_annotation(
        text=footnote_text,
        xref="paper", yref="paper",
        x=0, y=-0.15,
        showarrow=False,
        font=dict(size=10),
        align="left"
    )
    
    return fig

def get_reference_lines_for_plot(data, config):
    """
    Get reference line values for plotting
    
    Args:
        data (pd.DataFrame): Measurement data
        config (dict): Pipeline configuration
        
    Returns:
        list: Reference line values
    """
    ref_lines_type = config.get('ref_lines', 'UNIFORM')
    
    if ref_lines_type == 'NONE':
        return []
    elif ref_lines_type == 'UNIFORM':
        low_values = data[config['low_limit_var']].dropna().unique()
        high_values = data[config['high_limit_var']].dropna().unique()
        
        ref_lines = []
        if len(low_values) == 1:
            ref_lines.append(low_values[0])
        if len(high_values) == 1:
            ref_lines.append(high_values[0])
        
        return ref_lines
    
    return []

def create_summary_table(stats_data, config):
    """
    Create summary statistics table (equivalent to SAS AXISTABLE)
    
    Args:
        stats_data (pd.DataFrame): Statistical summaries
        config (dict): Pipeline configuration
        
    Returns:
        pd.DataFrame: Formatted summary table
    """
    summary_table = stats_data.copy()
    
    summary_table['mean'] = summary_table['mean'].round(2)
    summary_table['std'] = summary_table['std'].round(3)
    summary_table['median'] = summary_table['median'].round(1)
    
    return summary_table
