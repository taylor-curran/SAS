import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import os
import logging

def get_spark_session():
    """Get or create Spark session"""
    return SparkSession.builder.getOrCreate()

def create_box_plot(data_df, stats_df, config):
    """
    Create box plot using Plotly to replace SAS SGRENDER functionality
    Replicates the box plot generation from WPCT-F.07.01.sas lines 398-423
    """
    treatment_var = config.get('treatment_var', 'trtp_short')
    measurement_var = config.get('measurement_var', 'AVAL')
    visit_var = config.get('visit_var', 'AVISITN')
    parameter_label = config.get('parameter_label', 'Laboratory Parameter')
    timepoint_label = config.get('timepoint_label', 'Analysis Timepoint')
    
    fig = go.Figure()
    
    treatments = data_df[treatment_var].unique()
    visits = sorted(data_df[visit_var].unique())
    
    colors = px.colors.qualitative.Set1[:len(treatments)]
    
    for i, treatment in enumerate(treatments):
        treatment_data = data_df[data_df[treatment_var] == treatment]
        
        for visit in visits:
            visit_data = treatment_data[treatment_data[visit_var] == visit]
            
            if len(visit_data) > 0:
                fig.add_trace(go.Box(
                    y=visit_data[measurement_var],
                    x=[f"Visit {visit}"] * len(visit_data),
                    name=f"{treatment}",
                    marker_color=colors[i],
                    boxpoints='outliers',
                    jitter=0.3,
                    pointpos=-1.8,
                    showlegend=(visit == visits[0])
                ))
    
    reference_lines = config.get('reference_lines', [])
    for ref_line in reference_lines:
        fig.add_hline(
            y=ref_line, 
            line_dash="dash", 
            line_color="red",
            annotation_text=f"Reference: {ref_line:.2f}"
        )
    
    fig.update_layout(
        title=f"Box Plot - {parameter_label} Observed Values by Visit, {timepoint_label}",
        xaxis_title="Visit",
        yaxis_title=parameter_label,
        boxmode='group',
        template='plotly_white',
        width=1200,
        height=600,
        font=dict(size=12),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    axis_range = config.get('axis_range', {})
    if 'min' in axis_range and 'max' in axis_range:
        fig.update_yaxis(range=[axis_range['min'], axis_range['max']])
    
    return fig

def add_statistical_annotations(fig, stats_df, config):
    """
    Add statistical annotations to the box plot
    Replicates the statistical table functionality from SAS
    """
    treatment_var = config.get('treatment_var', 'trtp_short')
    
    annotations = []
    
    for _, row in stats_df.iterrows():
        treatment = row[treatment_var]
        visit = row['AVISITN']
        n = row['n']
        mean = row['mean_formatted'] if 'mean_formatted' in row else row['mean']
        std = row['std_formatted'] if 'std_formatted' in row else row['std']
        
        annotation_text = f"N={n}<br>Mean={mean:.2f}<br>SD={std:.3f}"
        
        annotations.append(
            dict(
                x=f"Visit {visit}",
                y=mean + std,
                text=annotation_text,
                showarrow=False,
                font=dict(size=10),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="black",
                borderwidth=1
            )
        )
    
    fig.update_layout(annotations=annotations)
    return fig

def create_outlier_overlay(fig, data_df, config):
    """
    Add outlier overlay to highlight values outside normal range
    Replicates the red dot functionality for normal range outliers
    """
    treatment_var = config.get('treatment_var', 'trtp_short')
    measurement_var = config.get('measurement_var', 'AVAL')
    visit_var = config.get('visit_var', 'AVISITN')
    
    outlier_data = data_df[data_df['is_normal_range_outlier'] == True]
    
    if len(outlier_data) > 0:
        fig.add_trace(go.Scatter(
            x=[f"Visit {visit}" for visit in outlier_data[visit_var]],
            y=outlier_data[measurement_var],
            mode='markers',
            marker=dict(
                color='red',
                size=8,
                symbol='circle',
                line=dict(width=2, color='darkred')
            ),
            name='Outside Normal Range',
            showlegend=True
        ))
    
    return fig

def generate_multiple_plots(data_df, stats_df, metadata, config):
    """
    Generate multiple plots for different parameters and timepoints
    Replicates the multi-page functionality from SAS
    """
    plots = []
    
    parameters = data_df['PARAMCD'].unique()
    timepoints = data_df['ATPTN'].unique()
    
    for param in parameters:
        for timepoint in timepoints:
            param_data = data_df[
                (data_df['PARAMCD'] == param) & 
                (data_df['ATPTN'] == timepoint)
            ]
            
            param_stats = stats_df[
                (stats_df['PARAMCD'] == param) if 'PARAMCD' in stats_df.columns else stats_df
            ]
            
            if len(param_data) > 0:
                plot_config = config.copy()
                plot_config['parameter_label'] = f"{param} Parameter"
                plot_config['timepoint_label'] = f"Timepoint {timepoint}"
                
                fig = create_box_plot(param_data, param_stats, plot_config)
                fig = add_statistical_annotations(fig, param_stats, plot_config)
                fig = create_outlier_overlay(fig, param_data, plot_config)
                
                plots.append({
                    'figure': fig,
                    'parameter': param,
                    'timepoint': timepoint,
                    'filename': f"boxplot_{param}_timepoint_{timepoint}.html"
                })
    
    return plots

def save_plots(plots, output_path):
    """
    Save plots to specified output directory
    """
    os.makedirs(output_path, exist_ok=True)
    
    saved_files = []
    
    for plot_info in plots:
        filepath = os.path.join(output_path, plot_info['filename'])
        plot_info['figure'].write_html(filepath)
        saved_files.append(filepath)
        
        logging.info(f"Saved plot: {filepath}")
    
    return saved_files

def generate_box_plots(config):
    """
    Main visualization function
    Replaces SAS SGRENDER with Python box plots
    
    Args:
        config (dict): Configuration containing visualization parameters
    
    Returns:
        dict: Status and metadata about generated visualizations
    """
    spark = get_spark_session()
    
    input_path = config['input_path']
    output_path = config['output_path']
    
    stats_df = spark.read.parquet(input_path).toPandas()
    
    try:
        metadata_df = spark.read.parquet(input_path + "_metadata").toPandas()
        metadata = metadata_df.iloc[0].to_dict()
    except:
        metadata = {}
    
    try:
        data_path = input_path.replace('statistics', 'preprocessed_data')
        data_df = spark.read.parquet(data_path).toPandas()
    except:
        data_df = stats_df
    
    plot_config = {
        'treatment_var': config.get('treatment_var', 'trtp_short'),
        'measurement_var': config.get('measurement_var', 'AVAL'),
        'visit_var': config.get('visit_var', 'AVISITN'),
        'reference_lines': metadata.get('reference_lines', []),
        'axis_range': metadata.get('axis_range', {}),
        'max_boxes_per_page': config.get('max_boxes_per_page', 20)
    }
    
    plots = generate_multiple_plots(data_df, stats_df, metadata, plot_config)
    
    saved_files = save_plots(plots, output_path)
    
    result = {
        'status': 'success',
        'plots_generated': len(plots),
        'output_files': saved_files,
        'output_path': output_path
    }
    
    logging.info(f"Visualization generation completed: {result}")
    return result

if __name__ == "__main__":
    test_config = {
        'reference_lines': 'UNIFORM',
        'max_boxes_per_page': 20,
        'output_format': 'html',
        'input_path': '/tmp/clinical_analysis/statistics',
        'output_path': '/tmp/clinical_analysis/visualizations/'
    }
    
    result = generate_box_plots(test_config)
    print(f"Visualization result: {result}")
