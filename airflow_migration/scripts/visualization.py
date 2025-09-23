import argparse
import yaml
from pyspark.sql import SparkSession
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_box_plots(stats_df, raw_df, config, output_path):
    """
    Create box plots equivalent to SAS SGRENDER functionality.
    Replaces SGRENDER template from WPCT-F.07.01.sas lines 398-423
    """
    logger.info("Creating box plots")
    
    stats_pd = stats_df.toPandas()
    raw_pd = raw_df.toPandas()
    
    parameter = config['analysis']['parameter']
    measurement_var = config['variables']['measurement_var']
    treatment_var = config['variables']['treatment_var']
    color_palette = config['visualization']['color_palette']
    
    parameters = stats_pd['PARAMCD'].unique() if 'PARAMCD' in stats_pd.columns else [parameter]
    timepoints = stats_pd['ATPTN'].unique() if 'ATPTN' in stats_pd.columns else [1]
    
    for param in parameters:
        for timepoint in timepoints:
            param_data = raw_pd[raw_pd['PARAMCD'] == param] if 'PARAMCD' in raw_pd.columns else raw_pd
            param_stats = stats_pd[stats_pd['PARAMCD'] == param] if 'PARAMCD' in stats_pd.columns else stats_pd
            
            fig = create_parameter_boxplot(param_data, param_stats, config, param, timepoint)
            
            filename = f"WPCT-F.07.01_Box_plot_{param}_by_visit_for_timepoint_{timepoint}.html"
            filepath = os.path.join(output_path, filename)
            fig.write_html(filepath)
            
            logger.info(f"Saved box plot: {filepath}")

def create_parameter_boxplot(raw_data, stats_data, config, parameter, timepoint):
    """
    Create individual box plot for a parameter and timepoint.
    """
    measurement_var = config['variables']['measurement_var']
    treatment_var = config['variables']['treatment_var']
    color_palette = config['visualization']['color_palette']
    
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.8, 0.2],
        subplot_titles=[f"Box Plot - {parameter} Observed Values by Visit", "Summary Statistics"],
        vertical_spacing=0.1
    )
    
    visits = sorted(raw_data['AVISITN'].unique())
    treatments = sorted(raw_data[treatment_var].unique())
    
    for i, treatment in enumerate(treatments):
        treatment_data = raw_data[raw_data[treatment_var] == treatment]
        color = color_palette[i % len(color_palette)]
        
        fig.add_trace(
            go.Box(
                x=treatment_data['AVISITN'],
                y=treatment_data[measurement_var],
                name=treatment,
                boxpoints='outliers',
                marker_color=color,
                line_color=color,
                showlegend=True
            ),
            row=1, col=1
        )
        
        treatment_stats = stats_data[stats_data[treatment_var] == treatment]
        fig.add_trace(
            go.Scatter(
                x=treatment_stats['AVISITN'],
                y=treatment_stats['mean'],
                mode='markers',
                marker=dict(symbol='diamond', size=8, color=color),
                name=f'{treatment} Mean',
                showlegend=False
            ),
            row=1, col=1
        )
    
    add_reference_lines(fig, raw_data, config)
    
    add_statistics_table(fig, stats_data, config)
    
    fig.update_layout(
        title=f"Box Plot - {parameter} Observed Values by Visit, Analysis Timepoint: {timepoint}",
        width=config['visualization']['width'],
        height=config['visualization']['height'],
        showlegend=True,
        annotations=[
            dict(
                text="Box plot type is schematic: the box shows median and interquartile range (IQR, the box height); the whiskers extend to the minimum<br>" +
                     "and maximum data points within 1.5 IQR of the lower and upper quartiles, respectively. Values outside the whiskers are shown as outliers.<br>" +
                     "Means are marked with a different symbol for each treatment. Red dots indicate measures outside the normal reference range.",
                showarrow=False,
                xref="paper", yref="paper",
                x=0, y=-0.1,
                xanchor='left', yanchor='top',
                font=dict(size=10)
            )
        ]
    )
    
    fig.update_xaxes(title_text="Visit Number", row=1, col=1)
    fig.update_yaxes(title_text=parameter, row=1, col=1)
    
    return fig

def add_reference_lines(fig, data, config):
    """
    Add reference lines based on normal range limits.
    Replaces reference line logic from SAS
    """
    ref_lines_config = config['analysis']['reference_lines']
    low_ref_var = config['variables']['low_ref_var']
    high_ref_var = config['variables']['high_ref_var']
    
    if ref_lines_config == 'NONE':
        return
    
    if low_ref_var in data.columns and high_ref_var in data.columns:
        low_refs = data[low_ref_var].dropna().unique()
        high_refs = data[high_ref_var].dropna().unique()
        
        if ref_lines_config == 'UNIFORM':
            if len(low_refs) == 1:
                fig.add_hline(y=low_refs[0], line_dash="dash", line_color="red", 
                             annotation_text="Lower Normal Limit")
            if len(high_refs) == 1:
                fig.add_hline(y=high_refs[0], line_dash="dash", line_color="red",
                             annotation_text="Upper Normal Limit")
        
        elif ref_lines_config == 'NARROW':
            if len(low_refs) > 0:
                fig.add_hline(y=max(low_refs), line_dash="dash", line_color="red",
                             annotation_text="Lower Normal Limit")
            if len(high_refs) > 0:
                fig.add_hline(y=min(high_refs), line_dash="dash", line_color="red",
                             annotation_text="Upper Normal Limit")

def add_statistics_table(fig, stats_data, config):
    """
    Add summary statistics table below the plot.
    """
    treatment_var = config['variables']['treatment_var']
    
    table_data = []
    for _, row in stats_data.iterrows():
        table_data.append([
            row['AVISIT'],
            row[treatment_var],
            f"{row['n']:.0f}",
            f"{row['mean']:.2f}",
            f"{row['std']:.3f}",
            f"{row['median']:.1f}",
            f"{row['datamin']:.1f}",
            f"{row['datamax']:.1f}"
        ])
    
    fig.add_trace(
        go.Table(
            header=dict(
                values=['Visit', 'Treatment', 'N', 'Mean', 'Std Dev', 'Median', 'Min', 'Max'],
                fill_color='lightgray',
                align='center'
            ),
            cells=dict(
                values=list(zip(*table_data)) if table_data else [[]]*8,
                fill_color='white',
                align='center'
            )
        ),
        row=2, col=1
    )

def setup_output_directory(output_path):
    """
    Create output directory if it doesn't exist.
    """
    os.makedirs(output_path, exist_ok=True)
    logger.info(f"Output directory ready: {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Clinical Data Visualization')
    parser.add_argument('--config-path', required=True, help='Path to configuration file')
    parser.add_argument('--input-table', required=True, help='Input statistics table name')
    parser.add_argument('--output-path', required=True, help='Output path for visualizations')
    
    args = parser.parse_args()
    
    with open(args.config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    spark = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .config("spark.executor.memory", config['spark']['executor_memory']) \
        .config("spark.driver.memory", config['spark']['driver_memory']) \
        .getOrCreate()
    
    try:
        setup_output_directory(args.output_path)
        
        stats_df = spark.table(args.input_table)
        logger.info(f"Loaded statistics for {stats_df.count()} groups")
        
        raw_df = spark.table("clinical_filtered_data")
        logger.info(f"Loaded {raw_df.count()} raw observations")
        
        create_box_plots(stats_df, raw_df, config, args.output_path)
        
        logger.info("Visualization generation completed successfully")
        
    except Exception as e:
        logger.error(f"Visualization generation failed: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
