"""
Visualization Script

This script generates interactive box plots from the statistical analysis results,
replacing the SAS SGRENDER visualization with Plotly.
"""

import os
import json
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.phuse_utils import get_reference_lines, get_var_min_max, axis_order


def read_analysis_data(analysis_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Read the analysis data, statistics, and metadata.
    
    Args:
        analysis_dir: Directory containing the analysis results
        
    Returns:
        Tuple of (data DataFrame, statistics DataFrame, metadata dictionary)
    """
    data_df = pd.read_parquet(os.path.join(analysis_dir, "data.parquet"))
    
    stats_df = pd.read_parquet(os.path.join(analysis_dir, "stats.parquet"))
    
    with open(os.path.join(analysis_dir, "metadata.json"), "r") as f:
        metadata = json.load(f)
    
    return data_df, stats_df, metadata


def create_box_plot(
    data_df: pd.DataFrame,
    stats_df: pd.DataFrame,
    metadata: Dict,
    config: Dict,
    output_dir: str
) -> None:
    """
    Create an interactive box plot using Plotly.
    
    Args:
        data_df: DataFrame containing the analysis data
        stats_df: DataFrame containing the statistics
        metadata: Dictionary containing metadata about the parameter and timepoint
        config: Configuration dictionary
        output_dir: Directory to save the plot
    """
    measure_var = config.get("variables", {}).get("measurement_var", "aval")
    treatment_var = config.get("variables", {}).get("treatment_var", "trtp_short")
    treatment_num_var = config.get("variables", {}).get("treatment_num_var", "trtpn")
    low_var = config.get("variables", {}).get("low_limit_var", "a1lo")
    high_var = config.get("variables", {}).get("high_limit_var", "a1hi")
    ref_lines_type = config.get("plotting", {}).get("ref_lines", "UNIFORM")
    
    visits = sorted(data_df["avisitn"].unique())
    treatments = sorted(data_df[treatment_var].unique())
    
    fig = go.Figure()
    
    for visit in visits:
        visit_label = data_df[data_df["avisitn"] == visit]["avisit"].iloc[0]
        
        for i, treatment in enumerate(treatments):
            visit_trt_data = data_df[(data_df["avisitn"] == visit) & (data_df[treatment_var] == treatment)]
            
            if visit_trt_data.empty:
                continue
            
            values = visit_trt_data[measure_var].dropna().tolist()
            
            if not values:
                continue
            
            outliers = visit_trt_data[f"{measure_var}_outlier"].dropna().tolist()
            
            fig.add_trace(go.Box(
                x=[f"{visit_label} - {treatment}"],
                y=values,
                name=f"Visit {visit_label}, {treatment}",
                boxmean=True,  # Show mean as a dashed line
                marker=dict(
                    color=f"hsl({(i * 360) // len(treatments)}, 70%, 50%)"
                ),
                showlegend=False
            ))
            
            if outliers:
                fig.add_trace(go.Scatter(
                    x=[f"{visit_label} - {treatment}"] * len(outliers),
                    y=outliers,
                    mode="markers",
                    marker=dict(
                        color="red",
                        symbol="circle",
                        size=8
                    ),
                    name="Outside Normal Range",
                    showlegend=i == 0 and visit == visits[0]  # Show legend only once
                ))
    
    if ref_lines_type != "NONE":
        ref_lines = []
        
        if ref_lines_type == "UNIFORM":
            low_values = data_df[low_var].dropna().unique()
            high_values = data_df[high_var].dropna().unique()
            
            if len(low_values) == 1:
                ref_lines.append(low_values[0])
            
            if len(high_values) == 1:
                ref_lines.append(high_values[0])
        
        elif ref_lines_type == "NARROW":
            low_values = data_df[low_var].dropna().unique()
            high_values = data_df[high_var].dropna().unique()
            
            if len(low_values) > 0:
                ref_lines.append(max(low_values))
            
            if len(high_values) > 0:
                ref_lines.append(min(high_values))
        
        for ref_line in ref_lines:
            fig.add_shape(
                type="line",
                x0=-0.5,
                y0=ref_line,
                x1=len(visits) * len(treatments) - 0.5,
                y1=ref_line,
                line=dict(
                    color="rgba(0, 0, 255, 0.5)",
                    width=1,
                    dash="dash"
                )
            )
    
    table_data = []
    table_header = ["Visit - Treatment", "N", "Mean", "Std Dev", "Min", "Q1", "Median", "Q3", "Max"]
    
    for visit in visits:
        visit_label = data_df[data_df["avisitn"] == visit]["avisit"].iloc[0]
        
        for treatment in treatments:
            stats_row = stats_df[(stats_df["avisitn"] == visit) & (stats_df[treatment_var] == treatment)]
            
            if stats_row.empty:
                continue
            
            row = [
                f"{visit_label} - {treatment}",
                f"{int(stats_row['n'].iloc[0])}",
                f"{stats_row['mean'].iloc[0]:.1f}",
                f"{stats_row['std'].iloc[0]:.2f}",
                f"{stats_row['datamin'].iloc[0]:.1f}",
                f"{stats_row['q1'].iloc[0]:.1f}",
                f"{stats_row['median'].iloc[0]:.1f}",
                f"{stats_row['q3'].iloc[0]:.1f}",
                f"{stats_row['datamax'].iloc[0]:.1f}"
            ]
            
            table_data.append(row)
    
    fig.update_layout(
        title=f"Box Plot - {metadata['parameter_label']} Observed Values by Visit, Analysis Timepoint: {metadata['timepoint_label']}",
        xaxis_title="Visit - Treatment",
        yaxis_title=metadata['parameter_label'],
        boxmode="group",
        template="plotly_white",
        width=1000,
        height=600,
        margin=dict(t=100, b=200),  # Extra space at bottom for table
        annotations=[
            dict(
                x=0.5,
                y=-0.15,
                showarrow=False,
                text="<br>".join([
                    "Box plot type is schematic: the box shows median and interquartile range (IQR, the box height); the whiskers extend to the minimum",
                    "and maximum data points within 1.5 IQR of the lower and upper quartiles, respectively. Values outside the whiskers are shown as outliers.",
                    "Means are marked with a different symbol for each treatment. Red dots indicate measures outside the normal reference range."
                ]),
                xref="paper",
                yref="paper",
                font=dict(size=10)
            )
        ]
    )
    
    table = go.Figure(data=[go.Table(
        header=dict(
            values=table_header,
            fill_color="paleturquoise",
            align="center",
            font=dict(size=12)
        ),
        cells=dict(
            values=list(zip(*table_data)),
            fill_color="lavender",
            align="center",
            font=dict(size=11)
        )
    )])
    
    table.update_layout(
        title="Summary Statistics",
        width=1000,
        height=400
    )
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    fig.write_html(os.path.join(output_dir, f"boxplot_{metadata['parameter_code']}_timepoint_{metadata['timepoint_num']}.html"))
    
    fig.write_image(os.path.join(output_dir, f"boxplot_{metadata['parameter_code']}_timepoint_{metadata['timepoint_num']}.png"))
    
    table.write_html(os.path.join(output_dir, f"stats_{metadata['parameter_code']}_timepoint_{metadata['timepoint_num']}.html"))
    table.write_image(os.path.join(output_dir, f"stats_{metadata['parameter_code']}_timepoint_{metadata['timepoint_num']}.png"))
    
    print(f"Created box plot for {metadata['parameter_label']}, timepoint {metadata['timepoint_label']}")


def generate_visualizations(
    config: Dict,
    input_path: str,
    output_path: str
) -> None:
    """
    Generate visualizations for all parameters and timepoints.
    
    Args:
        config: Configuration dictionary
        input_path: Path to the analysis results
        output_path: Path to save the visualizations
    """
    parameters = config.get("data_sources", {}).get("parameters", ["ALB"])
    
    for param in parameters:
        param_dir = os.path.join(input_path, f"param_{param.lower()}")
        
        timepoints = [d for d in os.listdir(param_dir) if d.startswith("timepoint_")]
        
        for tp_dir in timepoints:
            analysis_dir = os.path.join(param_dir, tp_dir)
            
            data_df, stats_df, metadata = read_analysis_data(analysis_dir)
            
            output_dir = os.path.join(output_path, f"param_{param.lower()}", tp_dir)
            
            create_box_plot(data_df, stats_df, metadata, config, output_dir)
    
    print(f"Visualization generation completed successfully. Results saved to {output_path}")


def main(config_path: str):
    """
    Main function to run the visualization process.
    
    Args:
        config_path: Path to the configuration file
    """
    import yaml
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    try:
        input_path = config.get("paths", {}).get("analysis_output_path", "/tmp/clinical_data_analysis")
        output_path = config.get("paths", {}).get("visualization_output_path", "/tmp/clinical_data_visualization")
        
        generate_visualizations(config, input_path, output_path)
        
    except Exception as e:
        print(f"Error generating visualizations: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate visualizations for clinical trial data")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    
    args = parser.parse_args()
    main(args.config)
