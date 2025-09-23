#!/usr/bin/env python3
"""
Integration test for the clinical data analysis pipeline
Tests the complete data flow from ingestion to visualization
"""

import os
import sys
import tempfile
import shutil
from unittest.mock import MagicMock

sys.path.append('modules')
sys.path.append('config')

from data_ingestion import ingest_adlbc_data
from data_preprocessing import preprocess_clinical_data
from statistical_calculations import calculate_statistics
from visualization_generation import generate_box_plots
from pipeline_config import PipelineConfig

def test_pipeline_integration():
    """Test the complete pipeline integration"""
    print("Starting pipeline integration test...")
    
    temp_dir = tempfile.mkdtemp()
    print(f"Using temporary directory: {temp_dir}")
    
    try:
        pipeline_config = PipelineConfig('testing')
        config = pipeline_config.get_all()
        config['output_path'] = temp_dir
        
        print("✓ Configuration initialized")
        
        mock_context = {
            'task_instance': MagicMock()
        }
        
        xcom_data = {}
        def mock_xcom_pull(task_ids, key):
            return xcom_data.get(f"{task_ids}_{key}")
        
        def mock_xcom_push(key, value):
            task_id = mock_context.get('current_task_id', 'test')
            xcom_data[f"{task_id}_{key}"] = value
        
        mock_context['task_instance'].xcom_pull = mock_xcom_pull
        mock_context['task_instance'].xcom_push = mock_xcom_push
        
        print("\n1. Testing data ingestion...")
        mock_context['current_task_id'] = 'data_ingestion'
        
        try:
            ingestion_result = ingest_adlbc_data(config, **mock_context)
            print(f"✓ Data ingestion completed: {ingestion_result}")
            
            if os.path.exists(ingestion_result):
                print("✓ Ingestion output file exists")
            else:
                print("✗ Ingestion output file missing")
                return False
                
        except Exception as e:
            print(f"✗ Data ingestion failed: {e}")
            return False
        
        print("\n2. Testing data preprocessing...")
        mock_context['current_task_id'] = 'data_preprocessing'
        xcom_data['data_ingestion_ingested_data_path'] = ingestion_result
        
        try:
            preprocessing_result = preprocess_clinical_data(config, **mock_context)
            print(f"✓ Data preprocessing completed: {preprocessing_result}")
            
            if os.path.exists(preprocessing_result):
                print("✓ Preprocessing output file exists")
            else:
                print("✗ Preprocessing output file missing")
                return False
                
        except Exception as e:
            print(f"✗ Data preprocessing failed: {e}")
            return False
        
        print("\n3. Testing statistical calculations...")
        mock_context['current_task_id'] = 'statistical_calculations'
        xcom_data['data_preprocessing_preprocessed_data_path'] = preprocessing_result
        
        try:
            stats_result = calculate_statistics(config, **mock_context)
            print(f"✓ Statistical calculations completed: {stats_result}")
            
            if os.path.exists(stats_result):
                print("✓ Statistics output file exists")
            else:
                print("✗ Statistics output file missing")
                return False
                
        except Exception as e:
            print(f"✗ Statistical calculations failed: {e}")
            return False
        
        print("\n4. Testing visualization generation...")
        mock_context['current_task_id'] = 'visualization_generation'
        xcom_data['statistical_calculations_statistics_data_path'] = stats_result
        xcom_data['statistical_calculations_plot_data_path'] = xcom_data.get('statistical_calculations_plot_data_path')
        
        try:
            viz_result = generate_box_plots(config, **mock_context)
            print(f"✓ Visualization generation completed: {viz_result}")
            
            if os.path.exists(viz_result):
                print("✓ Visualization output directory exists")
                
                html_files = [f for f in os.listdir(viz_result) if f.endswith('.html')]
                if html_files:
                    print(f"✓ Generated {len(html_files)} HTML visualization files")
                else:
                    print("✗ No HTML visualization files found")
                    return False
            else:
                print("✗ Visualization output directory missing")
                return False
                
        except Exception as e:
            print(f"✗ Visualization generation failed: {e}")
            return False
        
        print("\n✓ All pipeline tests passed successfully!")
        return True
        
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"Cleaned up temporary directory: {temp_dir}")

if __name__ == "__main__":
    success = test_pipeline_integration()
    sys.exit(0 if success else 1)
