"""
Pipeline Configuration
Centralized configuration management for the clinical data analysis pipeline

This replaces SAS macro variables and provides a single source of truth
for all pipeline parameters and settings.
"""

import os
from typing import Dict, List, Any

class PipelineConfig:
    """
    Configuration class for clinical data analysis pipeline
    
    Replaces SAS macro variables from WPCT-F.07.01.sas lines 163-179:
    %let m_var = ALB;
    %let t_var = AVISITN; 
    %let ref_lines = UNIFORM;
    etc.
    """
    
    def __init__(self, environment: str = 'development'):
        self.environment = environment
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration based on environment"""
        
        base_config = {
            'dataset': 'ADLBC',
            'data_path': '/home/ubuntu/repos/SAS/data/adam',
            
            'parameter': 'ALB',  # %let m_var = ALB;
            'parameter_label': 'Albumin (g/L)',
            
            'treatment_var': 'trtp_short',  # %let t_var = trtp_short;
            'treatment_num_var': 'trtpn',   # %let tn_var = trtpn;
            'measurement_var': 'aval',      # %let m_var = aval;
            'low_limit_var': 'a1lo',        # %let lo_var = a1lo;
            'high_limit_var': 'a1hi',       # %let hi_var = a1hi;
            
            'population_flag': 'saffl',     # %let p_fl = saffl;
            'analysis_flag': 'anl01fl',     # %let a_fl = anl01fl;
            
            'visit_filter': [0, 2, 4, 6],  # and AVISITN in (0 2 4 6)
            'parameter_filter': ['ALB'],    # paramcd in (&param)
            
            'ref_lines': 'UNIFORM',         # %let ref_lines = UNIFORM;
            'max_boxes_per_page': 20,       # %let max_boxes_per_page = 20;
            
            'output_path': '/tmp/clinical_outputs',
            'output_format': 'html',  # Changed from PDF to HTML for web compatibility
            
            'spark_config': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.skewJoin.enabled': 'true',
            },
            
            'treatment_formats': {
                0: 'P',        # Placebo
                54: 'X-high',  # High dose
                81: 'X-low',   # Low dose
            }
        }
        
        if self.environment == 'production':
            base_config.update({
                'data_path': 's3://clinical-data-bucket/adam',
                'output_path': 's3://clinical-outputs-bucket',
                'spark_config': {
                    **base_config['spark_config'],
                    'spark.sql.adaptive.advisoryPartitionSizeInBytes': '128MB',
                }
            })
        elif self.environment == 'testing':
            base_config.update({
                'data_path': '/tmp/test_data/adam',
                'output_path': '/tmp/test_outputs',
            })
        
        return base_config
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self._config.get(key, default)
    
    def get_all(self) -> Dict[str, Any]:
        """Get all configuration values"""
        return self._config.copy()
    
    def update(self, updates: Dict[str, Any]) -> None:
        """Update configuration values"""
        self._config.update(updates)
    
    @property
    def required_columns(self) -> List[str]:
        """Get list of required columns for analysis"""
        return [
            'STUDYID', 'USUBJID',
            self.get('population_flag'),
            self.get('analysis_flag'),
            self.get('treatment_var'),
            self.get('treatment_num_var'),
            'PARAM', 'PARAMCD',
            self.get('measurement_var'),
            self.get('low_limit_var'),
            self.get('high_limit_var'),
            'AVISIT', 'AVISITN',
            'ATPT', 'ATPTN'
        ]
    
    @property
    def analysis_variables(self) -> List[str]:
        """Get analysis variables (replaces SAS &ana_variables)"""
        return self.required_columns
    
    def get_treatment_format(self, treatment_num: int) -> str:
        """Get formatted treatment label"""
        formats = self.get('treatment_formats', {})
        return formats.get(treatment_num, 'UNEXPECTED')
    
    def validate_config(self) -> List[str]:
        """Validate configuration and return any errors"""
        errors = []
        
        required_keys = [
            'dataset', 'parameter', 'treatment_var', 'treatment_num_var',
            'measurement_var', 'population_flag', 'analysis_flag'
        ]
        
        for key in required_keys:
            if not self.get(key):
                errors.append(f"Missing required configuration: {key}")
        
        valid_ref_lines = ['NONE', 'UNIFORM', 'NARROW', 'ALL']
        if self.get('ref_lines') not in valid_ref_lines:
            errors.append(f"Invalid ref_lines value. Must be one of: {valid_ref_lines}")
        
        return errors

config = PipelineConfig()

def get_config(environment: str = 'development') -> PipelineConfig:
    """Get configuration for specified environment"""
    return PipelineConfig(environment)

AIRFLOW_VARIABLE_MAPPINGS = {
    'clinical_dataset': 'dataset',
    'analysis_parameter': 'parameter', 
    'treatment_variable': 'treatment_var',
    'treatment_num_variable': 'treatment_num_var',
    'measurement_variable': 'measurement_var',
    'low_limit_variable': 'low_limit_var',
    'high_limit_variable': 'high_limit_var',
    'population_flag': 'population_flag',
    'analysis_flag': 'analysis_flag',
    'reference_lines': 'ref_lines',
    'max_boxes_per_page': 'max_boxes_per_page',
    'output_path': 'output_path',
}
