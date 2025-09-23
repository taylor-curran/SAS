import py_compile
import yaml
import sys

def test_python_files():
    """Test Python file syntax"""
    files_to_test = [
        'dags/clinical_data_analysis_dag.py',
        'scripts/data_ingestion.py',
        'scripts/data_preprocessing.py',
        'scripts/statistical_analysis.py',
        'scripts/visualization.py',
        'utils/phuse_utilities.py'
    ]
    
    for file_path in files_to_test:
        try:
            py_compile.compile(file_path, doraise=True)
            print(f"✓ {file_path} - syntax OK")
        except py_compile.PyCompileError as e:
            print(f"✗ {file_path} - syntax error: {e}")
            return False
    
    return True

def test_yaml_config():
    """Test YAML configuration file"""
    try:
        with open('config/pipeline_config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        print("✓ config/pipeline_config.yaml - valid YAML")
        return True
    except Exception as e:
        print(f"✗ config/pipeline_config.yaml - error: {e}")
        return False

if __name__ == "__main__":
    print("Testing migration files...")
    
    python_ok = test_python_files()
    yaml_ok = test_yaml_config()
    
    if python_ok and yaml_ok:
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed!")
        sys.exit(1)
