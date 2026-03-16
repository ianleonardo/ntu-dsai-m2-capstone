#!/usr/bin/env python3
"""
Test script to verify Dagster assets are properly configured.
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_imports():
    """Test that all Dagster assets can be imported."""
    try:
        from dataprocessing.dagster_orchestration.assets.sec_download import sec_raw_data, sec_gcs_data
        print("✅ SEC download assets imported successfully")
        
        from dataprocessing.dagster_orchestration.assets.meltano_integration import (
            meltano_staging_data,
            bigquery_sec_data,
            sec_pipeline_summary,
        )
        print("✅ Meltano integration assets imported successfully")
        
        from dataprocessing.dagster_orchestration.jobs.sec_pipeline import (
            sec_pipeline_job,
            sec_download_job,
            sec_bigquery_load_job,
        )
        print("✅ Pipeline jobs imported successfully")
        
        from dataprocessing.dagster_orchestration.schedules.sec_schedules import (
            quarterly_sec_schedule,
            monthly_validation_schedule,
        )
        print("✅ Schedules imported successfully")
        
        from dataprocessing.dagster_orchestration.repository import sec_data_repository
        print("✅ Repository imported successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

def test_asset_configuration():
    """Test asset configuration."""
    try:
        from dataprocessing.dagster_orchestration.assets.sec_download import SecDownloadConfig, SecGcsConfig
        
        # Test configuration classes
        config1 = SecDownloadConfig(year=2023, quarters=["q1"])
        print(f"✅ SecDownloadConfig created: year={config1.year}, quarters={config1.quarters}")
        
        config2 = SecGcsConfig(bucket_name="test-bucket", keep_local=True)
        print(f"✅ SecGcsConfig created: bucket={config2.bucket_name}, keep_local={config2.keep_local}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def main():
    """Run all tests."""
    print("Testing Dagster Asset Configuration...")
    print("=" * 50)
    
    # Test imports
    print("\n1. Testing imports:")
    imports_ok = test_imports()
    
    # Test configuration
    print("\n2. Testing configuration:")
    config_ok = test_asset_configuration()
    
    # Summary
    print("\n" + "=" * 50)
    if imports_ok and config_ok:
        print("🎉 All tests passed! Dagster integration is ready.")
        print("\nNext steps:")
        print("1. Start Dagster web server: uv run --with dagster dagster dev --port 3001")
        print("2. Open browser: http://127.0.0.1:3001")
        print("3. Run pipeline jobs with proper configuration")
    else:
        print("❌ Some tests failed. Check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
