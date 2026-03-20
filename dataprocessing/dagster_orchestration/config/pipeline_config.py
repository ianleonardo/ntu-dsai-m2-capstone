"""
Centralized pipeline configuration for SEC data processing.

This module provides a single source of truth for pipeline parameters
that can be shared across all assets and jobs.
"""

from typing import Optional, List
from pydantic import BaseModel, Field
import os
from datetime import datetime


class SECPipelineConfig(BaseModel):
    """Centralized SEC pipeline configuration."""
    
    # Core parameters (single source of truth)
    year: int = Field(
        default_factory=lambda: int(os.getenv("SEC_YEAR", str(datetime.now().year))),
        description="Year for SEC data processing (controls all pipeline assets)"
    )
    
    dataset: str = Field(
        default=os.getenv("BIGQUERY_DATASET", "insider_transactions"),
        description="BigQuery dataset name (controls all pipeline assets)"
    )
    
    quarters: Optional[List[str]] = Field(
        default=None,
        description="List of quarters to process (e.g., ['q1', 'q2'])"
    )
    
    # BigQuery configuration
    project_id: str = Field(
        default=os.getenv("GOOGLE_PROJECT_ID", "ntu-dsai-488112"),
        description="Google Cloud project ID"
    )
    
    # Processing parameters
    batch_size: int = Field(
        default=int(os.getenv("SEC_BATCH_SIZE", "1000")),
        description="Batch size for BigQuery inserts"
    )
    
    dry_run: bool = Field(
        default=False,
        description="Run in dry-run mode (no data insertion)"
    )
    
    class Config:
        extra = "forbid"
    
    def get_quarters(self) -> List[str]:
        """Get resolved quarters list."""
        if self.quarters is not None:
            return self.quarters
        
        # Fall back to environment variable or all quarters
        quarter_env = os.getenv("SEC_LOAD_QUARTER")
        if quarter_env:
            return [quarter_env.strip().lower()]
        
        return ["q1", "q2", "q3", "q4"]
    
    def validate_quarters(self) -> List[str]:
        """Validate and return quarters."""
        quarters = self.get_quarters()
        valid_quarters = {"q1", "q2", "q3", "q4"}
        
        for q in quarters:
            if q not in valid_quarters:
                raise ValueError(f"Invalid quarter: {q}. Must be one of {sorted(valid_quarters)}")
        
        return quarters
    
    def get_quarters_description(self) -> str:
        """Get human-readable quarters description."""
        quarters = self.get_quarters()
        if len(quarters) == 4:
            return "all quarters"
        else:
            return ", ".join(quarters)


# Global configuration instance
pipeline_config = SECPipelineConfig()


def get_pipeline_config(**overrides) -> SECPipelineConfig:
    """
    Get pipeline configuration with optional overrides.
    
    Args:
        **overrides: Configuration overrides (e.g., year=2024, quarters=['q1'])
    
    Returns:
        SECPipelineConfig: Configuration instance with applied overrides
    """
    config_dict = pipeline_config.dict()
    config_dict.update(overrides);
    return SECPipelineConfig(**config_dict)
