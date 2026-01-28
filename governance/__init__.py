"""
EU AI Act Governance Implementation

Production-ready Python implementation of EU AI Act compliance controls for ML pipelines.

This package provides:
- Data lineage tracking with cryptographic verification (Article 10)
- Compliance deployment gates (Articles 9-15)
- Risk-based data quality validation
- Bias monitoring in production
- Storage backends for PostgreSQL and MongoDB

Example usage:
    from governance import LineageTracker, ComplianceGate, RiskLevel
    
    # Track data lineage
    tracker = LineageTracker(storage_backend)
    tracker.track_extraction("customer_db", query, data)
    
    # Validate compliance before deployment
    gate = ComplianceGate(db, registry)
    can_deploy, report = gate.validate_deployment(model_id, RiskLevel.HIGH)

For full documentation, see:
https://codeandclause.ai/ai-governance-ml-pipeline-implementation/
"""

from .data_lineage import LineageTracker, DataLineage
from .compliance_gate import ComplianceGate, RiskLevel, ComplianceCheck
from .storage_backends import (
    GovernanceStorageBackend,
    PostgresGovernanceStorage,
    MongoGovernanceStorage
)

__version__ = "1.0.0"
__author__ = "Code & Clause"
__license__ = "MIT"

__all__ = [
    # Data lineage
    "LineageTracker",
    "DataLineage",
    
    # Compliance gates
    "ComplianceGate",
    "RiskLevel",
    "ComplianceCheck",
    
    # Storage backends
    "GovernanceStorageBackend",
    "PostgresGovernanceStorage",
    "MongoGovernanceStorage",
]
