"""
Basic Usage Example - EU AI Act Governance

This example demonstrates the core workflow:
1. Track data lineage
2. Validate compliance before deployment

For production use, adapt the storage backend and model registry
to your infrastructure.
"""

import pandas as pd
from datetime import datetime

# Import governance modules
from governance import LineageTracker, ComplianceGate, RiskLevel
from governance.storage_backends import PostgresGovernanceStorage


def main():
    """Basic usage example"""
    
    print("=" * 60)
    print("EU AI Act Governance - Basic Usage Example")
    print("=" * 60)
    
    # Step 1: Initialize storage backend
    print("\n1. Initializing storage backend...")
    
    # In production, use your actual database credentials
    storage = PostgresGovernanceStorage(
        host="localhost",
        database="ai_governance",
        user="postgres",
        password="your_password"
    )
    
    # Step 2: Track data lineage
    print("\n2. Tracking data lineage...")
    
    tracker = LineageTracker(storage)
    
    # Simulate data extraction
    raw_data = pd.DataFrame({
        'user_id': [1, 2, 3, 4, 5],
        'feature_a': [0.1, 0.2, None, 0.4, 0.5],
        'feature_b': [1, 2, 3, 4, 5],
        'target': [0, 1, 1, 0, 1]
    })
    
    dataset_id = tracker.track_extraction(
        source="customer_db",
        query="SELECT * FROM users WHERE active=true",
        data=raw_data
    )
    
    print(f"   ✓ Dataset tracked: {dataset_id}")
    
    # Track transformation: clean data
    cleaned_data = raw_data.dropna()
    
    tracker.track_transformation(
        step_name="remove_nulls",
        input_data=raw_data,
        output_data=cleaned_data,
        transformation_code="df.dropna()"
    )
    
    print(f"   ✓ Transformation tracked: remove_nulls")
    print(f"   ✓ Lineage chain validated: {tracker.current_lineage.validate_lineage_chain()}")
    
    # Link to model (in production, this would be your actual model ID)
    model_id = "model_v1.0.0"
    tracker.link_to_model(model_id)
    
    print(f"   ✓ Linked to model: {model_id}")
    
    # Step 3: Validate compliance before deployment
    print("\n3. Validating compliance before deployment...")
    
    # Note: In production, you'd have a real model registry
    # For this example, we'll demonstrate the gate check pattern
    
    # Initialize compliance gate
    # (In production, pass your actual model_registry)
    gate = ComplianceGate(
        governance_db=storage,
        model_registry=None,  # Replace with actual registry
        config={
            'bias_threshold': 0.10,
            'min_accuracy_high_risk': 0.85
        }
    )
    
    print(f"   ✓ Compliance gate initialized")
    print(f"   ✓ Configuration: bias_threshold=0.10, min_accuracy=0.85")
    
    # In production, you would run:
    # can_deploy, report = gate.validate_deployment(model_id, RiskLevel.HIGH)
    # 
    # if can_deploy:
    #     deploy_model(model_id, "production")
    # else:
    #     print(f"Deployment blocked: {report['failures']}")
    
    print("\n" + "=" * 60)
    print("✓ Basic workflow complete!")
    print("=" * 60)
    
    print("\nNext steps:")
    print("1. Set up your database (PostgreSQL or MongoDB)")
    print("2. Integrate with your ML training pipeline")
    print("3. Configure compliance gate for your risk level")
    print("4. Add to your CI/CD deployment pipeline")
    
    print("\nFull guide: https://codeandclause.ai/ai-governance-ml-pipeline-implementation/")


if __name__ == "__main__":
    # Note: This example assumes PostgreSQL is running
    # For testing, you may want to use a mock storage backend
    
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nTo run this example:")
        print("1. Set up PostgreSQL database")
        print("2. Update connection credentials in the code")
        print("3. Run: python examples/basic_usage.py")
