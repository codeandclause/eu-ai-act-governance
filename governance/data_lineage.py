"""
Data Lineage Tracking for EU AI Act Compliance
Implements Article 10 requirements for data governance and provenance.

Usage:
    tracker = LineageTracker(database_connection)
    tracker.track_extraction("customer_db", "SELECT * FROM customers", raw_data)
    tracker.track_transformation("clean", raw_data, cleaned_data, "df.dropna()")
    tracker.link_to_model(model_id)
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
import hashlib
import json


@dataclass
class DataLineage:
    """
    Complete provenance tracking for EU AI Act Article 10 compliance.
    
    Captures:
    - Source systems and extraction queries
    - Complete transformation pipeline
    - Data quality metrics at each stage
    - Cryptographic hashes for integrity verification
    """
    
    dataset_id: str
    source_systems: List[str]
    extraction_timestamp: datetime
    transformation_pipeline: List[Dict] = field(default_factory=list)
    data_quality_metrics: Dict = field(default_factory=dict)
    content_hash: str = ""
    
    def compute_content_hash(self, data) -> str:
        """
        Cryptographic hash to prove data hasn't been tampered with.
        Critical for audit integrity - you can prove the exact data
        that trained a model.
        
        Args:
            data: Pandas DataFrame or similar structure
            
        Returns:
            SHA256 hash of data content
        """
        data_string = json.dumps(data.to_dict(), sort_keys=True)
        return hashlib.sha256(data_string.encode()).hexdigest()
    
    def validate_lineage_chain(self) -> bool:
        """
        Verify unbroken chain from source to model.
        
        Checks:
        - No gaps in transformation sequence
        - Timestamps are monotonically increasing
        - Required fields present in each step
        - Hash continuity (step N output_hash == step N+1 input_hash)
        
        Returns:
            True if chain is valid, False otherwise
        """
        if not self.transformation_pipeline:
            return False
        
        # Check timestamp ordering
        prev_timestamp = self.extraction_timestamp
        for step in self.transformation_pipeline:
            step_time = step.get('timestamp')
            if step_time and step_time < prev_timestamp:
                return False  # Time went backwards - invalid chain
            prev_timestamp = step_time
        
        # Check for required fields
        required_fields = ['step', 'input_hash', 'output_hash', 'timestamp']
        for step in self.transformation_pipeline:
            if not all(field in step for field in required_fields):
                return False
        
        # CRITICAL: Verify hash continuity
        # Each step's output_hash must match the next step's input_hash
        for i in range(1, len(self.transformation_pipeline)):
            prev_output = self.transformation_pipeline[i-1]['output_hash']
            current_input = self.transformation_pipeline[i]['input_hash']
            if prev_output != current_input:
                return False  # Broken chain - data changed without logging
        
        # Verify final content_hash matches last transformation's output_hash
        if len(self.transformation_pipeline) > 0:
            last_output = self.transformation_pipeline[-1]['output_hash']
            if self.content_hash != last_output:
                return False  # Final hash mismatch
        
        return True


class LineageTracker:
    """
    Integrate with your data pipeline to automatically track lineage.
    Works with Pandas, Spark, or any data framework.
    
    Example:
        tracker = LineageTracker(db_connection)
        
        # Track extraction
        df = database.query("SELECT * FROM users")
        dataset_id = tracker.track_extraction("user_db", query, df)
        
        # Track transformations
        cleaned = clean_data(df)
        tracker.track_transformation("clean", df, cleaned, "remove_nulls()")
        
        # Link to model
        model = train(cleaned)
        tracker.link_to_model(model.id)
    """
    
    def __init__(self, storage_backend):
        """
        Args:
            storage_backend: Database connection supporting append-only writes
                           (Postgres, MongoDB, etc.)
        """
        self.storage = storage_backend
        self.current_lineage: Optional[DataLineage] = None
    
    def _stable_hash(self, data) -> str:
        """
        Compute deterministic hash for audit-proof consistency.
        
        Uses JSON serialization with sorted keys to ensure:
        - Same data always produces same hash
        - Platform/pandas version independent
        - Cryptographically verifiable
        
        Args:
            data: Pandas DataFrame or dict-like structure
            
        Returns:
            SHA256 hash string
        """
        if hasattr(data, 'to_dict'):
            # Pandas DataFrame or Series
            data_str = json.dumps(data.to_dict(orient='records'), sort_keys=True)
        elif isinstance(data, dict):
            data_str = json.dumps(data, sort_keys=True)
        else:
            # Fallback for other types
            data_str = json.dumps(str(data), sort_keys=True)
        
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def track_extraction(self, source: str, query: str, data) -> str:
        """
        Track data extraction from source systems.
        
        Args:
            source: Source system identifier (e.g., "customer_db")
            query: Extraction query or method
            data: Extracted data (Pandas DataFrame)
            
        Returns:
            dataset_id: Unique identifier for this dataset
        """
        dataset_id = self._generate_id()
        
        self.current_lineage = DataLineage(
            dataset_id=dataset_id,
            source_systems=[source],
            extraction_timestamp=datetime.now(),
            transformation_pipeline=[{
                'step': 'extraction',
                'source': source,
                'query': query,
                'row_count': len(data),
                'column_count': len(data.columns),
                'schema': {col: str(dtype) for col, dtype in data.dtypes.items()},
                'timestamp': datetime.now(),
                'input_hash': '',  # No input for extraction
                'output_hash': self._stable_hash(data)
            }],
            data_quality_metrics=self._compute_quality_metrics(data),
            content_hash=""
        )
        
        self.current_lineage.content_hash = self._stable_hash(data)
        
        # Persist to database
        self.storage.insert('data_lineage', self.current_lineage.__dict__)
        
        return dataset_id
    
    def track_transformation(self, step_name: str, input_data, output_data, 
                           transformation_code: str):
        """
        Track data transformation step.
        
        Args:
            step_name: Name of transformation (e.g., "remove_nulls")
            input_data: Data before transformation
            output_data: Data after transformation
            transformation_code: Code that performed transformation
        """
        if not self.current_lineage:
            raise ValueError("Must call track_extraction() before track_transformation()")
        
        # Use deterministic hashing for audit-proof consistency
        input_hash = self._stable_hash(input_data)
        output_hash = self._stable_hash(output_data)
        
        transformation_step = {
            'step': step_name,
            'input_hash': input_hash,
            'output_hash': output_hash,
            'transformation_code': transformation_code,
            'timestamp': datetime.now(),
            'row_count_before': len(input_data),
            'row_count_after': len(output_data),
            'rows_removed': len(input_data) - len(output_data),
            'columns_added': list(set(output_data.columns) - set(input_data.columns)),
            'columns_removed': list(set(input_data.columns) - set(output_data.columns))
        }
        
        self.current_lineage.transformation_pipeline.append(transformation_step)
        self.current_lineage.content_hash = self._stable_hash(output_data)
        
        # Update in database
        self.storage.update('data_lineage', 
                          {'dataset_id': self.current_lineage.dataset_id},
                          {'transformation_pipeline': self.current_lineage.transformation_pipeline,
                           'content_hash': self.current_lineage.content_hash})
    
    def link_to_model(self, model_id: str) -> Dict:
        """
        Create immutable link between dataset and model.
        
        Args:
            model_id: Unique identifier for trained model
            
        Returns:
            Link record
        """
        if not self.current_lineage:
            raise ValueError("No active lineage to link")
        
        link_record = {
            'dataset_id': self.current_lineage.dataset_id,
            'model_id': model_id,
            'linked_at': datetime.now(),
            'dataset_hash': self.current_lineage.content_hash
        }
        
        self.storage.insert('model_data_lineage', link_record)
        return link_record
    
    def _generate_id(self) -> str:
        """Generate unique dataset ID"""
        timestamp = datetime.now().isoformat()
        random_suffix = hashlib.sha256(timestamp.encode()).hexdigest()[:8]
        return f"dataset_{timestamp}_{random_suffix}"
    
    def _compute_quality_metrics(self, data) -> Dict:
        """
        Compute data quality metrics required for EU AI Act Article 10.
        Includes completeness, duplicates, and basic representativeness.
        Adapt to your domain and protected attributes.
        """
        metrics = {
            'completeness': 1.0 - (data.isnull().sum().sum() / (len(data) * len(data.columns))),
            'row_count': len(data),
            'column_count': len(data.columns),
            'duplicate_rows': data.duplicated().sum(),
            'computed_at': datetime.now().isoformat()
        }
        
        # Add representativeness checks (Article 10 - bias mitigation)
        # Example: Check class balance for classification targets
        # Adapt this to your domain's protected attributes
        if 'target' in data.columns:
            class_distribution = data['target'].value_counts(normalize=True).to_dict()
            metrics['class_balance'] = class_distribution
            
            # Flag if any class is severely underrepresented (<5%)
            min_class_ratio = min(class_distribution.values()) if class_distribution else 1.0
            metrics['representativeness_flag'] = 'pass' if min_class_ratio >= 0.05 else 'warning'
        
        return metrics
