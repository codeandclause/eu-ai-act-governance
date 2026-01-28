"""
Basic tests for EU AI Act Governance Implementation

Run with: pytest tests/test_data_lineage.py
"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock

from governance import LineageTracker, DataLineage


class TestDataLineage:
    """Test data lineage tracking functionality"""
    
    def test_stable_hash_consistency(self):
        """Test that stable_hash produces consistent results"""
        mock_storage = Mock()
        tracker = LineageTracker(mock_storage)
        
        # Create test dataframe
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        
        # Hash should be consistent
        hash1 = tracker._stable_hash(df)
        hash2 = tracker._stable_hash(df)
        
        assert hash1 == hash2, "Hashes should be consistent"
        assert len(hash1) == 64, "SHA256 hash should be 64 characters"
    
    def test_lineage_chain_validates_continuity(self):
        """Test that lineage validation catches broken chains"""
        lineage = DataLineage(
            dataset_id="test_123",
            source_systems=["test_db"],
            extraction_timestamp=datetime.now(),
            transformation_pipeline=[
                {
                    'step': 'step1',
                    'input_hash': '',
                    'output_hash': 'abc123',
                    'timestamp': datetime.now()
                },
                {
                    'step': 'step2',
                    'input_hash': 'xyz789',  # Doesn't match previous output
                    'output_hash': 'def456',
                    'timestamp': datetime.now()
                }
            ],
            content_hash='def456'
        )
        
        # Validation should fail due to broken chain
        assert not lineage.validate_lineage_chain(), \
            "Validation should fail for broken chain"
    
    def test_lineage_chain_validates_correct_chain(self):
        """Test that valid lineage chains pass validation"""
        lineage = DataLineage(
            dataset_id="test_123",
            source_systems=["test_db"],
            extraction_timestamp=datetime.now(),
            transformation_pipeline=[
                {
                    'step': 'extraction',
                    'input_hash': '',
                    'output_hash': 'abc123',
                    'timestamp': datetime.now()
                },
                {
                    'step': 'transform1',
                    'input_hash': 'abc123',  # Matches previous output
                    'output_hash': 'def456',
                    'timestamp': datetime.now()
                }
            ],
            content_hash='def456'  # Matches last output
        )
        
        # Validation should pass
        assert lineage.validate_lineage_chain(), \
            "Validation should pass for correct chain"
    
    def test_track_extraction_creates_lineage(self):
        """Test that track_extraction creates valid lineage"""
        mock_storage = Mock()
        tracker = LineageTracker(mock_storage)
        
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        
        dataset_id = tracker.track_extraction(
            source="test_db",
            query="SELECT * FROM test",
            data=df
        )
        
        assert dataset_id is not None, "Should return dataset ID"
        assert tracker.current_lineage is not None, "Should create lineage"
        assert len(tracker.current_lineage.transformation_pipeline) == 1, \
            "Should have extraction step"
        assert mock_storage.insert.called, "Should persist to storage"
    
    def test_track_transformation_requires_extraction(self):
        """Test that track_transformation requires prior extraction"""
        mock_storage = Mock()
        tracker = LineageTracker(mock_storage)
        
        df1 = pd.DataFrame({'a': [1, 2]})
        df2 = pd.DataFrame({'a': [1, 2]})
        
        # Should raise error without extraction first
        with pytest.raises(ValueError, match="track_extraction"):
            tracker.track_transformation(
                "test_transform",
                df1,
                df2,
                "df.copy()"
            )


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
