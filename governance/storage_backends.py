"""
Storage Backend Implementations for Governance Systems

Provides concrete implementations of storage interfaces used by
LineageTracker and ComplianceGate.

Usage:
    # PostgreSQL backend
    from storage_backends import PostgresGovernanceStorage
    
    storage = PostgresGovernanceStorage(
        host="localhost",
        database="ai_governance",
        user="governance_user",
        password="secure_password"
    )
    
    tracker = LineageTracker(storage)
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import json
from datetime import datetime


class GovernanceStorageBackend(ABC):
    """
    Abstract base class for governance storage.
    Implement this interface for your database system.
    """
    
    @abstractmethod
    def insert(self, table: str, data: Dict) -> bool:
        """Insert a record"""
        pass
    
    @abstractmethod
    def update(self, table: str, condition: Dict, data: Dict) -> bool:
        """Update records matching condition"""
        pass
    
    @abstractmethod
    def query(self, sql: str, params: tuple) -> List[Dict]:
        """Execute query and return results"""
        pass
    
    @abstractmethod
    def bulk_insert(self, table: str, records: List[Dict]) -> bool:
        """Insert multiple records efficiently"""
        pass


class PostgresGovernanceStorage(GovernanceStorageBackend):
    """
    PostgreSQL implementation of governance storage.
    
    Features:
    - Append-only audit trail
    - JSONB support for flexible metadata
    - Connection pooling ready
    - Transaction support
    
    Example:
        storage = PostgresGovernanceStorage(
            host="localhost",
            database="ai_governance",
            user="governance_user",
            password="secure_password"
        )
        
        # Use with LineageTracker
        tracker = LineageTracker(storage)
    """
    
    def __init__(self, host: str, database: str, user: str, 
                 password: str, port: int = 5432):
        """
        Initialize PostgreSQL connection.
        
        Args:
            host: Database host
            database: Database name
            user: Database user
            password: User password
            port: Database port (default 5432)
        """
        self.connection_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self._test_connection()
    
    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.connection_params)
    
    def _test_connection(self):
        """Test database connectivity"""
        try:
            conn = self._get_connection()
            conn.close()
        except Exception as e:
            raise ConnectionError(f"Cannot connect to PostgreSQL: {str(e)}")
    
    def insert(self, table: str, data: Dict) -> bool:
        """
        Insert a record into table.
        
        Args:
            table: Table name
            data: Dictionary of column: value pairs
            
        Returns:
            True if successful
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Convert datetime objects to ISO format
            data = self._serialize_dates(data)
            
            # Build INSERT statement
            columns = list(data.keys())
            values = [data[col] for col in columns]
            
            # Handle JSONB columns
            values = [Json(v) if isinstance(v, (dict, list)) else v for v in values]
            
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join(columns)
            
            sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
            
            cursor.execute(sql, values)
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
        except Exception as e:
            print(f"Error inserting into {table}: {str(e)}")
            return False
    
    def update(self, table: str, condition: Dict, data: Dict) -> bool:
        """
        Update records matching condition.
        
        Args:
            table: Table name
            condition: WHERE clause conditions (dict)
            data: Data to update
            
        Returns:
            True if successful
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            data = self._serialize_dates(data)
            
            # Build SET clause
            set_clauses = []
            set_values = []
            for col, val in data.items():
                set_clauses.append(f"{col} = %s")
                set_values.append(Json(val) if isinstance(val, (dict, list)) else val)
            
            # Build WHERE clause
            where_clauses = []
            where_values = []
            for col, val in condition.items():
                where_clauses.append(f"{col} = %s")
                where_values.append(val)
            
            sql = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"
            
            cursor.execute(sql, set_values + where_values)
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
        except Exception as e:
            print(f"Error updating {table}: {str(e)}")
            return False
    
    def query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """
        Execute query and return results as list of dicts.
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            List of dictionaries (column: value)
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            # Convert RealDictRow to regular dict
            return [dict(row) for row in results]
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return []
    
    def bulk_insert(self, table: str, records: List[Dict]) -> bool:
        """
        Efficiently insert multiple records.
        
        Args:
            table: Table name
            records: List of dictionaries to insert
            
        Returns:
            True if successful
        """
        if not records:
            return True
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Serialize all records
            records = [self._serialize_dates(r) for r in records]
            
            # Use first record to determine columns
            columns = list(records[0].keys())
            column_names = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            
            sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
            
            # Prepare all value tuples
            value_tuples = []
            for record in records:
                values = [record[col] for col in columns]
                values = [Json(v) if isinstance(v, (dict, list)) else v for v in values]
                value_tuples.append(tuple(values))
            
            cursor.executemany(sql, value_tuples)
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
        except Exception as e:
            print(f"Error bulk inserting into {table}: {str(e)}")
            return False
    
    def _serialize_dates(self, data: Dict) -> Dict:
        """Convert datetime objects to ISO format strings"""
        result = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, dict):
                result[key] = self._serialize_dates(value)
            elif isinstance(value, list):
                result[key] = [
                    self._serialize_dates(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        return result


class MongoGovernanceStorage(GovernanceStorageBackend):
    """
    MongoDB implementation (alternative to PostgreSQL).
    
    Advantages:
    - Flexible schema (good for evolving metadata requirements)
    - Native JSON/BSON support
    - Good for high-write workloads (audit logs)
    
    Example:
        from pymongo import MongoClient
        
        storage = MongoGovernanceStorage(
            connection_string="mongodb://localhost:27017/",
            database="ai_governance"
        )
    """
    
    def __init__(self, connection_string: str, database: str):
        """
        Initialize MongoDB connection.
        
        Args:
            connection_string: MongoDB connection URI
            database: Database name
        """
        try:
            from pymongo import MongoClient
            self.client = MongoClient(connection_string)
            self.db = self.client[database]
        except ImportError:
            raise ImportError("pymongo not installed. Run: pip install pymongo")
        except Exception as e:
            raise ConnectionError(f"Cannot connect to MongoDB: {str(e)}")
    
    def insert(self, table: str, data: Dict) -> bool:
        """Insert document into collection"""
        try:
            collection = self.db[table]
            collection.insert_one(data)
            return True
        except Exception as e:
            print(f"Error inserting into {table}: {str(e)}")
            return False
    
    def update(self, table: str, condition: Dict, data: Dict) -> bool:
        """Update documents matching condition"""
        try:
            collection = self.db[table]
            collection.update_many(condition, {'$set': data})
            return True
        except Exception as e:
            print(f"Error updating {table}: {str(e)}")
            return False
    
    def query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """
        Note: MongoDB doesn't use SQL. This method expects:
        sql = collection_name
        params = (query_dict, projection_dict)
        """
        try:
            collection_name = sql
            query = params[0] if len(params) > 0 else {}
            projection = params[1] if len(params) > 1 else None
            
            collection = self.db[collection_name]
            cursor = collection.find(query, projection)
            
            results = list(cursor)
            
            # Remove MongoDB's _id field if present
            for doc in results:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            return results
        except Exception as e:
            print(f"Error querying {sql}: {str(e)}")
            return []
    
    def bulk_insert(self, table: str, records: List[Dict]) -> bool:
        """Bulk insert documents"""
        try:
            collection = self.db[table]
            collection.insert_many(records)
            return True
        except Exception as e:
            print(f"Error bulk inserting into {table}: {str(e)}")
            return False


# Example usage
if __name__ == "__main__":
    # PostgreSQL example
    storage = PostgresGovernanceStorage(
        host="localhost",
        database="ai_governance",
        user="governance_user",
        password="secure_password"
    )
    
    # Test insert
    test_data = {
        'model_id': 'test_model_123',
        'risk_level': 'high',
        'created_at': datetime.now(),
        'metadata': {'key': 'value'}
    }
    
    success = storage.insert('test_table', test_data)
    print(f"Insert successful: {success}")
    
    # Test query
    results = storage.query(
        "SELECT * FROM test_table WHERE model_id = %s",
        ('test_model_123',)
    )
    print(f"Query results: {results}")
