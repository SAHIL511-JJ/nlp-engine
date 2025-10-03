import sqlalchemy as sa
from sqlalchemy import create_engine, inspect, text
from typing import Dict, List, Any
import logging
import re

logger = logging.getLogger(__name__)

SYNONYM_MAP = {
	'employee': ['employee', 'employees', 'emp', 'staff', 'personnel'],
	' department': ['department', 'dept', 'division', 'team'],
	'salary': ['salary', 'compensation', 'pay', 'annual_salary', 'pay_rate'],
	'name': ['name', 'full_name', 'employee_name'],
	'id': ['id', 'emp_id', 'employee_id', 'person_id'],
}


def normalize_identifier(name: str) -> str:
	return name.lower().strip()


def match_synonym(term: str, candidates: list[str]) -> str | None:
	term_n = normalize_identifier(term)
	for canon, alts in SYNONYM_MAP.items():
		if term_n in (normalize_identifier(a) for a in alts):
			for cand in candidates:
				if normalize_identifier(cand) in (normalize_identifier(a) for a in alts):
					return cand
	return None


class SchemaDiscovery:
    def __init__(self):
        self.engine = None
        self.inspector = None
        self.synonym_mappings = {
            'employee': ['employee', 'employees', 'emp', 'staff', 'personnel', 'worker'],
            'salary': ['salary', 'compensation', 'pay', 'wage', 'income', 'earnings'],
            'department': ['department', 'dept', 'division', 'unit', 'team', 'group'],
            'name': ['name', 'full_name', 'employee_name', 'staff_name'],
            'id': ['id', 'emp_id', 'employee_id', 'staff_id', 'person_id'],
            'hire_date': ['hire_date', 'join_date', 'start_date', 'hired_on'],
            'position': ['position', 'role', 'title', 'job_title', 'designation'],
            'manager': ['manager', 'supervisor', 'reports_to', 'head']
        }

    def analyze_database(self, connection_string: str) -> Dict[str, Any]:
        """Connect to database and automatically discover schema"""
        try:
            self.engine = create_engine(connection_string)
            self.inspector = inspect(self.engine)
            
            tables = self.inspector.get_table_names()
            schema = {
                'tables': [],
                'relationships': [],
                'total_tables': len(tables),
                'total_columns': 0
            }
            
            for table_name in tables:
                table_info = self._analyze_table(table_name)
                schema['tables'].append(table_info)
                schema['total_columns'] += len(table_info['columns'])
            
            schema['relationships'] = self._discover_relationships(schema['tables'])
            
            logger.info(f"Discovered {len(tables)} tables with {schema['total_columns']} columns")
            return schema
            
        except Exception as e:
            logger.error(f"Schema discovery failed: {str(e)}")
            raise

    def _analyze_table(self, table_name: str) -> Dict[str, Any]:
        """Analyze a single table and its columns"""
        columns = []
        column_info = self.inspector.get_columns(table_name)
        
        for col in column_info:
            column_data = {
                'name': col['name'],
                'type': str(col['type']),
                'nullable': col['nullable'],
                'primary_key': False,
                'estimated_purpose': self._estimate_column_purpose(col['name'])
            }
            columns.append(column_data)
        
        # Get primary keys
        primary_keys = self.inspector.get_pk_constraint(table_name)
        if primary_keys and 'constrained_columns' in primary_keys:
            for col_name in primary_keys['constrained_columns']:
                for col in columns:
                    if col['name'] == col_name:
                        col['primary_key'] = True
        
        # Get sample data
        sample_data = self._get_sample_data(table_name, columns)
        
        return {
            'name': table_name,
            'columns': columns,
            'sample_data': sample_data,
            'estimated_purpose': self._estimate_table_purpose(table_name)
        }

    def _estimate_table_purpose(self, table_name: str) -> str:
        """Estimate the purpose of a table based on its name"""
        table_lower = table_name.lower()
        
        if any(term in table_lower for term in ['emp', 'staff', 'personnel', 'worker']):
            return 'employee_data'
        elif any(term in table_lower for term in ['dept', 'division', 'team', 'group']):
            return 'department_data'
        elif any(term in table_lower for term in ['doc', 'file', 'resume', 'review']):
            return 'document_data'
        elif any(term in table_lower for term in ['salary', 'compensation', 'pay']):
            return 'compensation_data'
        else:
            return 'general_data'

    def _estimate_column_purpose(self, column_name: str) -> str:
        """Estimate the purpose of a column based on its name"""
        col_lower = column_name.lower()
        
        if any(term in col_lower for term in ['name', 'full_name', 'first', 'last']):
            return 'employee_name'
        elif any(term in col_lower for term in ['id', 'key', 'code']):
            return 'identifier'
        elif any(term in col_lower for term in ['salary', 'compensation', 'pay', 'wage']):
            return 'compensation'
        elif any(term in col_lower for term in ['date', 'time', 'year']):
            return 'date_time'
        elif any(term in col_lower for term in ['dept', 'division', 'team']):
            return 'department'
        elif any(term in col_lower for term in ['title', 'position', 'role']):
            return 'job_title'
        elif any(term in col_lower for term in ['email', 'phone', 'address']):
            return 'contact_info'
        else:
            return 'general'

    def _get_sample_data(self, table_name: str, columns: List[Dict]) -> List[Dict]:
        """Get sample data from the table"""
        try:
            with self.engine.connect() as conn:
                # Get column names for SELECT
                col_names = [col['name'] for col in columns]
                query = text(f"SELECT {', '.join(col_names)} FROM {table_name} LIMIT 5")
                result = conn.execute(query)
                rows = result.fetchall()
                
                sample_data = []
                for row in rows:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        row_dict[col['name']] = str(row[i]) if row[i] is not None else None
                    sample_data.append(row_dict)
                
                return sample_data
        except Exception as e:
            logger.warning(f"Could not fetch sample data for {table_name}: {str(e)}")
            return []

    def _discover_relationships(self, tables: List[Dict]) -> List[Dict]:
        """Discover relationships between tables"""
        relationships = []
        
        for table in tables:
            try:
                foreign_keys = self.inspector.get_foreign_keys(table['name'])
                for fk in foreign_keys:
                    relationship = {
                        'from_table': table['name'],
                        'from_column': fk['constrained_columns'][0],
                        'to_table': fk['referred_table'],
                        'to_column': fk['referred_columns'][0],
                        'type': 'foreign_key'
                    }
                    relationships.append(relationship)
            except Exception as e:
                logger.warning(f"Could not get foreign keys for {table['name']}: {str(e)}")
        
        # Also try to infer relationships based on column names
        for table in tables:
            for column in table['columns']:
                col_name = column['name'].lower()
                if col_name.endswith('_id') or col_name.endswith('_code'):
                    # Try to find matching table
                    possible_table = col_name[:-3]  # Remove _id suffix
                    for other_table in tables:
                        if other_table['name'].lower() == possible_table:
                            relationship = {
                                'from_table': table['name'],
                                'from_column': column['name'],
                                'to_table': other_table['name'],
                                'to_column': self._find_primary_key(other_table),
                                'type': 'inferred'
                            }
                            relationships.append(relationship)
        
        return relationships

    def _find_primary_key(self, table: Dict) -> str:
        """Find primary key column for a table"""
        for col in table['columns']:
            if col.get('primary_key', False):
                return col['name']
        # Fallback to first column named 'id'
        for col in table['columns']:
            if col['name'].lower() == 'id':
                return col['name']
        # Final fallback to first column
        return table['columns'][0]['name']

    def map_natural_language_to_schema(self, query: str, schema: Dict) -> Dict[str, Any]:
        """Map user's natural language to actual database structure"""
        query_lower = query.lower()
        mappings = {
            'table_mappings': {},
            'column_mappings': {},
            'detected_entities': []
        }
        
        # Map table names
        for table in schema['tables']:
            table_purpose = table['estimated_purpose']
            table_synonyms = self.synonym_mappings.get(table_purpose, [table['name']])
            
            for synonym in table_synonyms:
                if synonym in query_lower:
                    mappings['table_mappings'][synonym] = table['name']
                    mappings['detected_entities'].append({
                        'type': 'table',
                        'natural_language': synonym,
                        'database_term': table['name'],
                        'purpose': table_purpose
                    })
        
        # Map column names
        for table in schema['tables']:
            for column in table['columns']:
                col_purpose = column['estimated_purpose']
                col_synonyms = self.synonym_mappings.get(col_purpose, [column['name']])
                
                for synonym in col_synonyms:
                    if synonym in query_lower:
                        mappings['column_mappings'][synonym] = {
                            'table': table['name'],
                            'column': column['name']
                        }
                        mappings['detected_entities'].append({
                            'type': 'column',
                            'natural_language': synonym,
                            'database_term': column['name'],
                            'table': table['name'],
                            'purpose': col_purpose
                        })
        
        return mappings
