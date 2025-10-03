import sqlparse
from sqlalchemy import create_engine, text
from typing import Dict, List, Any, Optional
import logging
import time
import hashlib
import json
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class QueryCache:
    def __init__(self, ttl_seconds: int = 300, max_size: int = 1000):
        self.cache = {}
        self.ttl = timedelta(seconds=ttl_seconds)
        self.max_size = max_size

    def get(self, key: str) -> Optional[Dict]:
        if key in self.cache:
            entry = self.cache[key]
            if datetime.now() - entry['timestamp'] < self.ttl:
                return entry['data']
            else:
                del self.cache[key]
        return None

    def set(self, key: str, data: Dict):
        if len(self.cache) >= self.max_size:
            # Remove oldest entry
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k]['timestamp'])
            del self.cache[oldest_key]
        
        self.cache[key] = {
            'data': data,
            'timestamp': datetime.now()
        }

    def clear(self):
        self.cache.clear()

class QueryEngine:
    def __init__(self, connection_string: str, schema: Dict):
        self.engine = create_engine(connection_string)
        self.schema = schema
        self.cache = QueryCache()
        self.query_history = []

    def process_query(self, user_query: str, use_cache: bool = True) -> Dict[str, Any]:
        """Process natural language query with caching and optimization"""
        start_time = time.time()
        
        # Generate cache key
        cache_key = self._generate_cache_key(user_query)
        
        # Check cache
        if use_cache:
            cached_result = self.cache.get(cache_key)
            if cached_result:
                logger.info(f"Cache hit for query: {user_query}")
                cached_result['response_time'] = time.time() - start_time
                return cached_result
        
        # Classify query type
        query_type = self._classify_query_type(user_query)
        
        try:
            if query_type == 'sql':
                results = self._process_sql_query(user_query)
            elif query_type == 'document':
                results = self._process_document_query(user_query)
            else:  # hybrid
                sql_results = self._process_sql_query(user_query)
                doc_results = self._process_document_query(user_query)
                results = {
                    'sql_results': sql_results.get('data', []),
                    'document_results': doc_results.get('data', []),
                    'combined_count': len(sql_results.get('data', [])) + len(doc_results.get('data', []))
                }
            
            response_data = {
                'results': results,
                'query_type': query_type,
                'response_time': time.time() - start_time,
                'cache_hit': False,
                'sources': self._extract_sources(query_type, results),
                'generated_sql': results.get('generated_sql') if query_type in ['sql', 'hybrid'] else None
            }
            
            # Cache the result
            if use_cache:
                self.cache.set(cache_key, response_data)
            
            # Add to history
            self.query_history.append({
                'query': user_query,
                'timestamp': datetime.now(),
                'response_time': response_data['response_time'],
                'type': query_type
            })
            
            # Keep only last 100 queries in history
            if len(self.query_history) > 100:
                self.query_history = self.query_history[-100:]
            
            return response_data
            
        except Exception as e:
            logger.error(f"Query processing failed: {str(e)}")
            return {
                'results': {'error': str(e)},
                'query_type': query_type,
                'response_time': time.time() - start_time,
                'cache_hit': False,
                'sources': [],
                'error': True
            }

    def _classify_query_type(self, query: str) -> str:
        """Classify query as SQL, document, or hybrid"""
        query_lower = query.lower()
        
        sql_indicators = ['count', 'average', 'sum', 'max', 'min', 'group by', 'order by', 
                         'salary', 'department', 'hire', 'joined', 'report']
        doc_indicators = ['resume', 'cv', 'document', 'file', 'review', 'skill', 'experience',
                         'python', 'java', 'certification', 'education']
        
        has_sql = any(indicator in query_lower for indicator in sql_indicators)
        has_doc = any(indicator in query_lower for indicator in doc_indicators)
        
        if has_sql and has_doc:
            return 'hybrid'
        elif has_sql:
            return 'sql'
        else:
            return 'document'

    def _process_sql_query(self, query: str) -> Dict[str, Any]:
        """Process SQL-oriented queries"""
        # Simple NLP to SQL conversion (in a real system, you'd use an LLM here)
        generated_sql = self._nlp_to_sql(query)
        
        try:
            # Validate and optimize SQL
            optimized_sql = self.optimize_sql_query(generated_sql)
            
            # Execute query
            with self.engine.connect() as conn:
                result = conn.execute(text(optimized_sql))
                columns = result.keys()
                data = [dict(zip(columns, row)) for row in result.fetchall()]
                
                return {
                    'data': data,
                    'count': len(data),
                    'generated_sql': optimized_sql
                }
                
        except Exception as e:
            logger.error(f"SQL execution failed: {str(e)}")
            return {
                'data': [],
                'error': str(e),
                'generated_sql': generated_sql
            }

    def _nlp_to_sql(self, query: str) -> str:
        """Convert natural language to SQL (simplified version)"""
        query_lower = query.lower()
        
        # Simple rule-based NLP to SQL conversion
        if 'how many' in query_lower and 'employee' in query_lower:
            employee_table = self._find_employee_table()
            return f"SELECT COUNT(*) as employee_count FROM {employee_table}"
        
        elif 'average salary' in query_lower and 'department' in query_lower:
            employee_table = self._find_employee_table()
            department_table = self._find_department_table()
            salary_column = self._find_salary_column(employee_table)
            dept_column = self._find_department_column(employee_table)
            
            return f"""
            SELECT d.dept_name, AVG(e.{salary_column}) as average_salary 
            FROM {employee_table} e 
            JOIN {department_table} d ON e.{dept_column} = d.dept_id 
            GROUP BY d.dept_name
            """
        
        elif 'employees hired this year' in query_lower:
            employee_table = self._find_employee_table()
            date_column = self._find_date_column(employee_table, ['hire', 'join', 'start'])
            
            return f"""
            SELECT * FROM {employee_table} 
            WHERE EXTRACT(YEAR FROM {date_column}) = EXTRACT(YEAR FROM CURRENT_DATE)
            """
        
        else:
            # Default: select from employee table with limit
            employee_table = self._find_employee_table()
            return f"SELECT * FROM {employee_table} LIMIT 10"

    def _find_employee_table(self) -> str:
        for table in self.schema['tables']:
            if table['estimated_purpose'] == 'employee_data':
                return table['name']
        return self.schema['tables'][0]['name']  # Fallback

    def _find_department_table(self) -> str:
        for table in self.schema['tables']:
            if table['estimated_purpose'] == 'department_data':
                return table['name']
        # If no department table found, we might need to handle this differently
        raise Exception("Department table not found")

    def _find_salary_column(self, table_name: str) -> str:
        table = next(t for t in self.schema['tables'] if t['name'] == table_name)
        for col in table['columns']:
            if col['estimated_purpose'] == 'compensation':
                return col['name']
        # Fallback to common salary column names
        for col in table['columns']:
            if 'salary' in col['name'].lower() or 'comp' in col['name'].lower():
                return col['name']
        return table['columns'][0]['name']  # Final fallback

    def _find_department_column(self, table_name: str) -> str:
        table = next(t for t in self.schema['tables'] if t['name'] == table_name)
        for col in table['columns']:
            if col['estimated_purpose'] == 'department':
                return col['name']
        for col in table['columns']:
            if 'dept' in col['name'].lower():
                return col['name']
        return table['columns'][0]['name']

    def _find_date_column(self, table_name: str, keywords: List[str]) -> str:
        table = next(t for t in self.schema['tables'] if t['name'] == table_name)
        for col in table['columns']:
            if col['estimated_purpose'] == 'date_time':
                col_lower = col['name'].lower()
                if any(keyword in col_lower for keyword in keywords):
                    return col['name']
        for col in table['columns']:
            if col['estimated_purpose'] == 'date_time':
                return col['name']
        return table['columns'][0]['name']

    def optimize_sql_query(self, sql: str) -> str:
        """Optimize generated SQL query"""
        # Parse and format SQL
        parsed = sqlparse.parse(sql)
        if parsed:
            formatted_sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
            
            # Add LIMIT if not present and no aggregate functions
            if 'LIMIT' not in formatted_sql.upper() and not any(
                kw in formatted_sql.upper() for kw in ['COUNT', 'AVG', 'SUM', 'MAX', 'MIN', 'GROUP']
            ):
                formatted_sql += " LIMIT 100"
            
            return formatted_sql
        return sql

    def _process_document_query(self, query: str) -> Dict[str, Any]:
        """Process document-oriented queries"""
        # Simplified document search (in real implementation, use vector search)
        # This would integrate with the document processor
        return {
            'data': [
                {
                    'title': 'Sample_Resume_1.pdf',
                    'content': f"Relevant content matching: {query}",
                    'relevance': 0.95,
                    'type': 'resume'
                },
                {
                    'title': 'Performance_Review_Q4.docx',
                    'content': f"Review data related to: {query}",
                    'relevance': 0.82,
                    'type': 'review'
                }
            ],
            'count': 2
        }

    def _extract_sources(self, query_type: str, results: Dict) -> List[str]:
        """Extract source information from results"""
        sources = []
        
        if query_type in ['sql', 'hybrid']:
            sources.append('database')
        
        if query_type in ['document', 'hybrid']:
            sources.append('documents')
            
        return sources

    def _generate_cache_key(self, query: str) -> str:
        """Generate a unique cache key for the query"""
        return hashlib.md5(f"{query}_{json.dumps(self.schema, sort_keys=True)}".encode()).hexdigest()

    def get_query_history(self, limit: int = 50) -> List[Dict]:
        """Get recent query history"""
