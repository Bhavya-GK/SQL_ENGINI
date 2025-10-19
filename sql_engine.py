# professional_database.py - COMPLETE WORKING VERSION
import json
import re
import os
from datetime import datetime
from tabulate import tabulate

class SQLParser:
    @staticmethod
    def parse_create_table(query):
        """Parse CREATE TABLE statement"""
        try:
            # Extract table name and column definitions
            table_match = re.search(r'CREATE TABLE\s+(\w+)\s*\((.*)\)', query, re.IGNORECASE | re.DOTALL)
            if not table_match:
                return None, "Invalid CREATE TABLE syntax"
            
            table_name = table_match.group(1)
            columns_text = table_match.group(2)
            
            # Parse column definitions
            columns = {}
            column_defs = [col.strip() for col in columns_text.split(',')]
            
            for col_def in column_defs:
                if not col_def:
                    continue
                
                # Extract column name and properties
                col_parts = col_def.strip().split()
                if not col_parts:
                    continue
                
                column_name = col_parts[0]
                column_info = {'type': 'TEXT'}  # Default type
                
                # Determine data type
                for i, part in enumerate(col_parts[1:], 1):
                    part_upper = part.upper()
                    if part_upper in ['INT', 'INTEGER']:
                        column_info['type'] = 'INT'
                    elif part_upper in ['FLOAT', 'DOUBLE', 'DECIMAL']:
                        column_info['type'] = 'FLOAT'
                    elif part_upper in ['BOOLEAN', 'BOOL']:
                        column_info['type'] = 'BOOLEAN'
                    elif part_upper in ['DATE', 'DATETIME']:
                        column_info['type'] = 'DATE'
                    elif part_upper == 'PRIMARY' and i+1 < len(col_parts) and col_parts[i+1].upper() == 'KEY':
                        column_info['primary_key'] = True
                    elif part_upper == 'NOT' and i+1 < len(col_parts) and col_parts[i+1].upper() == 'NULL':
                        column_info['not_null'] = True
                    elif part_upper == 'UNIQUE':
                        column_info['unique'] = True
                    elif part_upper == 'DEFAULT' and i+1 < len(col_parts):
                        column_info['default'] = col_parts[i+1].strip("'\"")
            
                columns[column_name] = column_info
            
            schema = {'columns': columns}
            return table_name, schema
            
        except Exception as e:
            return None, f"Parse error: {str(e)}"
    
    @staticmethod
    def parse_insert(query):
        """Parse INSERT statement"""
        try:
            table_match = re.search(r'INSERT INTO\s+(\w+)\s+VALUES\s*\(([^)]+)\)', query, re.IGNORECASE)
            if not table_match:
                return None, "Invalid INSERT syntax"
            
            table_name = table_match.group(1)
            values_text = table_match.group(2)
            
            # Parse values
            values = []
            current = ""
            in_quotes = False
            quote_char = None
            
            for char in values_text:
                if char in ['"', "'"] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                elif char == ',' and not in_quotes:
                    values.append(current.strip())
                    current = ""
                    continue
                
                current += char
            
            if current.strip():
                values.append(current.strip())
            
            # Clean values
            cleaned_values = []
            for val in values:
                val = val.strip()
                if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                    cleaned_values.append(val[1:-1])
                elif val.upper() in ['TRUE', 'FALSE']:
                    cleaned_values.append(val.upper() == 'TRUE')
                elif val.isdigit():
                    cleaned_values.append(int(val))
                elif val.replace('.', '').isdigit():
                    cleaned_values.append(float(val))
                else:
                    cleaned_values.append(val)
            
            return table_name, cleaned_values
            
        except Exception as e:
            return None, f"Parse error: {str(e)}"
    
    @staticmethod
    def parse_select(query):
        """Parse SELECT statement"""
        try:
            # Extract basic components
            table_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
            if not table_match:
                return None, "Invalid SELECT syntax: Missing FROM clause"
            
            table_name = table_match.group(1)
            
            # Parse WHERE conditions
            conditions = []
            where_match = re.search(r'WHERE\s+(.+)', query, re.IGNORECASE)
            if where_match:
                where_clause = where_match.group(1)
                # Simple condition parsing (column operator value)
                condition_parts = re.split(r'\s+(AND|OR)\s+', where_clause, flags=re.IGNORECASE)
                
                for condition in condition_parts:
                    if condition.upper() in ['AND', 'OR']:
                        continue
                    
                    # Parse individual condition
                    operators = ['>=', '<=', '!=', '=', '>', '<', ' LIKE ']
                    found_operator = None
                    for op in operators:
                        if op in condition:
                            found_operator = op
                            break
                    
                    if found_operator:
                        col, val = condition.split(found_operator, 1)
                        conditions.append((col.strip(), found_operator.strip(), val.strip()))
            
            # Parse ORDER BY
            order_by = None
            order_match = re.search(r'ORDER BY\s+(\w+)\s+(ASC|DESC)', query, re.IGNORECASE)
            if order_match:
                order_by = (order_match.group(1), order_match.group(2))
            
            # Parse LIMIT
            limit = None
            limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
            if limit_match:
                limit = int(limit_match.group(1))
            
            return table_name, conditions, order_by, limit
            
        except Exception as e:
            return None, f"Parse error: {str(e)}"
    
    @staticmethod
    def parse_update(query):
        """Parse UPDATE statement"""
        try:
            table_match = re.search(r'UPDATE\s+(\w+)\s+SET\s+(.+?)\s+WHERE\s+(.+)', query, re.IGNORECASE | re.DOTALL)
            if not table_match:
                return None, "Invalid UPDATE syntax"
            
            table_name = table_match.group(1)
            set_clause = table_match.group(2)
            where_clause = table_match.group(3)
            
            # Parse SET clause
            updates = {}
            set_pairs = set_clause.split(',')
            for pair in set_pairs:
                if '=' in pair:
                    col, val = pair.split('=', 1)
                    updates[col.strip()] = val.strip().strip("'\"")
            
            # Parse WHERE conditions (simplified)
            conditions = []
            operators = ['>=', '<=', '!=', '=', '>', '<']
            found_operator = None
            for op in operators:
                if op in where_clause:
                    found_operator = op
                    break
            
            if found_operator:
                col, val = where_clause.split(found_operator, 1)
                conditions.append((col.strip(), found_operator.strip(), val.strip()))
            
            return table_name, updates, conditions
            
        except Exception as e:
            return None, f"Parse error: {str(e)}"
    
    @staticmethod
    def parse_delete(query):
        """Parse DELETE statement"""
        try:
            table_match = re.search(r'DELETE FROM\s+(\w+)\s+WHERE\s+(.+)', query, re.IGNORECASE)
            if not table_match:
                return None, "Invalid DELETE syntax"
            
            table_name = table_match.group(1)
            where_clause = table_match.group(2)
            
            # Parse WHERE conditions (simplified)
            conditions = []
            operators = ['>=', '<=', '!=', '=', '>', '<']
            found_operator = None
            for op in operators:
                if op in where_clause:
                    found_operator = op
                    break
            
            if found_operator:
                col, val = where_clause.split(found_operator, 1)
                conditions.append((col.strip(), found_operator.strip(), val.strip()))
            
            return table_name, conditions
            
        except Exception as e:
            return None, f"Parse error: {str(e)}"

class StorageEngine:
    def __init__(self, data_file="sql_engine.json"):
        self.data_file = data_file
        self.data = {}
        self.schemas = {}
        self.indexes = {}
        self.load_data()
    
    def load_data(self):
        """Load database from file"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    saved_data = json.load(f)
                    self.data = saved_data.get('tables', {})
                    self.schemas = saved_data.get('schemas', {})
                    self.indexes = saved_data.get('indexes', {})
                print(f"✓ Database loaded with {len(self.data)} tables")
            else:
                self.data = {}
                self.schemas = {}
                self.indexes = {}
                print("✓ New database created")
        except Exception as e:
            print(f"❌ Error loading database: {e}")
            self.data = {}
            self.schemas = {}
            self.indexes = {}
    
    def save_data(self):
        """Save database to file"""
        try:
            data_to_save = {
                'tables': self.data,
                'schemas': self.schemas,
                'indexes': self.indexes,
                'metadata': {
                    'last_updated': datetime.now().isoformat(),
                    'total_tables': len(self.data)
                }
            }
            with open(self.data_file, 'w') as f:
                json.dump(data_to_save, f, indent=2)
        except Exception as e:
            print(f"❌ Error saving database: {e}")
    
    # Enhanced Table Operations with Duplicate Protection
    def table_exists(self, table_name):
        """Check if table already exists"""
        return table_name in self.data
    
    def get_all_tables(self):
        """Get list of all existing tables"""
        return list(self.data.keys())
    
    def get_table_info(self, table_name):
        """Get detailed information about a table"""
        if not self.table_exists(table_name):
            return None
        
        schema = self.schemas.get(table_name, {})
        record_count = len(self.data.get(table_name, {}))
        columns = schema.get('columns', {})
        
        return {
            'name': table_name,
            'columns': columns,
            'record_count': record_count,
            'indexes': list(self.indexes.get(table_name, {}).keys())
        }
    
    def create_table(self, table_name, schema):
        """Create table only if it doesn't exist - WITH DUPLICATE PROTECTION"""
        if self.table_exists(table_name):
            return False, f"Table '{table_name}' already exists! Use a different name or DROP TABLE first."
        
        self.data[table_name] = {}
        self.schemas[table_name] = schema
        self.save_data()
        print(f"✓ Table '{table_name}' created successfully")
        return True, f"Table '{table_name}' created successfully"
    
    def drop_table(self, table_name):
        """Drop table if it exists"""
        if not self.table_exists(table_name):
            return False, f"Table '{table_name}' does not exist"
        
        # Confirm deletion for safety
        record_count = len(self.data[table_name])
        del self.data[table_name]
        if table_name in self.schemas:
            del self.schemas[table_name]
        if table_name in self.indexes:
            del self.indexes[table_name]
        
        self.save_data()
        return True, f"Table '{table_name}' dropped successfully ({record_count} records deleted)"
    
    def rename_table(self, old_name, new_name):
        """Rename an existing table"""
        if not self.table_exists(old_name):
            return False, f"Table '{old_name}' does not exist"
        
        if self.table_exists(new_name):
            return False, f"Table '{new_name}' already exists"
        
        # Rename the table
        self.data[new_name] = self.data.pop(old_name)
        if old_name in self.schemas:
            self.schemas[new_name] = self.schemas.pop(old_name)
        if old_name in self.indexes:
            self.indexes[new_name] = self.indexes.pop(old_name)
        
        self.save_data()
        return True, f"Table '{old_name}' renamed to '{new_name}'"
    
    # CRUD Operations with Table Existence Checks
    def insert(self, table_name, record_data):
        if not self.table_exists(table_name):
            return False, f"Table '{table_name}' does not exist. Create it first using CREATE TABLE."
        
        schema = self.schemas.get(table_name, {})
        columns = schema.get('columns', {})
        
        # Validate against schema
        validation_result = self._validate_record(record_data, columns, table_name)
        if not validation_result[0]:
            return validation_result
        
        # Get primary key
        pk_column = None
        for col_name, col_info in columns.items():
            if col_info.get('primary_key'):
                pk_column = col_name
                break
        
        if not pk_column:
            return False, "No primary key defined in table schema"
        
        record_id = record_data.get(pk_column)
        if not record_id:
            return False, f"Primary key '{pk_column}' is required"
        
        # Check for duplicate primary key
        if str(record_id) in self.data[table_name]:
            return False, f"Duplicate primary key '{record_id}' - record already exists"
        
        # Store record
        self.data[table_name][str(record_id)] = record_data
        
        # Update indexes
        self._update_indexes(table_name, record_id, record_data)
        
        self.save_data()
        return True, f"Record inserted into '{table_name}'"
    
    def select(self, table_name, conditions=None, order_by=None, limit=None):
        if not self.table_exists(table_name):
            return None, f"Table '{table_name}' does not exist"
        
        records = self.data[table_name]
        
        # Apply WHERE conditions
        if conditions:
            filtered_records = {}
            for record_id, record in records.items():
                if self._evaluate_conditions(record, conditions):
                    filtered_records[record_id] = record
            records = filtered_records
        
        # Convert to list for sorting
        records_list = list(records.values())
        
        # Apply ORDER BY
        if order_by:
            column, direction = order_by
            reverse = (direction.upper() == 'DESC')
            records_list.sort(key=lambda x: x.get(column, ''), reverse=reverse)
        
        # Apply LIMIT
        if limit and limit > 0:
            records_list = records_list[:limit]
        
        return records_list, f"Found {len(records_list)} records in '{table_name}'"
    
    def update(self, table_name, updates, conditions=None):
        if not self.table_exists(table_name):
            return False, f"Table '{table_name}' does not exist"
        
        updated_count = 0
        schema = self.schemas.get(table_name, {})
        columns = schema.get('columns', {})
        
        for record_id, record in self.data[table_name].items():
            # Check conditions
            if conditions and not self._evaluate_conditions(record, conditions):
                continue
            
            # Create updated record
            updated_record = record.copy()
            for column, new_value in updates.items():
                if column in record:
                    # Validate new value
                    col_info = columns.get(column, {})
                    validation = self._validate_value(new_value, col_info, column)
                    if not validation[0]:
                        return False, f"Validation failed for {column}: {validation[1]}"
                    updated_record[column] = new_value
            
            # Replace record
            self.data[table_name][record_id] = updated_record
            updated_count += 1
            
            # Update indexes
            self._update_indexes(table_name, record_id, updated_record)
        
        if updated_count > 0:
            self.save_data()
            return True, f"Updated {updated_count} records in '{table_name}'"
        else:
            return True, f"No records matched the conditions in '{table_name}'"
    
    def delete(self, table_name, conditions=None):
        if not self.table_exists(table_name):
            return False, f"Table '{table_name}' does not exist"
        
        deleted_count = 0
        records_to_delete = []
        
        for record_id, record in self.data[table_name].items():
            if not conditions or self._evaluate_conditions(record, conditions):
                records_to_delete.append(record_id)
        
        for record_id in records_to_delete:
            del self.data[table_name][record_id]
            deleted_count += 1
        
        if deleted_count > 0:
            self.save_data()
            return True, f"Deleted {deleted_count} records from '{table_name}'"
        else:
            return True, f"No records matched the conditions in '{table_name}'"

    # Schema Operations
    def describe_table(self, table_name):
        if not self.table_exists(table_name):
            return None, f"Table '{table_name}' does not exist"
        
        schema = self.schemas.get(table_name, {})
        return schema, f"Schema for table '{table_name}'"
    
    # Index Operations
    def create_index(self, table_name, column_name):
        if not self.table_exists(table_name):
            return False, f"Table '{table_name}' does not exist"
        
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        if column_name in self.indexes[table_name]:
            return False, f"Index on '{table_name}.{column_name}' already exists"
        
        # Build index
        index = {}
        for record_id, record in self.data[table_name].items():
            value = record.get(column_name)
            if value not in index:
                index[value] = []
            index[value].append(record_id)
        
        self.indexes[table_name][column_name] = index
        self.save_data()
        return True, f"Index created on '{table_name}.{column_name}'"
    
    # Helper Methods
    def _validate_record(self, record, columns, table_name):
        for col_name, col_info in columns.items():
            if col_info.get('not_null') and col_name not in record:
                return False, f"Required column '{col_name}' is missing"
        
        for col_name, value in record.items():
            if col_name not in columns:
                return False, f"Unknown column '{col_name}' in table '{table_name}'"
            
            col_info = columns[col_name]
            validation = self._validate_value(value, col_info, col_name)
            if not validation[0]:
                return validation
        
        return True, "Validation passed"
    
    def _validate_value(self, value, col_info, col_name):
        data_type = col_info.get('type', 'TEXT')
        
        try:
            if data_type == 'INT':
                int(value)
            elif data_type == 'FLOAT':
                float(value)
            elif data_type == 'BOOLEAN':
                if str(value).upper() not in ['TRUE', 'FALSE', '1', '0']:
                    return False, f"Column '{col_name}' must be BOOLEAN"
        except (ValueError, TypeError):
            return False, f"Column '{col_name}' must be {data_type}"
        
        return True, "Value valid"
    
    def _evaluate_conditions(self, record, conditions):
        for condition in conditions:
            column, operator, value = condition
            record_value = record.get(column)
            
            if operator == '=':
                if str(record_value) != str(value):
                    return False
            elif operator == '!=':
                if str(record_value) == str(value):
                    return False
            elif operator == '>':
                if not (float(record_value) > float(value)):
                    return False
            elif operator == '<':
                if not (float(record_value) < float(value)):
                    return False
            elif operator == '>=':
                if not (float(record_value) >= float(value)):
                    return False
            elif operator == '<=':
                if not (float(record_value) <= float(value)):
                    return False
        
        return True
    
    def _update_indexes(self, table_name, record_id, record):
        if table_name in self.indexes:
            for column_name, index in self.indexes[table_name].items():
                value = record.get(column_name)
                if value not in index:
                    index[value] = []
                if record_id not in index[value]:
                    index[value].append(record_id)

class ProfessionalDatabase:
    def __init__(self):
        print("🚀 Starting Professional Database Engine...")
        self.storage = StorageEngine()
        self.parser = SQLParser()
        print("✅ Professional Database ready with duplicate table protection!")
    
    def execute(self, query):
        query = query.strip()
        original_query = query
        query_upper = query.upper()
        
        print(f"📝 Executing: {original_query}")
        
        try:
            if query_upper.startswith("CREATE TABLE"):
                return self._create_table(original_query)
            elif query_upper.startswith("DROP TABLE"):
                return self._drop_table(original_query)
            elif query_upper.startswith("RENAME TABLE"):
                return self._rename_table(original_query)
            elif query_upper.startswith("INSERT INTO"):
                return self._insert(original_query)
            elif query_upper.startswith("SELECT"):
                return self._select(original_query)
            elif query_upper.startswith("UPDATE"):
                return self._update(original_query)
            elif query_upper.startswith("DELETE FROM"):
                return self._delete(original_query)
            elif query_upper.startswith("DESC ") or query_upper.startswith("DESCRIBE "):
                return self._describe_table(original_query)
            elif query_upper == "SHOW TABLES":
                return self._show_tables()
            elif query_upper.startswith("SHOW TABLE "):
                return self._show_table_info(original_query)
            elif query_upper.startswith("CREATE INDEX"):
                return self._create_index(original_query)
            else:
                return {"error": f"Unsupported SQL command"}
                
        except Exception as e:
            return {"error": f"Execution error: {str(e)}"}
    
    def _create_table(self, query):
        table_name, schema = self.parser.parse_create_table(query)
        if not table_name:
            return {"error": schema}
        
        # Check if table already exists
        if self.storage.table_exists(table_name):
            table_info = self.storage.get_table_info(table_name)
            columns = table_info['columns'] if table_info else {}
            column_list = ", ".join(columns.keys()) if columns else "unknown columns"
            return {
                "error": f"❌ Table '{table_name}' already exists!\n"
                        f"💡 Table has {len(columns)} columns: {column_list}\n"
                        f"💡 Use 'DESC {table_name}' to see structure\n"
                        f"💡 Use 'DROP TABLE {table_name}' to delete it first"
            }
        
        success, message = self.storage.create_table(table_name, schema)
        if success:
            columns = schema.get('columns', {})
            column_list = ", ".join([f"{name} {info['type']}" for name, info in columns.items()])
            return {"message": f"✅ {message}\n📊 Columns: {column_list}"}
        else:
            return {"error": f"❌ {message}"}
    
    def _drop_table(self, query):
        table_match = re.search(r'DROP TABLE\s+(\w+)', query, re.IGNORECASE)
        if not table_match:
            return {"error": "Invalid DROP TABLE syntax. Use: DROP TABLE table_name"}
        
        table_name = table_match.group(1)
        
        # Confirm table exists
        if not self.storage.table_exists(table_name):
            return {"error": f"❌ Cannot drop '{table_name}' - table does not exist"}
        
        success, message = self.storage.drop_table(table_name)
        if success:
            return {"message": f"✅ {message}"}
        else:
            return {"error": f"❌ {message}"}
    
    def _rename_table(self, query):
        rename_match = re.search(r'RENAME TABLE\s+(\w+)\s+TO\s+(\w+)', query, re.IGNORECASE)
        if not rename_match:
            return {"error": "Invalid RENAME syntax. Use: RENAME TABLE old_name TO new_name"}
        
        old_name = rename_match.group(1)
        new_name = rename_match.group(2)
        
        success, message = self.storage.rename_table(old_name, new_name)
        if success:
            return {"message": f"✅ {message}"}
        else:
            return {"error": f"❌ {message}"}
    
    def _show_table_info(self, query):
        table_match = re.search(r'SHOW TABLE\s+(\w+)', query, re.IGNORECASE)
        if not table_match:
            return {"error": "Invalid SHOW TABLE syntax. Use: SHOW TABLE table_name"}
        
        table_name = table_match.group(1)
        table_info = self.storage.get_table_info(table_name)
        
        if not table_info:
            return {"error": f"Table '{table_name}' does not exist"}
        
        # Format table information
        info_lines = [
            f"📋 Table: {table_info['name']}",
            f"📊 Records: {table_info['record_count']}",
            f"🔑 Indexes: {', '.join(table_info['indexes']) if table_info['indexes'] else 'None'}"
        ]
        
        return {"message": "\n".join(info_lines)}
    
    def _insert(self, query):
        table_name, values = self.parser.parse_insert(query)
        if not table_name:
            return {"error": values}
        
        # Check if table exists
        if not self.storage.table_exists(table_name):
            return {"error": f"❌ Table '{table_name}' does not exist. Create it first using:\nCREATE TABLE {table_name} (...)"}
        
        # Create record data
        schema = self.storage.schemas.get(table_name, {})
        columns = list(schema.get('columns', {}).keys())
        
        if len(values) != len(columns):
            return {"error": f"Column count mismatch. Table has {len(columns)} columns, but {len(values)} values provided"}
        
        record_data = dict(zip(columns, values))
        success, message = self.storage.insert(table_name, record_data)
        
        if success:
            return {"message": f"✅ {message}"}
        else:
            return {"error": f"❌ {message}"}
    
    def _select(self, query):
        result = self.parser.parse_select(query)
        if not result or not result[0]:
            return {"error": result[1] if result else "Invalid SELECT syntax"}
        
        table_name, conditions, order_by, limit = result
        
        # Check if table exists
        if not self.storage.table_exists(table_name):
            return {"error": f"❌ Table '{table_name}' does not exist"}
        
        records, message = self.storage.select(table_name, conditions, order_by, limit)
        
        if records is None:
            return {"error": message}
        else:
            return {"result": records, "message": message}
    
    def _update(self, query):
        result = self.parser.parse_update(query)
        if not result or not result[0]:
            return {"error": result[1] if result else "Invalid UPDATE syntax"}
        
        table_name, updates, conditions = result
        
        # Check if table exists
        if not self.storage.table_exists(table_name):
            return {"error": f"❌ Table '{table_name}' does not exist"}
        
        success, message = self.storage.update(table_name, updates, conditions)
        
        if success:
            return {"message": f"✅ {message}"}
        else:
            return {"error": f"❌ {message}"}
    
    def _delete(self, query):
        result = self.parser.parse_delete(query)
        if not result or not result[0]:
            return {"error": result[1] if result else "Invalid DELETE syntax"}
        
        table_name, conditions = result
        
        # Check if table exists
        if not self.storage.table_exists(table_name):
            return {"error": f"❌ Table '{table_name}' does not exist"}
        
        success, message = self.storage.delete(table_name, conditions)
        
        if success:
            return {"message": f"✅ {message}"}
        else:
            return {"error": f"❌ {message}"}
    
    def _describe_table(self, query):
        table_match = re.search(r'(?:DESC|DESCRIBE)\s+(\w+)', query, re.IGNORECASE)
        if not table_match:
            return {"error": "Invalid DESCRIBE syntax"}
        
        table_name = table_match.group(1)
        
        # Check if table exists
        if not self.storage.table_exists(table_name):
            return {"error": f"❌ Table '{table_name}' does not exist"}
        
        schema, message = self.storage.describe_table(table_name)
        
        if schema is None:
            return {"error": message}
        else:
            return {"schema": schema, "message": message}
    
    def _show_tables(self):
        tables = self.storage.get_all_tables()
        if not tables:
            return {"message": "📭 No tables in database"}
        
        # Get detailed table information
        table_data = []
        for i, table_name in enumerate(tables):
            table_info = self.storage.get_table_info(table_name)
            record_count = table_info['record_count'] if table_info else 0
            column_count = len(table_info['columns']) if table_info and 'columns' in table_info else 0
            table_data.append([i+1, table_name, column_count, record_count])
        
        headers = ['#', 'TABLE NAME', 'COLUMNS', 'RECORDS']
        table = tabulate(table_data, headers, tablefmt='grid')
        return {"message": f"📊 Database Tables:\n{table}\nTotal: {len(tables)} tables"}
    
    def _create_index(self, query):
        index_match = re.search(r'CREATE INDEX ON\s+(\w+)\s*\((\w+)\)', query, re.IGNORECASE)
        if not index_match:
            return {"error": "Invalid CREATE INDEX syntax. Use: CREATE INDEX ON table_name(column_name)"}
        
        table_name = index_match.group(1)
        column_name = index_match.group(2)
        
        # Check if table exists
        if not self.storage.table_exists(table_name):
            return {"error": f"❌ Table '{table_name}' does not exist"}
        
        success, message = self.storage.create_index(table_name, column_name)
        
        if success:
            return {"message": f"✅ {message}"}
        else:
            return {"error": f"❌ {message}"}

def format_database_result(result):
    """Format database results professionally"""
    if not result:
        return "❌ No result"
    
    if 'error' in result:
        return result['error']
    
    if 'message' in result:
        return result['message']
    
    if 'schema' in result:
        schema = result['schema']
        columns = schema.get('columns', {})
        
        if not columns:
            return "📭 No columns defined"
        
        table_data = []
        for col_name, col_info in columns.items():
            constraints = []
            if col_info.get('primary_key'):
                constraints.append('PRIMARY KEY')
            if col_info.get('not_null'):
                constraints.append('NOT NULL')
            if col_info.get('unique'):
                constraints.append('UNIQUE')
            if col_info.get('default') is not None:
                constraints.append(f"DEFAULT {col_info['default']}")
            
            table_data.append([
                col_name,
                col_info.get('type', 'TEXT'),
                ', '.join(constraints) if constraints else '-'
            ])
        
        headers = ['COLUMN', 'TYPE', 'CONSTRAINTS']
        table = tabulate(table_data, headers, tablefmt='grid')
        return f"📋 Table Schema:\n{table}"
    
    if 'result' in result:
        data = result['result']
        message = result.get('message', 'Query executed successfully')
        
        if not data:
            return f"📭 {message}"
        
        if isinstance(data, list) and data:
            # Get all keys from first record for headers
            headers = list(data[0].keys())
            table_data = [[record.get(h, 'NULL') for h in headers] for record in data]
            table = tabulate(table_data, headers, tablefmt='grid')
            return f"📊 {message}:\n{table}"
    
    return "❓ Unexpected result format"

def main():
    """Professional Database Interactive Shell"""
    try:
        import tabulate
    except ImportError:
        print("📦 Installing tabulate library...")
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "tabulate"])
        print("✅ Tabulate installed successfully!")
    
    db = ProfessionalDatabase()
    
    print("=" * 80)
    print("🎪 PROFESSIONAL DATABASE - DUPLICATE TABLE PROTECTION")
    print("=" * 80)
    print("🛡️  DUPLICATE PROTECTION FEATURES:")
    print("  ✅ Prevents creating tables that already exist")
    print("  ✅ Clear error messages with suggestions")
    print("  ✅ Table existence checks for all operations")
    print("  ✅ SHOW TABLE command for detailed table info")
    print("  ✅ RENAME TABLE command to rename existing tables")
    print("=" * 80)
    print("📖 EXAMPLE QUERIES:")
    print("  CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    print("  CREATE TABLE users (id INT, email TEXT)  # This will fail")
    print("  SHOW TABLES")
    print("  SHOW TABLE users")
    print("  RENAME TABLE users TO customers")
    print("  DROP TABLE users")
    print("=" * 80)
    
    while True:
        try:
            command = input("\n💻 SQL> ").strip()
            
            if command.lower() in ['exit', 'quit', 'q']:
                print("\n👋 Thank you for using Professional Database!")
                break
            
            if command.lower() == 'help':
                print_help()
                continue
            
            if not command:
                continue
            
            result = db.execute(command)
            formatted = format_database_result(result)
            print(f"\n{formatted}")
            
        except KeyboardInterrupt:
            print("\n\n👋 Thank you for using Professional Database!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")

def print_help():
    """Display comprehensive help"""
    print("\n" + "=" * 60)
    print("📚 PROFESSIONAL DATABASE - DUPLICATE PROTECTION")
    print("=" * 60)
    print("\n🛡️  TABLE MANAGEMENT COMMANDS:")
    print("  CREATE TABLE table (...)     - Create new table")
    print("  DROP TABLE table            - Delete table")
    print("  RENAME TABLE old TO new     - Rename table")
    print("  SHOW TABLES                 - List all tables")
    print("  SHOW TABLE table            - Show table details")
    print("  DESC table                  - Show table schema")
    print("\n💡 DUPLICATE PROTECTION:")
    print("  • CREATE TABLE fails if table exists")
    print("  • Clear error messages with suggestions")
    print("  • All operations check if table exists first")
    print("=" * 60)

if __name__ == "__main__":
    main()
