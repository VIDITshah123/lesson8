import pandas as pd
import sqlite3
import os
from datetime import datetime

def get_sql_type(dtype):
    """Convert pandas dtype to SQL type"""
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'REAL'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        return 'TEXT'

def generate_sql_script(excel_file, db_path):
    """Generate SQL script from Excel file"""
    # Read all sheets from Excel
    xl = pd.ExcelFile(excel_file)
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    sql_script = "-- SQL Script generated on {}\n".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sql_script += f"-- Source: {os.path.basename(excel_file)}\n\n"
    
    # Process each sheet
    for sheet_name in xl.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        table_name = sheet_name.lower().replace(' ', '_')
        
        # Generate CREATE TABLE statement
        columns = []
        for col in df.columns:
            col_name = col.lower().replace(' ', '_')
            col_type = get_sql_type(df[col].dtype)
            columns.append(f'    "{col_name}" {col_type}')
        
        create_table = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" (\n"
        create_table += ",\n".join(columns)
        
        # Add primary key if 'id' column exists
        if 'id' in [col.lower() for col in df.columns]:
            create_table += ",\n    PRIMARY KEY (\"id\")"
        
        create_table += "\n);\n"
        
        sql_script += f"-- Table: {table_name}\n"
        sql_script += create_table
        
        # Generate INSERT statements
        if not df.empty:
            sql_script += f"-- Data for {table_name}\n"
            # Replace NaN with None for proper NULL handling
            df = df.where(pd.notnull(df), None)
            
            for _, row in df.iterrows():
                cols = ', '.join([f'"{col.lower().replace(" ", "_")}"' for col in df.columns])
                placeholders = ', '.join(['?'] * len(df.columns))
                
                # Convert values to list, handling None values
                values = []
                for val in row.values:
                    if pd.isna(val):
                        values.append(None)
                    elif isinstance(val, (pd.Timestamp, datetime)):
                        values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                    else:
                        values.append(val)
                
                # Add INSERT statement
                insert_sql = f"INSERT OR REPLACE INTO \"{table_name}\" ({cols}) VALUES ({placeholders});\n"
                
                # Add to script with parameterized values
                sql_script += insert_sql
                sql_script += f"-- Values: {tuple(values)}\n"
        
        sql_script += "\n"
    
    # Close database connection
    conn.close()
    
    # Write SQL script to file
    sql_file = os.path.join(os.path.dirname(excel_file), 'db', 'migration_from_excel.sql')
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write(sql_script)
    
    print(f"SQL script generated: {sql_file}")
    return sql_file

if __name__ == "__main__":
    excel_file = os.path.join(os.path.dirname(__file__), 'basedatafile.xlsx')
    db_path = os.path.join(os.path.dirname(__file__), 'db', 'rsvp-base.db')
    
    if not os.path.exists(excel_file):
        print(f"Error: Excel file not found at {excel_file}")
    elif not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
    else:
        sql_file = generate_sql_script(excel_file, db_path)
        print(f"Please review the generated SQL script at: {sql_file}")
        print("After reviewing, you can apply it to the database using:")
        print(f'sqlite3 "{db_path}" < "{sql_file}"')
