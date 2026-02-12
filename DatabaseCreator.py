import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text


class DatabaseCreator:
    def __init__(self, db_settings, base_db="postgres", host="localhost", port=5432):
        """Initialize with base database (default: 'postgres') and connection details"""
        self.user = db_settings['user']
        self.password = db_settings['password']
        self.host = host
        self.port = port
        self.base_db = base_db
        self.engine = self._create_engine(self.base_db)

    def _create_engine(self, dbname):
        """Internal method to create a SQLAlchemy engine for a given database"""
        connection_string = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{dbname}"
        return create_engine(connection_string)

    def create_database(self, new_db_name):
        """Create a new PostgreSQL database"""
        with self.engine.connect() as conn:
            conn.execute(text("COMMIT"))  # required before CREATE DATABASE
            conn.execute(text(f"CREATE DATABASE {new_db_name}"))

    def get_engine_for(self, dbname):
        """Return a SQLAlchemy engine for a specific database (e.g., the newly created one)"""
        return self._create_engine(dbname)
    
    #infer sql data type from pandas dtype, then create staging table from that

    def map_dtype_to_sql(self,dtype) -> str:
        """Map pandas dtype to SQL type."""
        if pd.api.types.is_integer_dtype(dtype):
            return "NUMERIC"
        elif pd.api.types.is_float_dtype(dtype):
            return "NUMERIC"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            return "TEXT"
        else:
            return "TEXT"  # fallback for unsupported dtypes

    def create_staging_table_sql_from_df(self,
        df: pd.DataFrame, 
        table_name: str, 
        unlogged: bool = False,
        overrides: dict = None
    ) -> str:
        """
        Generate a SQL string to drop and create a staging table
        by inferring column types from a pandas DataFrame, with optional overrides.
        
        Parameters:
            df (pd.DataFrame): DataFrame to infer schema from
            table_name (str): Table name
            unlogged (bool): Whether to create an UNLOGGED table
            overrides (dict): Optional {col_name: sql_type} to force type
        """
        if overrides is None:
            overrides = {}

        table_type = "UNLOGGED" if unlogged else ""
        
        col_defs = []
        for col, dtype in df.dtypes.items():
            sql_type = overrides.get(col, self.map_dtype_to_sql(dtype))
            col_defs.append(f"{col} {sql_type}")
        
        col_defs_str = ",\n        ".join(col_defs)
        
        sql = f"""
        DROP TABLE IF EXISTS {table_name} CASCADE;
        CREATE {table_type} TABLE {table_name} (
            {col_defs_str}
        );"""
        self.insert_table_sql_query = sql.strip()
        
        return self.insert_table_sql_query
    
    def create_staging_table_then_insert_data(self, table_name, data):
        self.table_name = table_name
        self.data = data
        with self.engine.connect() as conn:
            conn.execute(text(self.create_staging_table_sql_from_df(self.data, self.table_name)))
            conn.commit()
        self.data.to_sql(self.table_name, con=self.engine, if_exists='append',index=False)

    def table_to_df(self, table_name):
        with self.engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            return df
        
    def sql_query_to_df(self, query):
        with self.engine.connect() as conn:
            df = pd.read_sql(f"{query}", conn)
            return df
        
    def run_sql(self, query, params=None):
        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
            return df
        
    def execute_sql(self, query, params=None):
        """Execute SQL that doesn't return results (INSERT, UPDATE, DELETE, ALTER, etc.)"""
        with self.engine.connect() as conn:
            conn.execute(text(query), params or {})
            conn.commit()

    