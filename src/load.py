from typing import Dict

from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def get_database_connection(database_url: str) -> Engine:
    
    try:
        engine = create_engine(database_url)
        print("Conexión a la base de datos establecida exitosamente.")
        return engine
    except Exception as e:
        raise SystemExit(f"Error al conectar la base de datos: {e}")




def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Load the dataframes into the SQLite database."""
    try:
        for table_name, df in data_frames.items():
            try:
                # Aquí se pasa el objeto Engine directamente
                df.to_sql(name=table_name, con=database, if_exists="replace", index=False)
                print(f"Tabla '{table_name}' cargada exitosamente.")
            except Exception as e:
                print(f"Error al cargar la tabla '{table_name}': {e}")
    except Exception as e:
        print(f"Error al conectar la base de datos: {e}")
