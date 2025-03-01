from typing import Dict

import requests
import pandas as pd
from pandas import DataFrame, read_csv, read_json, to_datetime

def temp() -> DataFrame:
    """Get the temperature data.
    Returns:
        DataFrame: A dataframe with the temperature data.
    """
    return read_csv("data/temperature.csv")

def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil.
    Args:
        public_holidays_url (str): url to the public holidays.
        year (str): The year to get the public holidays for.
    Raises:
        SystemExit: If the request fails.
    Returns:
        DataFrame: A dataframe with the public holidays.
    """
    # TODO: Implementa esta función.
    # Debes usar la biblioteca requests para obtener los días festivos públicos del año dado.
    # La URL es public_holidays_url/{year}/BR.
    # Debes eliminar las columnas "types" y "counties" del DataFrame.
    # Debes convertir la columna "date" a datetime.
    # Debes lanzar SystemExit si la solicitud falla. Investiga el método raise_for_status
    # de la biblioteca requests.

   
    
    try:
        url = f"{public_holidays_url}"    #/{year}/BR
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()
        
        if not data:
            raise ValueError("No public holidays found for the given year")
        
        holidays_df = pd.DataFrame(data)
        holidays_df = holidays_df.drop(columns=["types", "counties"], errors="ignore")  
        holidays_df["date"] = pd.to_datetime(holidays_df["date"])

        return holidays_df[["date", "localName"]]

    except requests.RequestException as e:
        raise SystemExit(f"Error fetching public holidays: {e}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Invalid API response: {e}")
        
    


def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes.
    Args:
        csv_folder (str): The path to the csv's folder.
        csv_table_mapping (Dict[str, str]): The mapping of the csv file names to the
        table names.
        public_holidays_url (str): The url to the public holidays.
    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the table names and values as
        the dataframes.
    """
    dataframes = {
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }

    holidays = get_public_holidays(public_holidays_url, "2017")

    if isinstance(holidays, pd.DataFrame):
        dataframes["public_holidays"] = holidays
    else:
        raise ValueError("Public holidays is not a DataFrame")

    return dataframes
