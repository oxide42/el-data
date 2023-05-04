from pandas import DataFrame
import numpy as np

def combine(df1, df2: DataFrame, on: str = 'date') -> DataFrame:
    df1 = df1.set_index(on)
    df2 = df2.set_index(on)
    df = df1.combine_first(df2)
    df = df.sort_values('date').reset_index()
    return df

def slope(index, data, order=1):
    coeffs = np.polyfit(index, list(data), order)
    return float(coeffs[-2])