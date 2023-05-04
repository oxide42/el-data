from dateutil import tz
from datetime import datetime
import pandas as pd


def energidataservice(est_data: pd.DataFrame) -> pd.DataFrame:
    est_data['date'] = pd.to_datetime(est_data["HourUTC"], infer_datetime_format=True, errors='coerce')
    est_data['SpotPriceDKK'] = round(est_data['SpotPriceDKK'] / 1000, 2)
    est_data['SpotPriceDK1'] = est_data.loc[est_data['PriceArea'] == 'DK1', 'SpotPriceDKK']
    est_data['SpotPriceDK2'] = est_data.loc[est_data['PriceArea'] == 'DK2', 'SpotPriceDKK']

    est_data = est_data.groupby(est_data['date']).aggregate({'SpotPriceDK1': 'first', 'SpotPriceDK2': 'first'}).reset_index()
    est_data = est_data.set_index("date").resample("1h").first().ffill().reset_index()
    est_data = est_data.set_index('date').tz_localize('utc').reset_index()
    return est_data

def openweathermap(json: str) -> pd.DataFrame:
    df = pd.json_normalize(json, record_path=['list'])
    df['date'] = pd.to_datetime(1e9*df['dt'], infer_datetime_format=True)
    df = df.set_index('date').tz_localize("utc").reset_index()
    return df

def nve(json: str) -> pd.DataFrame:
    df = pd.json_normalize(json)

    df = df.rename(columns={'dato_Id': 'date', 'fyllingsgrad': 'Water'})

    df = df.loc[df['date'] >= '2020-01-01']
    df.loc[len(df.index), 'date'] = '2020-01-01'
    df.loc[len(df.index), 'date'] = datetime.utcnow().strftime('%Y-%m-%d')

    df['date'] = pd.to_datetime(df["date"], infer_datetime_format=True, errors='coerce')
    df = df[['date', 'Water']]

    df = df.groupby(['date']).mean(numeric_only=True).reset_index()

    df = df.set_index('date').tz_localize("utc").reset_index()
    df = df.set_index("date").resample("1h").first().ffill().bfill().reset_index()
    df = df.sort_values(['date'])

    return df

def ecad(df, id) -> pd.DataFrame:
    df = df.rename(columns={'DATE': 'date', 'FG': id})
    df = df.loc[df['Q_FG'] == 0]
    df = pd.DataFrame(df, columns=['date', id])
    df['date'] = pd.to_datetime(df["date"], infer_datetime_format=True, errors='coerce')

    df = df.loc[df['date'] >= '2020-01-01']
    df.loc[len(df.index), 'date'] = '2020-01-01'
    df.loc[len(df.index), 'date'] = datetime.utcnow().strftime('%Y-%m-%d')

    df = df.set_index('date').tz_localize("utc").reset_index()
    df = df.set_index("date").resample("1h").first()
    df[id] = df[id].interpolate(method='polynomial', order=2)
    df = df.sort_values(['date']).reset_index()

    df[id] = df[id] / 10

    return df

def marketwatch(json) -> pd.DataFrame:
    df_date = pd.json_normalize(json, record_path=['TimeInfo', 'Ticks'])
    df_price = pd.json_normalize(json, record_path=['Series', 'DataPoints'])

    df = pd.DataFrame();
    df['date'] = df_date[0]
    df['CoalPrice'] = df_price[0]

    df['date'] = pd.to_datetime(1e6*df['date'], infer_datetime_format=True)
    df = df.set_index('date').tz_localize("utc").reset_index()

    df = df.loc[df['date'] >= '2020-01-01'].dropna()
    df = df.set_index("date").resample("1h").first().ffill().bfill().reset_index()

    return df

def dmi(json, id) -> pd.DataFrame:
    df = pd.json_normalize(json['features'])
    df = pd.DataFrame(df, columns=['properties.from', 'properties.value'])
    df = df.rename(columns={'properties.from': 'date', 'properties.value': id})
    df['date'] = pd.to_datetime(df["date"], infer_datetime_format=True, errors='coerce')

    return df
