import os
import glob

from pandas import DataFrame
from functools import reduce
from dateutil import parser
from dateutil.tz import gettz
import pandas as pd
import json
import numpy as np
import requests
from datetime import datetime

import core.adapters
from core.config import cfg
import core.crosscutting

FROM_FILE = True
FETCH_FROM_DATE = '2020-01-01'

if __name__ == '__main__':
    ROOT = '..'
else:
    ROOT = '.'
    
'''
    Stations: https://confluence.govcloud.dk/pages/viewpage.action?pageId=41717446
    API: https://confluence.govcloud.dk/pages/viewpage.acztion?pageId=41718244
    EU: https://www.ecad.eu
'''
weather_stations = [
    {'id': 'w1', 'source': 'dmi', 'source_id': '06052', 'name': 'Thyborøn', 'lat': 56.705, 'lon': 8.2206},
    {'id': 'w2', 'source': 'dmi', 'source_id': '06041', 'name': 'Skagen Fyr', 'lat': 57.7364, 'lon': 10.6316},
    {'id': 'w3', 'source': 'dmi', 'source_id': '06070', 'name': 'Århus Lufthavn', 'lat': 56.3083, 'lon': 10.6254},
    {'id': 'w4', 'source': 'dmi', 'source_id': '06118', 'name': 'Sønderborg Lufthavn', 'lat': 54.9616, 'lon': 9.793},
    {'id': 'w5', 'source': 'dmi', 'source_id': '06096', 'name': 'Rømø/Juvre', 'lat': 55.1904, 'lon': 8.5599},
    {'id': 'w6', 'source': 'dmi', 'source_id': '06170', 'name': 'Roskilde Lufthavn', 'lat': 55.5867, 'lon': 12.1366},
    {'id': 'w7', 'source': 'dmi', 'source_id': '06096', 'name': 'Rødbyhavn', 'lat': 54.6573, 'lon': 11.3515},
    {'id': 'w8', 'source': 'dmi', 'source_id': '06120', 'name': 'Odense Lufthavn', 'lat': 55.4735, 'lon': 10.3297},
    {'id': 'w9', 'source': 'dmi', 'source_id': '06079', 'name': 'Anholt Havn', 'lat': 56.7169, 'lon': 11.5098},
    # {'id': 'wa', 'source': 'ecad', 'source_id': 'FG_STAID000160', 'name': 'Den Helder, NL', 'lat': 52.9613, 'lon': 4.7472},
    # {'id': 'wb', 'source': 'ecad', 'source_id': 'FG_STAID000476', 'name': 'Flughafen Hannover-Langenhagen, DE', 'lat': 52.4658, 'lon': 9.6797},
    {'id': 'wc', 'source': 'dmi', 'source_id': '06169', 'name': 'Gniben', 'lat': 56.0067, 'lon': 11.2805},
    {'id': 'wd', 'source': 'dmi', 'source_id': '06156', 'name': 'Holbæk', 'lat': 55.7358, 'lon': 11.6035},
]

def Groom(df, is_utc):
    df = df.sort_values('date')
    df['date'] = pd.to_datetime(df["date"], infer_datetime_format=True, errors='coerce')
    if is_utc:
        df = df.set_index('date').tz_localize("utc").reset_index()
    else:
        df = df.set_index('date').tz_convert("utc").reset_index()

    return df


def import_el_prices():
    if FROM_FILE and os.path.isfile(ROOT + '/data/api/price.json'):
        with open(ROOT + '/data/api/price.json') as json_file:
            price_data = json.load(json_file)
    else:
        now_time = datetime.utcnow().strftime('%Y-%m-%dT%H:00')
        response = requests.get(
            'https://api.energidataservice.dk/dataset/Elspotprices?start=%s&end=%s&columns=HourUTC,SpotPriceDKK,PriceArea&timezone=utc&filter={"PriceArea":"DK1,DK2"}' % (FETCH_FROM_DATE, now_time))
        if response.status_code < 200 or response.status_code > 299:
            return response.text
        price_data = response.json()

        with open(ROOT + '/data/api/price.json', 'w+', encoding='utf-8') as json_file:
            json.dump(price_data, json_file)

    df = DataFrame(price_data['records'])
    df = core.adapters.energidataservice(df)

    return df


def import_gas_prices():
    root = '.'
    if __name__ == '__main__':
        root = '..'

    if not FROM_FILE:
        url = "https://webservice-eex.gvsi.com/query/json/getDaily/close/tradedatetimegmt/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0',
            'Accept': '*/*',
            'Accept-Language': 'da,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Origin': 'https://www.eex.com',
            'Connection': 'keep-alive',
            'Referer': 'https://www.eex.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'DNT': '1',
            'Sec-GPC': '1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
        }
        now = datetime.utcnow()
        params = {
            'priceSymbol': '"#E.ETF_GND1"',
            # 'priceSymbol': '"#E.ETF_WDRP"',
            'chartstartdate': now.strftime('%Y/%m/01'),
            'chartstopdate': now.strftime('%Y/%m/%d'),
            'dailybarinterval': 'Days',
            'aggregatepriceselection': 'First',
        }

        response = requests.get(url, params=params, headers=headers)
        if response.status_code < 200 or response.status_code > 299:
            return response.text

        gas_price_data = response.json()
        with open(root + '/data/api/gas/%s.json' % (now.strftime('%Y-%m')), 'w+', encoding='utf-8') as json_file:
            json.dump(gas_price_data, json_file)

    df = None
    file_list = glob.glob(root + '/data/api/gas/*.json')
    for file in file_list:
        with open(file) as json_file:
            content = json.load(json_file)
            if df is None:
                df = DataFrame(content['results']['items'])
                df = df.set_index('tradedatetimegmt')
            else:
                df_part = DataFrame(content['results']['items'])
                df_part = df_part.set_index('tradedatetimegmt')
                df = df.combine_first(df_part)

    df = df.reset_index()
    # Price in EUR/MWh | Volume in MWh | Time in CE(S)T
    # df = DataFrame(gas_price_data['results']['items'])
    df['date'] = pd.to_datetime(df['tradedatetimegmt'], format='%m/%d/%Y %I:%M:%S %p')
    df = df.rename(columns={'close': 'GasPrice'})[['GasPrice', 'date']]
    df['GasPrice'] = df['GasPrice'] * 0.01055 * 7.5    # eur/MWh -> kr/m3

    # 4294967295 -> 42.94967295
    df.loc[df['GasPrice'] > 1000, 'GasPrice'] = df['GasPrice'] / 1e8

    df = Groom(df, is_utc=True).bfill().dropna()

    return df


def import_clouds() -> DataFrame:
    API_KEY = '33262e76-8ef3-4061-b309-fc77150e082b'

    if FROM_FILE and os.path.isfile(ROOT + '/data/api/clouds.json'):
        with open(ROOT + '/data/api/clouds.json') as json_file:
            clouds_data = json.load(json_file)
    else:
        now_time = datetime.utcnow().strftime('%Y-%m-%dT%H:00:00Z')
        clouds_data = requests.get(
            'https://dmigw.govcloud.dk/v2/climateData/collections/countryValue/items?timeResolution=hour&datetime=%sT00:00:00Z/%s&parameterId=mean_cloud_cover&api-key=%s&limit=30000' % (FETCH_FROM_DATE, now_time, API_KEY)).json()
        with open(ROOT + '/data/api/clouds.json', 'w+', encoding='utf-8') as json_file:
            json.dump(clouds_data, json_file)

    df = pd.json_normalize(clouds_data['features'])
    df_sliced = pd.DataFrame(df, columns=['properties.from', 'properties.value'])
    df_sliced = df_sliced.rename(columns={'properties.from': 'date', 'properties.value': 'Clouds'})
    df_sliced['date'] = pd.to_datetime(df_sliced["date"], infer_datetime_format=True, errors='coerce')

    return df_sliced


def import_reservoir_pct() -> DataFrame:
    if FROM_FILE and os.path.isfile(ROOT + '/data/api/reservoir.json'):
        with open(ROOT + '/data/api/reservoir.json') as json_file:
            reservoir_data = json.load(json_file)
    else:
        url = 'https://biapi.nve.no/magasinstatistikk/api/Magasinstatistikk/HentOffentligData'
        reservoir_data = requests.get(url).json()
        with open(ROOT + '/data/api/reservoir.json', 'w+', encoding='utf-8') as json_file:
            json.dump(reservoir_data, json_file)

    df = core.adapters.nve(reservoir_data)

    return df

def import_coal_prices():
    if FROM_FILE and os.path.isfile(ROOT + '/data/api/coal.json'):
        with open(ROOT + '/data/api/coal.json') as json_file:
            coal_data = json.load(json_file)
    else:
        url = "https://api-secure.wsj.net/api/michelangelo/timeseries/history?json=%7B%22Step%22%3A%22P7D%22%2C%22TimeFrame%22%3A%22ALL%22%2C%22EntitlementToken%22%3A%22cecc4267a0194af89ca343805a3e57af%22%2C%22IncludeMockTick%22%3Afalse%2C%22FilterNullSlots%22%3Afalse%2C%22FilterClosedPoints%22%3Atrue%2C%22IncludeClosedSlots%22%3Afalse%2C%22IncludeOfficialClose%22%3Atrue%2C%22InjectOpen%22%3Afalse%2C%22ShowPreMarket%22%3Afalse%2C%22ShowAfterHours%22%3Afalse%2C%22UseExtendedTimeFrame%22%3Atrue%2C%22WantPriorClose%22%3Atrue%2C%22IncludeCurrentQuotes%22%3Afalse%2C%22ResetTodaysAfterHoursPercentChange%22%3Afalse%2C%22Series%22%3A%5B%7B%22Key%22%3A%22FUTURE%2FUS%2FXNYM%2FMTFC00%22%2C%22Dialect%22%3A%22Charting%22%2C%22Kind%22%3A%22Ticker%22%2C%22SeriesId%22%3A%22s1%22%2C%22DataTypes%22%3A%5B%22Last%22%5D%7D%5D%7D&ckey=cecc4267a0"

        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'da,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Dylan2010.EntitlementToken': 'cecc4267a0194af89ca343805a3e57af',
            'Origin': 'https://www.marketwatch.com',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'cross-site',
            'DNT': '1',
            'Sec-GPC': '1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Referer': 'https://www.marketwatch.com/'
        }
        response = requests.get(url, headers=headers)
        if response.status_code < 200 or response.status_code > 299:
            return response.text
        coal_data = response.json()
        with open(ROOT + '/data/api/coal.json', 'w+', encoding='utf-8') as json_file:
            json.dump(coal_data, json_file)

    df = core.adapters.marketwatch(coal_data)

    return df

def import_radiation() -> DataFrame:
    API_KEY = '33262e76-8ef3-4061-b309-fc77150e082b'

    if FROM_FILE and os.path.isfile(ROOT + '/data/api/radiation.json'):
        with open(ROOT + '/data/api/radiation.json') as json_file:
            radiation_data = json.load(json_file)
    else:
        now_time = datetime.utcnow().strftime('%Y-%m-%dT%H:00:00Z')
        radiation_data = requests.get(
            'https://dmigw.govcloud.dk/v2/climateData/collections/countryValue/items?timeResolution=hour&datetime=%sT00:00:00Z/%s&parameterId=mean_radiation&api-key=%s&limit=30000' % (FETCH_FROM_DATE, now_time, API_KEY)).json()
        with open(ROOT + '/data/api/radiation.json', 'w+', encoding='utf-8') as json_file:
            json.dump(radiation_data, json_file)

    df = pd.json_normalize(radiation_data['features'])
    df_sliced = pd.DataFrame(df, columns=['properties.from', 'properties.value'])
    df_sliced = df_sliced.rename(columns={'properties.from': 'date', 'properties.value': 'Radiation'})
    df_sliced['date'] = pd.to_datetime(df_sliced["date"], infer_datetime_format=True, errors='coerce')

    return df_sliced

def import_wind_dmi(id, source, source_id) -> DataFrame:
    storage_file_name = ROOT + '/data/api/wind/%s_%s.json' % (source, source_id)

    if FROM_FILE and os.path.isfile(storage_file_name):
        with open(storage_file_name) as json_file:
            wind_data = json.load(json_file)
    else:
        now_time = datetime.utcnow().strftime('%Y-%m-%dT%H:00:00Z')
        params = {
            'timeResolution': 'hour',
            'datetime': '%sT00:00:00Z/%s' % (FETCH_FROM_DATE, now_time),
            'parameterId': 'mean_wind_speed',
            'api-key': cfg().DMI_API_KEY,
            'limit': 30000
        }

        if source_id:
            level = 'stationValue'
            params['stationId'] = source_id
        else:
            level = 'countryValue'

        url = 'https://dmigw.govcloud.dk/v2/climateData/collections/%s/items' % level

        wind_data = requests.get(url=url, params=params).json()
        with open(storage_file_name, 'w+', encoding='utf-8') as json_file:
            json.dump(wind_data, json_file)

    df = pd.json_normalize(wind_data['features'])
    df_sliced = pd.DataFrame(df, columns=['properties.from', 'properties.value'])
    df_sliced = df_sliced.rename(columns={'properties.from': 'date', 'properties.value': id})
    df_sliced['date'] = pd.to_datetime(df_sliced["date"], infer_datetime_format=True, errors='coerce')

    return df_sliced

def import_wind_ecad(id, source, source_id) -> DataFrame:
    storage_file_name = ROOT + '/data/api/wind/%s_%s.txt' % (source, source_id)

    date_parser = lambda x: datetime.strptime(x, '%Y%m%d')
    df = pd.read_csv(storage_file_name, delimiter=',', skipinitialspace=True, usecols=['DATE', 'FG', 'Q_FG'], parse_dates=['DATE'], date_parser=date_parser)
    df = core.adapters.ecad(df, id)

    return df


def import_wind(id, source, source_id) -> DataFrame:
    if source == "dmi":
        return import_wind_dmi(id, source, source_id)
    if source == "ecad":
        return import_wind_ecad(id, source, source_id)

def import_temperature() -> DataFrame:
    storage_file_name = ROOT + '/data/api/temperature.json'

    if FROM_FILE and os.path.isfile(storage_file_name):
        with open(storage_file_name) as json_file:
            wind_data = json.load(json_file)
    else:
        now_time = datetime.utcnow().strftime('%Y-%m-%dT%H:00:00Z')
        params = {
            'timeResolution': 'hour',
            'datetime': '%sT00:00:00Z/%s' % (FETCH_FROM_DATE, now_time),
            'parameterId': 'mean_temp',
            'api-key': cfg().DMI_API_KEY,
            'limit': 30000
        }

        level = 'countryValue'
        url = 'https://dmigw.govcloud.dk/v2/climateData/collections/%s/items' % level

        response = requests.get(url=url, params=params)

        if response.status_code < 200 or response.status_code > 299:
            return response.text

        wind_data = response.json()
        with open(storage_file_name, 'w+', encoding='utf-8') as json_file:
            json.dump(wind_data, json_file)

    df = core.adapters.dmi(wind_data, 'Temperature')

    return df

def import_winds():
    df = None
    for wind in weather_stations:
        if df is None:
            df = import_wind(wind['id'], wind['source'], wind['source_id'])
            df = df.set_index('date')
        else:
            df_part = import_wind(wind['id'], wind['source'], wind['source_id'])
            df_part = df_part.set_index('date')
            df = df.combine_first(df_part)

    return df

def forecast_clouds(location: str) -> DataFrame:
    params = {
        "appid": cfg().OPENWEATHERMAP_API_KEY,
        "q": location,
        "units": "metric",
    }
    url = "https://api.openweathermap.org/data/2.5/forecast"

    weather_data = requests.get(url=url, params=params).json()

    df = core.adapters.openweathermap(weather_data)
    df = df.rename(columns={'clouds.all': 'Clouds'})
    df = df[['date', 'Clouds']]

    return df

def apply_solar_insolation(df: DataFrame) -> DataFrame:
    """
    Requires clouds loaded
    """
    from core.vejret.sun import SunCalculations, SolarInsolationCalculations
    lat = 55.4735
    lon = 10.3297
    df['SolarInsolation'] = 0

    def apply_sun(row):
        sundata = SunCalculations(lat, lon, row['date'])
        row['SolarInsolation'] = SolarInsolationCalculations(sundata["zenith"], row['Clouds'])
        return row

    df = df.apply(lambda row: apply_sun(row), axis=1)
    return df

def forecast_temperature(location: str) -> DataFrame:
    params = {
        "appid": cfg().OPENWEATHERMAP_API_KEY,
        "q": location,
        "units": "metric",
    }
    url = "https://api.openweathermap.org/data/2.5/forecast"

    weather_data = requests.get(url=url, params=params).json()

    df = core.adapters.openweathermap(weather_data)
    df = df.rename(columns={'main.temp': 'Temperature'})
    df = df[['date', 'Temperature']]

    return df

def forecast_wind_at(lat: float, lon: float, id: str) -> DataFrame:
    params = {
        "appid": cfg().OPENWEATHERMAP_API_KEY,
        "lat": lat,
        "lon": lon,
        "units": "metric",
    }
    url = "https://api.openweathermap.org/data/2.5/forecast"

    weather_data = requests.get(url=url, params=params).json()

    df = core.adapters.openweathermap(weather_data)
    df = df.rename(columns={'wind.speed': id})
    df = df[['date', id]]

    return df

def forecast_weather() -> DataFrame:
    df = None
    # Wind
    for weather in weather_stations:
        if df is None:
            df = forecast_wind_at(weather['lat'], weather['lon'], weather['id'])
            df = df.set_index('date')
        else:
            df_part = forecast_wind_at(weather['lat'], weather['lon'], weather['id'])
            df_part = df_part.set_index('date')
            df = df.combine_first(df_part)

    df = df.reset_index()

    # Clouds
    df_clouds = forecast_clouds('Denmark,dk')

    # Solar insolation
    df_clouds = apply_solar_insolation(df_clouds)
    df = core.crosscutting.combine(df, df_clouds)

    # Temperature
    df_temperature = forecast_temperature('Denmark,dk')
    df = core.crosscutting.combine(df, df_temperature)

    return df

def prepare_data(df: DataFrame) -> DataFrame:
    hours_per_day = 24
    hours_per_year = 365.2425 * hours_per_day
    tzinfos =  {"CET": gettz("Europe/Copenhagen")}
    start_date = parser.parse(FETCH_FROM_DATE + ' 00:00:00 CET', tzinfos=tzinfos)
    start_date = start_date.astimezone(None)

    df['dt'] = (df['date'] - start_date) / np.timedelta64(1, 'h')
    df['DSin'] = np.sin(df['dt'] * (2 * np.pi / hours_per_day))
    df['DCos'] = np.cos(df['dt'] * (2 * np.pi / hours_per_day))
    df['YSin'] = np.sin(df['dt'] * (2 * np.pi / hours_per_year))
    df['YCos'] = np.cos(df['dt'] * (2 * np.pi / hours_per_year))

    df = df.interpolate(method='bfill')
    for column in df.columns:
        if column in cfg().NON_FORECASTED_FEATURES:
            df[column] = df[column].interpolate(method='ffill')

    return df


def Data():
    df_el_prices = import_el_prices()
    df_gas_price = import_gas_prices()
    df_coal_price = import_coal_prices()
    df_winds = import_winds()
    df_clouds = import_clouds()
    df_clouds = apply_solar_insolation(df_clouds)
    df_temperature = import_temperature()
    df_water = import_reservoir_pct()

    data_frames = [df_el_prices, df_gas_price, df_water, df_winds, df_clouds, df_temperature, df_coal_price]
    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['date'], how='outer'), data_frames)
    df_merged = prepare_data(df_merged)
    df_merged = df_merged.dropna()

    if cfg().ONLY_RECENT_DATA:
        tzinfos =  {"CET": gettz("Europe/Copenhagen")}
        start_date = parser.parse('2021-08-01 00:00:00 CET', tzinfos=tzinfos)
        df_merged = df_merged.loc[df_merged['date'] >= start_date.strftime('%Y-%m-%dT%H:00:00Z')]

    df_merged.describe()
    return df_merged


def forecast_el_prices():
    now_time = datetime.utcnow().strftime('%Y-%m-%dT%H:00')

    url = "https://api.energidataservice.dk/dataset/Elspotprices"
    data = {
        'start': now_time,
        'columns': 'HourUTC,SpotPriceDKK,PriceArea',
        'filter': '{"PriceArea": "DK1,DK2"}',
        'timezone': 'utc'
    }
    headers = {'charset': 'utf-8'}
    response = requests.get(url=url, params=data, headers=headers)
    if response.status_code < 200 or response.status_code > 299:
        return {'error': response.text}

    df = DataFrame(pd.json_normalize(response.json()['records']))
    result = core.adapters.energidataservice(df)

    return result


def refresh_data():
    global FROM_FILE
    FROM_FILE = False
    try:
        Data()
    finally:
        FROM_FILE = True


def test():
    global FROM_FILE
    FROM_FILE = False
    print(import_el_prices())


if __name__ == '__main__':
    test()