import json
import numpy
import pandas as pd
import requests
from numpy import double
import re
import pygsheets


def fetch_exchange_rates(start_date, end_date, currencies_list):
    config = open('Config/config.json')
    config_data = json.loads(config.read())

    api_key = config_data["API_KEY"]
    base_currency = config_data["baseCurrency"]

    headers = {
        "apikey": api_key
    }

    exchange_rate_endpoint = config_data["ExchangeRateEndPoint"].format(start_date, end_date, base_currency, currencies_list)

    response = requests.request('GET', exchange_rate_endpoint, headers=headers)
    return response.json()


def generate_currency_data(json_data):
    df = pd.DataFrame.from_dict({(i, j): json_data[i][j]
                                 for i in json_data.keys()
                                 for j in json_data[i].keys()},
                                orient='index')

    df = df.reset_index()
    df['EUR'] = 1.0
    print(df)

    df.columns = ['Key', 'Date', 'HUF', 'CZK', 'GBP', 'EUR']

    df = df.drop('Key', axis=1)

    currency_data = df.melt(id_vars=['Date'])
    currency_data.columns = ['date', 'currency', 'conversion_rate']
    currency_data['base_currency'] = data['baseCurrency']

    return currency_data


def transform_data(orders, shops_mapping, cost_data, currency_data):
    shops_mapping.columns = ['shop_id', 'shop_name', 'currency']
    shops_mapping['country'] = shops_mapping['shop_name'].str.upper().str[-2:]

    orders['order_date'] = pd.to_datetime(orders.order_date).dt.date

    cost_data.columns = ['date', 'shop_id', 'adv_cost_local_currency']
    cost_data['date'] = pd.to_datetime(cost_data.date).dt.date

    cost_data = pd.merge(cost_data, shops_mapping, how='inner', on='shop_id')

    cost_data.date = pd.to_datetime(cost_data.date).dt.date
    currency_data.date = pd.to_datetime(currency_data.date).dt.date

    cost_data = pd.merge(cost_data, currency_data, how='left', on=['date', 'currency'])
    cost_data['adv_cost_eur'] = pd.to_numeric(cost_data['adv_cost_local_currency'] / cost_data['conversion_rate'])
    cost_data['adv_cost_eur'] = cost_data['adv_cost_eur'].astype(double)

    orders['discount_currency'] = orders['discount_currency'].replace(r'[^\w\s]|_', '', regex=True)
    orders['discount_currency'] = orders['discount_currency'].apply(lambda x: re.sub(r"\s+", "", str(x), flags=re.UNICODE))
    orders['discount_currency'] = orders['discount_currency'].str.upper().str.replace("EURO", "EUR")

    orders = pd.merge(orders, shops_mapping, how='inner', on='shop_id')
    orders['discount_currency'] = numpy.where(orders['discount_currency'] == "LOCAL", orders['currency'],
                                              orders['discount_currency'])

    orders.columns = orders.columns.str.replace('order_date', 'date')
    orders.date = pd.to_datetime(orders.date).dt.date
    orders = pd.merge(orders, currency_data, how='inner', on=['date', 'currency'])

    orders['discount_eur'] = numpy.where(orders['discount_currency'] == "EUR", orders['discount'],
                                         orders['discount'] / orders['conversion_rate'])

    cost_df_tmep = cost_data[['date', 'shop_id', 'adv_cost_local_currency', 'adv_cost_eur']].copy()
    orders = pd.merge(orders, cost_df_tmep, how='inner', on=['date', 'shop_id'])
    orders = orders.drop(['year', 'month', 'day', 'base_currency'], axis=1)
    orders['total_revenue'] = orders['revenue_before_discount (in euro)'] + orders['discount_eur']

    return orders


def calculate_kpis(orders):
    kpi_df = orders.groupby(['date', 'country'], as_index=False).agg(
        {'total_revenue': 'sum', 'conversion_rate': 'max', 'adv_cost_eur': 'max'})

    kpi_df['crr'] = (kpi_df['adv_cost_eur'] / kpi_df['total_revenue']) * 100

    revenue_by_category = orders.groupby(['date', 'country', 'product_category'], as_index=False) \
        .agg({'total_revenue': 'sum', 'conversion_rate': 'max'})

    kpi_df = pd.merge(revenue_by_category, kpi_df, how='inner', on=['date', 'country'])

    kpi_df = kpi_df.drop(['conversion_rate_y', 'adv_cost_eur'], axis=1)

    kpi_df.columns = ['date', 'country', 'product_category', 'total_revenue_by_category', 'exchange_rate',
                      'total_revenue', 'crr']

    kpi_df['revenue_share_per_category_after_discount'] = kpi_df['total_revenue_by_category'] / kpi_df[
        'total_revenue'] * 100

    return kpi_df


def write_data_to_gsheets(df_final):
    config = open('Config/config.json', "r")
    config_data = json.loads(config.read())
    try:
        gc = pygsheets.authorize(service_file=config_data['google_credentials'])
        sh = gc.open(config_data['GoogleSheetName'])
        # select the first sheet
        wks = sh.worksheet_by_title(config_data['GoogleWorksheet'])
        # update the first sheet with df, starting at cell B2.
        wks.set_dataframe(df_final, (1, 1))
    except:
        print("Fail to write in google sheets!")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    symbols = ""
    pd.set_option('display.float_format', '{:.2f}'.format)

    print("Reading Config.....\n")

    file = open('Config/config.json', "r")
    data = json.loads(file.read())

    print("Reading Input Files.......\n")

    ids_df = pd.read_csv(data['IdFilePath'], header=0, sep=';')
    orders_df = pd.read_excel(data["OrderDataFilePath"], header=0)
    cost_df = pd.read_excel(data['CostDataFilePath'], header=0)
    from_date = orders_df['order_date'].dt.date.min()
    to_date = orders_df['order_date'].dt.date.max()

    currencies = ids_df["Local Currency Abbreviation"].unique().tolist()
    currencies_str = ','.join(currencies)

    print("Fetching exchange rates from API.......\n")
    currency_json = fetch_exchange_rates(from_date, to_date, currencies_str)

    currency_dict = {"qoutes": currency_json['quotes']}
    print("Generating currency rate dataframe.......\n")
    currency_df = generate_currency_data(currency_dict)

    print("Transforming data.......\n")
    orders_transformed = transform_data(orders_df, ids_df, cost_df, currency_df)

    print("calculating KPIs.......\n")
    kpis = calculate_kpis(orders_transformed)

    try:
        print("Writing data to google sheets.......\n")
        write_data_to_gsheets(kpis)
        print("Data dumper to google sheets!\n")
    except:
        print("Fail to write in google sheets!\n")
