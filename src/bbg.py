import json
import numpy
import pandas as pd
import requests
from numpy import double
import re
import pygsheets


def fetch_exchange_rates(start_date, end_date, currencies_list):
    """
       fetch_exchange_rates fetches exhange rates from API  (api.apilayer.com/currency_data)
       for given dates and currencies list.

       :param start_date: date from fetch exchange rates should be fetched
       :param end_date: date till fetch exchange rates should be fetched
       :param currencies_list: comma separated currencies symbols for which exchangerates should be fetched from given  \
            base currency. Example  "GBP,EUR,CZK"
       :return: this function return api response as a json object
    """

    # Load config file
    config = open('Config/config.json')
    config_data = json.loads(config.read())

    # get API key from config
    api_key = config_data["API_KEY"]

    # get base currency from config
    base_currency = config_data["baseCurrency"]

    headers = {
        "apikey": api_key
    }

    # insert variable values to exchange rate end point
    exchange_rate_endpoint = config_data["ExchangeRateEndPoint"].format(start_date, end_date, base_currency,
                                                                        currencies_list)

    # send request to API
    response = requests.request('GET', exchange_rate_endpoint, headers=headers)

    return response.json()


def generate_currency_data(json_data):
    """
        generate_currency_data converts currency data provided by API response to dataframe.

       :param json_data: currencies json object
       :return: dataframe with currencies data. columns = [date, currency, conversion_rate]
    """
    # Load config file
    config = open('Config/config.json')
    config_data = json.loads(config.read())

    # convert dictionary to dataframe
    df = pd.DataFrame.from_dict({(i, j): json_data[i][j]
                                 for i in json_data.keys()
                                 for j in json_data[i].keys()},
                                orient='index')

    df = df.reset_index()

    # add EUR to EUR conversion rate
    df['EUR'] = 1.0

    # change column names
    df.columns = ['Key', 'Date', 'HUF', 'CZK', 'GBP', 'EUR']

    # drop Key column as it is not required
    df = df.drop('Key', axis=1)

    # melt data frame to all currencies in one column and conversion rates in other according to dates.
    currency_data = df.melt(id_vars=['Date'])
    currency_data.columns = ['date', 'currency', 'conversion_rate']
    currency_data['base_currency'] = config_data['baseCurrency']

    return currency_data


def transform_data(orders, shops_mapping, cost_data, currency_data):
    """
        :param orders: orders dataframe from orders-data excel file.
        :param shops_mapping: IDs csv file dataframe
        :param cost_data: cost dataframe from cost-data excel file.
        :param currency_data: currency dataframe

        :return: transformed dataframe with cost, orders and shop mapping data.
    """

    # Rename column names for IDs dataframe to make joins easier
    shops_mapping.columns = ['shop_id', 'shop_name', 'currency']

    # Create country column from shop name
    shops_mapping['country'] = shops_mapping['shop_name'].str.upper().str[-2:]

    # Convert order date to date format yyy-MM-dd
    orders['order_date'] = pd.to_datetime(orders.order_date).dt.date

    # Rename column names for IDs dataframe to make joins easier
    cost_data.columns = ['date', 'shop_id', 'adv_cost_local_currency']

    # Convert date to date format yyy-MM-dd
    cost_data['date'] = pd.to_datetime(cost_data.date).dt.date

    # Join cost data with shop mappings to get country and local currency
    cost_data = pd.merge(cost_data, shops_mapping, how='inner', on='shop_id')

    # Convert date to date type for joins
    cost_data.date = pd.to_datetime(cost_data.date).dt.date
    currency_data.date = pd.to_datetime(currency_data.date).dt.date

    # Join cost data with exchange rates data to get exchange rates for each day to convert advertisement cost in EUR.
    cost_data = pd.merge(cost_data, currency_data, how='left', on=['date', 'currency'])
    cost_data['adv_cost_eur'] = pd.to_numeric(cost_data['adv_cost_local_currency'] / cost_data['conversion_rate'])
    cost_data['adv_cost_eur'] = cost_data['adv_cost_eur'].astype(double)

    # Clean discount_currency column remove spaces and special characters
    orders['discount_currency'] = orders['discount_currency'].replace(r'[^\w\s]|_', '', regex=True)
    orders['discount_currency'] = orders['discount_currency'].apply( lambda x: re.sub(r"\s+", "", str(x), flags=re.UNICODE))
    orders['discount_currency'] = orders['discount_currency'].str.upper().str.replace("EURO", "EUR")

    # Join orders data with shop mappings to get country and local currency
    orders = pd.merge(orders, shops_mapping, how='inner', on='shop_id')

    # Replace "Local" keyword with actual local currency symbols
    orders['discount_currency'] = numpy.where(orders['discount_currency'] == "LOCAL", orders['currency'],
                                              orders['discount_currency'])

    # Rename column name to make joins easier
    orders.columns = orders.columns.str.replace('order_date', 'date')

    # Convert date to date type for joins
    orders.date = pd.to_datetime(orders.date).dt.date

    # Join orders data with currency data on date and currency to get exchange rates.
    orders = pd.merge(orders, currency_data, how='inner', on=['date', 'currency'])

    # Convert discount in EUR where required
    orders['discount_eur'] = numpy.where(orders['discount_currency'] == "EUR", orders['discount'],
                                         orders['discount'] / orders['conversion_rate'])

    # get relevant columns from cost data
    cost_df_tmep = cost_data[['date', 'shop_id', 'adv_cost_local_currency', 'adv_cost_eur']].copy()

    # Join orders data with cost data on date and shop_id to get advertisement costs.
    orders = pd.merge(orders, cost_df_tmep, how='inner', on=['date', 'shop_id'])

    # Drop unwanted columns
    orders = orders.drop(['year', 'month', 'day', 'base_currency'], axis=1)

    # calculate total revenue revenue_before_discount - discount_eur
    orders['total_revenue'] = orders['revenue_before_discount (in euro)'] + orders['discount_eur']

    return orders


def calculate_kpis(orders):
    """
        :param orders: tranformed data frame with cost and orders information.

        :return: dataframe with calculate KPIs. Exchange rate, Total revenue, Revenue by cotegory, CRR .
    """

    # calculate total revenue for each country on daily basis
    kpi_df = orders.groupby(['date', 'country'], as_index=False).agg(
        {'total_revenue': 'sum', 'conversion_rate': 'max', 'adv_cost_eur': 'max'})

    # calculate CRR
    kpi_df['crr'] = (kpi_df['adv_cost_eur'] / kpi_df['total_revenue']) * 100

    # calculate revenue by category on daily basis
    revenue_by_category = orders.groupby(['date', 'country', 'product_category'], as_index=False) \
        .agg({'total_revenue': 'sum', 'conversion_rate': 'max'})

    # calculate KPIs dataframe to merge all KPIs in one dataframe
    kpi_df = pd.merge(revenue_by_category, kpi_df, how='inner', on=['date', 'country'])

    # Drop unwanted columns
    kpi_df = kpi_df.drop(['conversion_rate_y', 'adv_cost_eur'], axis=1)

    # Rename columns
    kpi_df.columns = ['date', 'country', 'product_category', 'total_revenue_by_category', 'exchange_rate',
                      'total_revenue', 'crr']

    # Calculate revenue share per category after discount
    kpi_df['revenue_share_per_category_after_discount'] = kpi_df['total_revenue_by_category'] / kpi_df[
        'total_revenue'] * 100

    return kpi_df


def write_data_to_gsheets(df_final):
    """
        :param df_final: final dataframe with KPIs data to be written in google sheets.
    """

    # Read config file
    config = open('Config/config.json', "r")
    config_data = json.loads(config.read())

    try:
        # Authenticate with google sheets API
        gc = pygsheets.authorize(service_file=config_data['google_credentials'])

        # open google sheet
        sh = gc.open(config_data['GoogleSheetName'])

        # select the work sheet by name
        wks = sh.worksheet_by_title(config_data['GoogleWorksheet'])

        # update the first sheet with df, starting at cell B2.
        wks.set_dataframe(df_final, (1, 1))
    except:
        print("Fail to write in google sheets!")