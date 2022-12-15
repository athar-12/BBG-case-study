import bbg
import pandas as pd
import json


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    symbols = ""
    pd.set_option('display.float_format', '{:.2f}'.format)

    print("Reading Config.....\n")

    # Read config file
    file = open('./Config/config.json', "r")
    data = json.loads(file.read())

    print("Reading Input Files.......\n")

    # Load IDs data in dataframe
    ids_df = pd.read_csv(data['IdFilePath'], header=0, sep=';')

    # Load orders data in dataframe
    orders_df = pd.read_excel(data["OrderDataFilePath"], header=0)

    # Load cost data in dataframe
    cost_df = pd.read_excel(data['CostDataFilePath'], header=0)

    # fetch start date and end date to fetch exchange rates
    from_date = orders_df['order_date'].dt.date.min()
    to_date = orders_df['order_date'].dt.date.max()

    # get list of different currencies for which exchange rates are required
    currencies = ids_df["Local Currency Abbreviation"].unique().tolist()

    # convert list to string
    currencies_str = ','.join(currencies)

    print("Fetching exchange rates from API.......\n")

    # get exhanges rates
    currency_json = bbg.fetch_exchange_rates(from_date, to_date, currencies_str)

    # add key, value to dictionary
    currency_dict = {"qoutes": currency_json['quotes']}

    print("Generating currency rate dataframe.......\n")

    # convert currency dictionary to dataframe
    currency_df = bbg.generate_currency_data(currency_dict)

    print("Transforming data.......\n")

    # apply transformations on data
    orders_transformed = bbg.transform_data(orders_df, ids_df, cost_df, currency_df)

    print("calculating KPIs.......\n")

    # calculate all required KPIs
    kpis = bbg.calculate_kpis(orders_transformed)

    # dump data to google sheets
    try:
        print("Writing data to google sheets.......\n")
        bbg.write_data_to_gsheets(kpis)
        print("Data dumper to google sheets!\n")
    except:
        print("Fail to write in google sheets!\n")
