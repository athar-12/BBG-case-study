# BBG-case-study

## Description

An ETL pipeline to fetch exchange rates data from API, transform data to calculate required KPIs and dump it into google sheets.

##   Technology: Python 

## Required Packages
The required packages are mentioned in requirements.txt file.

### Github link
```
https://github.com/athar-12/BBG-case-study.git
```

##   Config
There is config.json file in Config folder which should be adjusted before running this application.
Path: /Config/config.json
```json
{
  "API_KEY": "DvBiBi07sN00ThDUssam0XLlcWUju3yo",
  "IdFilePath": "/Users/atharabbas/Downloads/IDs.csv",
  "CostDataFilePath": "/Users/atharabbas/Downloads/Cost-Data.xlsx",
  "OrderDataFilePath": "/Users/atharabbas/Downloads/Order-Data.xlsx",
  "baseCurrency": "EUR",
  "ExchangeRateEndPoint": "https://api.apilayer.com/currency_data/timeframe?start_date={}&end_date={}&source={}&currencies={}",
  "google_credentials": "/Users/atharabbas/Downloads/neon-infinity-335913-bd26ef908646.json",
  "GoogleSheetName": "BBG",
  "GoogleWorksheet": "BBG-case-study"
}

```
## STEPS
1. Load cost, order and IDs data.
2. Fetch exchange rates for currencies for given date range.
3. generate currency rate dataframe
4. Apply required transformations on data.
5. Calculate KPIs
6. Write data to google sheets

## Methods

#### Fetch exchange rates
This method  fetches exchange rates from API  (api.apilayer.com/currency_data) for given dates and currencies list.
```http
 fetch_exchange_rates(start_date, end_date, currencies_list)
```

| Parameter | Type     | Description                                                                                                                  |
| :-------- |:---------|:-----------------------------------------------------------------------------------------------------------------------------|
| `start_date` | `DATE`   | date from fetch exchange rates should be fetched                                                                             |
| `end_date` | `DATE`   | date till fetch exchange rates should be fetched                                                                             |
| `currencies_list` | `String` | comma separated currencies symbols for which exchangerates should be fetched from given base currency. Example  "GBP,EUR,CZK"|

#### Generate Currecny Data
This method converts currency data provided by API response to dataframe.

```http
 generate_currency_data(json_data)
```

| Parameter | Type     | Description                       |
| :-------- |:---------| :-------------------------------- |
| `json_data`      | `dict`   | **Required**. currencies json object |


#### Transform 

This method cleans data set and apply required transformations.

```http
 transform_data(orders, shops_mapping, cost_data, currency_data)
```

| Parameter | Type        | Description                       |
| :-------- |:------------| :-------------------------------- |
| `orders`      | `Dataframe` | **Required**. orders dataframe from orders-data excel file. |
| `shops_mapping`      | `Dataframe`       | **Required**. IDs csv file dataframe |
| `cost_data`      | `Dataframe`       | **Required**. cost dataframe from cost-data excel file. |
| `currency_data`      | `Dataframe`       | **Required**. currency dataframe |

#### Calculate KPIs 

This method calculate required KPIs.

```http
 calculate_kpis(orders)
```
| Parameter | Type        | Description                                                            |
| :-------- |:------------|:-----------------------------------------------------------------------|
| `orders`      | `Dataframe` | **Required**. transformed data frame with cost and orders information. |

#### Right to Google Sheets

This method calculate required KPIs.

```http
 write_data_to_gsheets(df_final)
```
| Parameter | Type        | Description                                                            |
| :-------- |:------------|:-----------------------------------------------------------------------|
| `df_final`      | `Dataframe` | **Required**. final dataframe with KPIs data to be written in google sheets. |
