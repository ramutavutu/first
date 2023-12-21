from datetime import datetime, timedelta
from google.cloud import secretmanager_v1

client = secretmanager_v1.SecretManagerServiceClient()
PROJECT_ID = "bkt-nonprod-dev-dwh-svc-00"


# Fetch secrets
token1 = client.access_secret_version("projects/"+PROJECT_ID+"/secrets/airflow-variables-bkt-matomo-api-token1/versions/1").payload.data.decode('UTF-8')
token2 = client.access_secret_version("projects/"+PROJECT_ID+"/secrets/airflow-variables-bkt-matomo-api-token2/versions/1").payload.data.decode('UTF-8')

API_CONFIG = {
    "base_url": "https://bridge2solutions.matomo.cloud",
    "endpoint": "index.php",
}

# now = datetime.now()
# firstOfThisMonth = now.replace(day=1)
# endOfLastMonth = firstOfThisMonth-timedelta(days=1)
# firstOfLastMonth = (endOfLastMonth.replace(day=1)).strftime("%Y-%m-%d")
# endOfLastMonth = endOfLastMonth.strftime("%Y-%m-%d")

# FDLM = firstOfLastMonth
# LDLM = endOfLastMonth

FDLM = "2023-01-01"
LDLM = "2023-01-31"

QUERY_PARAMS = [
       {
        #Delta Merchandise + gift
        "date": f"{FDLM},{LDLM}",
        "expanded": 1,
        "filter_limit": 100,
        "format": "CSV",
        "format_metrics": 1,
        "idSite": 2,
        "language": "en",
        "method": "API.get",
        "module": "API",
        "period": "day",
        "segment": "pageTitle==Merchandise%20Landing,pageTitle==Merchandise%20Search%20Results,pageTitle==Merchandise%20Product%20Details,pageTitle==Recently%20Viewed,pageTitle==Favorites,pageTitle==Products%20Cart,pageTitle==Products%20Checkout,pageTitle==Products%20Order%20Confirmation,pageTitle==Terms%20and%20Conditions,pageTitle==Order%20History,pageTitle==Order%20Details,pageTitle==Gift%20Card%20Landing,pageTitle==Gift%20Card%20Product%20Details,pageTitle==Gift%20Card%20Search%20Results",
        "token_auth": f"{token1}",
        "translateColumnNames": 1
    },
    
    { 
        #FDR_PSCU Merchandise + gift
        "date": f"{FDLM},{LDLM}",
        "expanded": 1,
        "filter_limit": 100,
        "format":"CSV",
        "format_metrics": 1,
        "idSite": 8,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Merchandise%20Landing,pageTitle==Merchandise%20Search%20Results,pageTitle==Merchandise%20Product%20Details,pageTitle==Recently%20Viewed,pageTitle==Favorites,pageTitle==Products%20Cart,pageTitle==Products%20Checkout,pageTitle==Products%20Order%20Confirmation,pageTitle==Terms%20and%20Conditions,pageTitle==Order%20History,pageTitle==Order%20Details,pageTitle==Gift%20Card%20Landing,pageTitle==Gift%20Card%20Product%20Details,pageTitle==Gift%20Card%20Search%20Results",
        "token_auth": f"{token1}",
        "translateColumnNames": 1
    },
    
    {
        #FDR Merchandise + gift
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":7,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Merchandise%20Landing,pageTitle==Merchandise%20Search%20Results,pageTitle==Merchandise%20Product%20Details,pageTitle==Recently%20Viewed,pageTitle==Favorites,pageTitle==Products%20Cart,pageTitle==Products%20Checkout,pageTitle==Products%20Order%20Confirmation,pageTitle==Terms%20and%20Conditions,pageTitle==Order%20History,pageTitle==Order%20Details,pageTitle==Gift%20Card%20Landing,pageTitle==Gift%20Card%20Product%20Details,pageTitle==Gift%20Card%20Search%20Results",
        "token_auth": f"{token1}",
        "translateColumnNames":1
    },

    {
        #WF Merchandise + gift
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":3,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Merchandise%20Landing,pageTitle==Merchandise%20Search%20Results,pageTitle==Merchandise%20Product%20Details,pageTitle==Recently%20Viewed,pageTitle==Favorites,pageTitle==Products%20Cart,pageTitle==Products%20Checkout,pageTitle==Products%20Order%20Confirmation,pageTitle==Terms%20and%20Conditions,pageTitle==Order%20History,pageTitle==Order%20Details,pageTitle==Gift%20Card%20Landing,pageTitle==Gift%20Card%20Product%20Details,pageTitle==Gift%20Card%20Search%20Results",
        "token_auth": f"{token1}",
        "translateColumnNames":1
    },

    {
        #PNC Merchandise + gift
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":9,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Merchandise%20Landing,pageTitle==Merchandise%20Search%20Results,pageTitle==Merchandise%20Product%20Details,pageTitle==Recently%20Viewed,pageTitle==Favorites,pageTitle==Products%20Cart,pageTitle==Products%20Checkout,pageTitle==Products%20Order%20Confirmation,pageTitle==Terms%20and%20Conditions,pageTitle==Order%20History,pageTitle==Order%20Details,pageTitle==Gift%20Card%20Landing,pageTitle==Gift%20Card%20Product%20Details,pageTitle==Gift%20Card%20Search%20Results",
        "token_auth": f"{token1}",
        "translateColumnNames":1
    },

    {
        #FSV Merchandise + gift
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":5,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Merchandise%20Landing,pageTitle==Merchandise%20Search%20Results,pageTitle==Merchandise%20Product%20Details,pageTitle==Recently%20Viewed,pageTitle==Favorites,pageTitle==Products%20Cart,pageTitle==Products%20Checkout,pageTitle==Products%20Order%20Confirmation,pageTitle==Terms%20and%20Conditions,pageTitle==Order%20History,pageTitle==Order%20Details,pageTitle==Gift%20Card%20Landing,pageTitle==Gift%20Card%20Product%20Details,pageTitle==Gift%20Card%20Search%20Results",
        "token_auth": f"{token1}",
        "translateColumnNames":1

    },

    {
        #RBC Merchandise + gift
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":4,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Merchandise%20Landing,pageTitle==Merchandise%20Search%20Results,pageTitle==Merchandise%20Product%20Details,pageTitle==Recently%20Viewed,pageTitle==Favorites,pageTitle==Products%20Cart,pageTitle==Products%20Checkout,pageTitle==Products%20Order%20Confirmation,pageTitle==Terms%20and%20Conditions,pageTitle==Order%20History,pageTitle==Order%20Details,pageTitle==Gift%20Card%20Landing,pageTitle==Gift%20Card%20Product%20Details,pageTitle==Gift%20Card%20Search%20Results",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #ScotiaMerchandise + gift
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":20,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Merchandise%20Landing,pageTitle==Merchandise%20Search%20Results,pageTitle==Merchandise%20Product%20Details,pageTitle==Recently%20Viewed,pageTitle==Favorites,pageTitle==Products%20Cart,pageTitle==Products%20Checkout,pageTitle==Products%20Order%20Confirmation,pageTitle==Terms%20and%20Conditions,pageTitle==Order%20History,pageTitle==Order%20Details,pageTitle==Gift%20Card%20Landing,pageTitle==Gift%20Card%20Product%20Details,pageTitle==Gift%20Card%20Search%20Results",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #FDR Apple
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":7,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Apple%20Browse%20Results,pageTitle==Apple%20Cart,pageTitle==Apple%20Catalog%20Landing,pageTitle==Apple%20Checkout,pageTitle==Apple%20Edit%20Address,pageTitle==Apple%20Error,pageTitle==Apple%20Landing,pageTitle==Apple%20Order%20Confirmation,pageTitle==Apple%20Order%20Details,pageTitle==Apple%20Order%20History,pageTitle==Apple%20Product%20Details,pageTitle==Apple%20Search%20Results,pageTitle==Apple%20Session%20Timeout,pageTitle==Apple%20Terms%20and%20Conditions",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #RBC Apple
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":4,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Apple%20Browse%20Results,pageTitle==Apple%20Cart,pageTitle==Apple%20Catalog%20Landing,pageTitle==Apple%20Checkout,pageTitle==Apple%20Edit%20Address,pageTitle==Apple%20Error,pageTitle==Apple%20Landing,pageTitle==Apple%20Order%20Confirmation,pageTitle==Apple%20Order%20Details,pageTitle==Apple%20Order%20History,pageTitle==Apple%20Product%20Details,pageTitle==Apple%20Search%20Results,pageTitle==Apple%20Session%20Timeout,pageTitle==Apple%20Terms%20and%20Conditions",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #Scotia Apple
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":20,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Apple%20Browse%20Results,pageTitle==Apple%20Cart,pageTitle==Apple%20Catalog%20Landing,pageTitle==Apple%20Checkout,pageTitle==Apple%20Edit%20Address,pageTitle==Apple%20Error,pageTitle==Apple%20Landing,pageTitle==Apple%20Order%20Confirmation,pageTitle==Apple%20Order%20Details,pageTitle==Apple%20Order%20History,pageTitle==Apple%20Product%20Details,pageTitle==Apple%20Search%20Results,pageTitle==Apple%20Session%20Timeout,pageTitle==Apple%20Terms%20and%20Conditions",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #WF Apple
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":3,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Apple%20Browse%20Results,pageTitle==Apple%20Cart,pageTitle==Apple%20Catalog%20Landing,pageTitle==Apple%20Checkout,pageTitle==Apple%20Edit%20Address,pageTitle==Apple%20Error,pageTitle==Apple%20Landing,pageTitle==Apple%20Order%20Confirmation,pageTitle==Apple%20Order%20Details,pageTitle==Apple%20Order%20History,pageTitle==Apple%20Product%20Details,pageTitle==Apple%20Search%20Results,pageTitle==Apple%20Session%20Timeout,pageTitle==Apple%20Terms%20and%20Conditions",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #UA Apple
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":21,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Apple%20Browse%20Results,pageTitle==Apple%20Cart,pageTitle==Apple%20Catalog%20Landing,pageTitle==Apple%20Checkout,pageTitle==Apple%20Edit%20Address,pageTitle==Apple%20Error,pageTitle==Apple%20Landing,pageTitle==Apple%20Order%20Confirmation,pageTitle==Apple%20Order%20Details,pageTitle==Apple%20Order%20History,pageTitle==Apple%20Product%20Details,pageTitle==Apple%20Search%20Results,pageTitle==Apple%20Session%20Timeout,pageTitle==Apple%20Terms%20and%20Conditions",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #PNC Apple
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":9,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Apple%20Browse%20Results,pageTitle==Apple%20Cart,pageTitle==Apple%20Catalog%20Landing,pageTitle==Apple%20Checkout,pageTitle==Apple%20Edit%20Address,pageTitle==Apple%20Error,pageTitle==Apple%20Landing,pageTitle==Apple%20Order%20Confirmation,pageTitle==Apple%20Order%20Details,pageTitle==Apple%20Order%20History,pageTitle==Apple%20Product%20Details,pageTitle==Apple%20Search%20Results,pageTitle==Apple%20Session%20Timeout,pageTitle==Apple%20Terms%20and%20Conditions",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #BAC Travel
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":35,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Hotels%20Landing,pageTitle==Hotels%20Search%20Results,pageTitle==Hotels%20Details,pageTitle==Car%20Rentals%20Landing,pageTitle==Car%20Rentals%20Search%20Results,pageTitle==Car%20Rentals%20Details,pageTitle==Activities%20Landing,pageTitle==Activities%20Search%20Results,pageTitle==Activities%20Details,pageTitle==Cruises%20Landing,pageTitle==Cruises%20Search%20Results,pageTitle==Travel%20Cart,pageTitle==Travel%20Checkout,pageTitle==Travel%20Order%20Confirmation,pageTitle==Cruises%20Details,pageTitle==Cruises%20Cabin%20Selection,pageTitle==Cruises%20Cabin%20Category,pageTitle==Flights%20Landing,pageTitle==Flights%20Search%20Results,pageTitle==Flights%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #FDR_PSCU Travel
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":8,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Hotels%20Landing,pageTitle==Hotels%20Search%20Results,pageTitle==Hotels%20Details,pageTitle==Car%20Rentals%20Landing,pageTitle==Car%20Rentals%20Search%20Results,pageTitle==Car%20Rentals%20Details,pageTitle==Activities%20Landing,pageTitle==Activities%20Search%20Results,pageTitle==Activities%20Details,pageTitle==Cruises%20Landing,pageTitle==Cruises%20Search%20Results,pageTitle==Travel%20Cart,pageTitle==Travel%20Checkout,pageTitle==Travel%20Order%20Confirmation,pageTitle==Cruises%20Details,pageTitle==Cruises%20Cabin%20Selection,pageTitle==Cruises%20Cabin%20Category,pageTitle==Flights%20Landing,pageTitle==Flights%20Search%20Results,pageTitle==Flights%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #FDR Travel
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":7,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Hotels%20Landing,pageTitle==Hotels%20Search%20Results,pageTitle==Hotels%20Details,pageTitle==Car%20Rentals%20Landing,pageTitle==Car%20Rentals%20Search%20Results,pageTitle==Car%20Rentals%20Details,pageTitle==Activities%20Landing,pageTitle==Activities%20Search%20Results,pageTitle==Activities%20Details,pageTitle==Cruises%20Landing,pageTitle==Cruises%20Search%20Results,pageTitle==Travel%20Cart,pageTitle==Travel%20Checkout,pageTitle==Travel%20Order%20Confirmation,pageTitle==Cruises%20Details,pageTitle==Cruises%20Cabin%20Selection,pageTitle==Cruises%20Cabin%20Category,pageTitle==Flights%20Landing,pageTitle==Flights%20Search%20Results,pageTitle==Flights%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #WF Travel
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":3,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Hotels%20Landing,pageTitle==Hotels%20Search%20Results,pageTitle==Hotels%20Details,pageTitle==Car%20Rentals%20Landing,pageTitle==Car%20Rentals%20Search%20Results,pageTitle==Car%20Rentals%20Details,pageTitle==Activities%20Landing,pageTitle==Activities%20Search%20Results,pageTitle==Activities%20Details,pageTitle==Cruises%20Landing,pageTitle==Cruises%20Search%20Results,pageTitle==Travel%20Cart,pageTitle==Travel%20Checkout,pageTitle==Travel%20Order%20Confirmation,pageTitle==Cruises%20Details,pageTitle==Cruises%20Cabin%20Selection,pageTitle==Cruises%20Cabin%20Category,pageTitle==Flights%20Landing,pageTitle==Flights%20Search%20Results,pageTitle==Flights%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #PNC Travel
         "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":9,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Hotels%20Landing,pageTitle==Hotels%20Search%20Results,pageTitle==Hotels%20Details,pageTitle==Car%20Rentals%20Landing,pageTitle==Car%20Rentals%20Search%20Results,pageTitle==Car%20Rentals%20Details,pageTitle==Activities%20Landing,pageTitle==Activities%20Search%20Results,pageTitle==Activities%20Details,pageTitle==Cruises%20Landing,pageTitle==Cruises%20Search%20Results,pageTitle==Travel%20Cart,pageTitle==Travel%20Checkout,pageTitle==Travel%20Order%20Confirmation,pageTitle==Cruises%20Details,pageTitle==Cruises%20Cabin%20Selection,pageTitle==Cruises%20Cabin%20Category,pageTitle==Flights%20Landing,pageTitle==Flights%20Search%20Results,pageTitle==Flights%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #PSG Travel
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":6,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Hotels%20Landing,pageTitle==Hotels%20Search%20Results,pageTitle==Hotels%20Details,pageTitle==Car%20Rentals%20Landing,pageTitle==Car%20Rentals%20Search%20Results,pageTitle==Car%20Rentals%20Details,pageTitle==Activities%20Landing,pageTitle==Activities%20Search%20Results,pageTitle==Activities%20Details,pageTitle==Cruises%20Landing,pageTitle==Cruises%20Search%20Results,pageTitle==Travel%20Cart,pageTitle==Travel%20Checkout,pageTitle==Travel%20Order%20Confirmation,pageTitle==Cruises%20Details,pageTitle==Cruises%20Cabin%20Selection,pageTitle==Cruises%20Cabin%20Category,pageTitle==Flights%20Landing,pageTitle==Flights%20Search%20Results,pageTitle==Flights%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #FSV Travel
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":5,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Hotels%20Landing,pageTitle==Hotels%20Search%20Results,pageTitle==Hotels%20Details,pageTitle==Car%20Rentals%20Landing,pageTitle==Car%20Rentals%20Search%20Results,pageTitle==Car%20Rentals%20Details,pageTitle==Activities%20Landing,pageTitle==Activities%20Search%20Results,pageTitle==Activities%20Details,pageTitle==Cruises%20Landing,pageTitle==Cruises%20Search%20Results,pageTitle==Travel%20Cart,pageTitle==Travel%20Checkout,pageTitle==Travel%20Order%20Confirmation,pageTitle==Cruises%20Details,pageTitle==Cruises%20Cabin%20Selection,pageTitle==Cruises%20Cabin%20Category,pageTitle==Flights%20Landing,pageTitle==Flights%20Search%20Results,pageTitle==Flights%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #FSV EventTickets
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":5,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Event%20Tickets%20Landing,pageTitle==Event%20Tickets%20Search%20Results,pageTitle==Event%20Tickets%20Categories,pageTitle==Event%20Tickets%20Subcategories,pageTitle==Event%20Tickets%20Product%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #FDR_PSCU CustomStore
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":8,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Custom%20Store%20Search%20Results,pageTitle==Custom%20Store%20Product%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #FDR CustomStore
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":7,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Custom%20Store%20Search%20Results,pageTitle==Custom%20Store%20Product%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #FSV CustomStore
        "date":f"{FDLM},{LDLM}",
        "expanded":1,
        "filter_limit":100,
        "format":"CSV",
        "format_metrics":1,
        "idSite":5,
        "language":"en",
        "method":"API.get",
        "module":"API",
        "period":"day",
        "segment":"pageTitle==Custom%20Store%20Search%20Results,pageTitle==Custom%20Store%20Product%20Details",
        "token_auth": f"{token2}",
        "translateColumnNames":1
    },

    {
        #CHASE
        "module":"API",
        "format":"CSV",
        "idSite":15,
        "period":"day",
        "date":f"{FDLM},{LDLM}",
        "method":"API.get",
        "filter_limit":-1,
        "format_metrics":1,
        "expanded":1,
        "translateColumnNames":1,
        "language":"en",
        "segment":"",
        "token_auth": f"{token2}"
    }
]



