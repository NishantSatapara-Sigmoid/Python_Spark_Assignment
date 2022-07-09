from Stock_Name import get_stock
import json
import requests
import pandas as pd

## stocks= get_stock()

stocks = ['AAN', 'AAON', 'AAT', 'AAWW', 'ABCB', 'ABG', 'ABM', 'ABTX', 'ACA', 'ACLS', 'ADC', 'ADTN', 'ADUS', 'AEIS', 'AEL', 'AGO', 'AGYS', 'AHH', 'AIN', 'AIR', 'AIT', 'AJRD', 'AKR', 'ALEX', 'ALG', 'ALGT', 'ALRM', 'AMBC', 'AMCX', 'AMEH', 'AMN', 'AMPH', 'AMSF', 'AMWD', 'ANDE', 'ANF', 'ANGO', 'ANIK', 'ANIP', 'AORT', 'AOSL', 'APEI', 'APOG', 'ARCB', 'ARI', 'ARLO', 'ARNC', 'AROC', 'ARR', 'ASIX', 'ASO', 'ASTE', 'ATEN', 'ATGE', 'ATI', 'ATNI', 'AVA', 'AVAV', 'AVD', 'AVNS', 'AWR', 'AX', 'AXL', 'AZZ', 'B', 'BANC', 'BANF', 'BANR', 'BBBY', 'BCC', 'BCOR', 'BCPC', 'BDN', 'BFS', 'BGS', 'BHE', 'BHLB', 'BIG', 'BJRI', 'BKE', 'BKU', 'BLFS', 'BLMN', 'BMI', 'BOOM', 'BOOT', 'BRC', 'BRKL', 'BSIG', 'CAKE', 'CAL', 'CALM', 'CAMP', 'CARA', 'CARS', 'CASH', 'CATO', 'CBU', 'CCOI', 'CCRN']
url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
headers = {
	"X-RapidAPI-Key": "c80d8e612emsha50372cf8510eadp15af9ajsn4c3afd249563",
	"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
}

for i in range(25):
    stock = stocks[i]
    querystring = {"ticker_symbol": stock, "years": "5", "format": "json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = json.loads(response.text)
    ##print(data)
    dataframe = pd.DataFrame(data['historical prices'])
    dataframe.insert(0,"Stock_Name",stock,True)
    file=stock+"_"+str((i+1))+'.csv'
    path='/Users/nishant/Desktop/Python_Spark_Assignment/csv/'+file
    dataframe.to_csv(path)
