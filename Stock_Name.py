
def get_stock():
	import json
	import requests
	url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"
	headers = {
		"X-RapidAPI-Key": "c80d8e612emsha50372cf8510eadp15af9ajsn4c3afd249563",
		"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
	}
	response = requests.request("GET", url, headers=headers)
	## print(response.text)
	stocks = json.loads(response.text)
	stocks = stocks["stocks"][:25]
	print(stocks)
	print(len(stocks))
	return stocks

get_stock()

