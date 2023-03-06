from bmdOilPriceFetch import bmdPriceFetch

data = bmdPriceFetch()
if data is not None:
    outputString = f"The price of WTI is ${data['regularMarketPrice']:.2f}"
    print(outputString)
