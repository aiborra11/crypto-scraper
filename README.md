# Crypto Scraper
 **Python script to automatically extract data coming from the orderbook for a desired cryptocurrency.**
 
### Output data

|   | timestamp                     | symbol | side | size | price  | tickDirection | trdMatchID                           | grossValue | homeNotional       | foreignNotional |
|---|-------------------------------|--------|------|------|--------|---------------|--------------------------------------|------------|--------------------|-----------------|
| 0 | 2016-05-05D04:50:46.067956000 | XBTUSD | Buy  | 1377 | 447.43 | ZeroPlusTick  | 07b3bf2e-b40f-7c24-6c51-3bd110fec715 | 307758123  | 3.0775810000000003 | 1377.0          |
| 1 | 2016-05-05D04:51:29.799042000 | XBTUSD | Buy  | 100  | 447.41 | MinusTick     | b9a3094b-0aff-5745-179c-038b3c5758db | 22350900   | 0.223509           | 100.0           |
| 2 | 2016-05-05D04:57:51.067311000 | XBTUSD | Buy  | 833  | 447.21 | MinusTick     | 9243015e-bf0a-bb3b-3fa8-bae4a3d0fb91 | 186266297  | 1.862663           | 833.0           |
| 3 | 2016-05-05D04:59:25.038946000 | XBTUSD | Sell | 1541 | 445.94 | MinusTick     | 51b3b292-a973-0c57-108c-acc6c444a66d | 345561545  | 3.4556150000000003 | 1541.0          |
| 4 | 2016-05-05D05:19:25.039453000 | XBTUSD | Sell | 1395 | 446.01 | PlusTick      | b2800914-b327-4983-497d-3ef4b7277c57 | 312772950  | 3.12773            | 1395.0          |

`timestamp:` When the trade happened.

`symbol:` Which is the contract belonging to this row.

`side`: Sell (short) vs Buy (long) position.

`size`: Amount of contracts traded.

`price:`  price at which the transaction was succeeded.

`tickDirection:` "MinusTick":  The trade happened at a lower price than the previous one. "PlusTick" : This trade happened at a higher price than the previous one. "ZeroPlusTick" : The previous trade was PLUSTICK and this one has a price equal or lower than the previous one. "ZeroMinusTick" : The previous trade was MINUSTICK and this one has a price equal or higher than the previous one.

`trdMatchID:` Primary key of this trade.

`grossValue:` How many sathoshis the trade was worth.

`homeNotional:` How many BTC the trade was worth.

`foreignNotional:` How many USD the trade was worth.
 
 
 
### Overview
Depending on the crypto, there might be a lot of data to collect, remember is data from the order book. Therefore, the script will need time. 

In case the script is not working on your machine, I would reduce the dates intervals and create more csv files to free some memory while processing the data. Feel free to reach me in case you need some help.

 
### Files

**data:** You should create a folder named "data" where the collected data will be stored. The path should be in the previous location from the script.

**Source:** Folder containing the scripts. 

**Main:** Executable script to collect all the data. When executing, we will only need to specify the cryptocurrency we are interested in (by default it is Bitcoin) and the date (by default is the first available day providing data). 

**Updmain:** Executable script to update data from a specific date. When executing, we will only need to specify the cryptocurrency we are interested in (by default it is Bitcoin) and the date (by default is the first available day providing data). 

********