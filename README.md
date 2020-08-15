# Crypto Scraper
**The scope of this project was to develop a script that eases the collection of data for a specific cryptocurrency.**

_It allows us to to automatically extract raw data coming from the order book, process it and obtain features that will allow us to understand the behaviour of this cryptocurrency._
 
### Scrapped Raw Data

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

### Processed Data (4H timeframe sample)
 
| Timestamp           |       Size |     GrossValue |   Total_BTC |   Total_USD |   ContractsTraded_Size |   ContractsTraded_GrossValue |   BearTransacts |   BullTransacts |   WarTransacts |   TotalTransacts |   Price_exp |   LogReturns |    Low |   High |
|:--------------------|-----------:|---------------:|------------:|------------:|-----------------------:|-----------------------------:|----------------:|----------------:|---------------:|-----------------:|------------:|-------------:|-------:|-------:|
| 2018-08-08 04:00:00 |  898925380 | 13626717530308 |    136267   | 8.98925e+08 |           -6.98192e+07 |                 -1.05337e+12 |           78499 |           75084 |          -3415 |           153583 |     6660.76 |   0          | 6712   | 6530   |
| 2018-08-08 08:00:00 | 1275264682 | 19665996153763 |    196660   | 1.27526e+09 |           -9.41728e+07 |                 -1.45769e+12 |          104325 |           89888 |         -14437 |           194213 |     6550.94 |  -0.0166241  | 6587.5 | 6375.5 |
| 2018-08-08 12:00:00 |  573375540 |  8848864475447 |     88488.6 | 5.73376e+08 |            2.97127e+07 |                  4.55558e+11 |           55602 |           67695 |          12093 |           123297 |     6501.53 |  -0.00757146 | 6533.5 | 6410   |
| 2018-08-08 16:00:00 |  541165657 |  8364671941273 |     83646.7 | 5.41166e+08 |           -9.5061e+06  |                 -1.479e+11   |           58820 |           57871 |           -949 |           116691 |     6487.62 |  -0.0021414  | 6514   | 6412   |
| 2018-08-08 20:00:00 | 1756631258 | 28033960379162 |    280340   | 1.75663e+09 |           -7.31778e+07 |                 -1.16484e+12 |          138093 |          119938 |         -18155 |           258031 |     6357.44 |  -0.0202705  | 6488   | 6100   |
 


### Notes
Depending on the crypto, there might be a lot of data to collect, remember is data from the order book. Therefore, the script will need time. 

In case the script is not working on your machine, I would reduce the dates intervals and create more csv files to free some memory while processing the data. 

Feel free to reach me in case you need some help.

 
### Files

**data:** You should create a folder named "data" where the collected data will be stored (only if we execute the mainCSV.py). The path should be in the previous location from the script.

**source:** Folder containing the magic. 

**mainCSV:** Executable script to collect all the data from the orderbook starting at a specific date. We will carry on with some feature engineering and store it into a csv compressed as gzip file. When executing, we will only need to specify the cryptocurrency we are interested in (Bitcoin by default) and the date (by default is the first available.). 

**mainDB:** Executable script to collect all **raw** data from the orderbook starting at a specific date and automatically store it in a mongoDB (in my case I named it "xbt". When executing, we will only need to specify the cryptocurrency we are interested in (Bitcoin by default) and the date (by default will be the date of the last time we executed this script.). 


### Part II

https://github.com/aiborra11/BTC-Manipulations

********
