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
 
 | timestamp           |       Size |     GrossValue |   Total_BTC |   Total_USD |   ContractsTraded_Size |   ContractsTraded_GrossValue |   BearTransacts |   BullTransacts |   WarTransacts |   TotalTransacts |   Price_exp |   LogReturns |   High |    Low |   Open |   Close |
|:--------------------|-----------:|---------------:|------------:|------------:|-----------------------:|-----------------------------:|----------------:|----------------:|---------------:|-----------------:|------------:|-------------:|-------:|-------:|-------:|--------:|
| 2018-08-09 04:00:00 |  387010569 |  6142817347210 |     61428.2 | 3.87011e+08 |            2.17375e+07 |                  3.44337e+11 |           36181 |           44281 |           8100 |            80462 |     6280.39 |  0           | 6329.5 | 6260   | 6273.5 |  6320   |
| 2018-08-09 08:00:00 |  438354040 |  6920371016908 |     69203.7 | 4.38354e+08 |            2.20478e+07 |                  3.46379e+11 |           42131 |           46161 |           4030 |            88292 |     6302.3  |  0.00348287  | 6390   | 6290   | 6320   |  6316.5 |
| 2018-08-09 12:00:00 |  314733664 |  4981585266840 |     49815.9 | 3.14734e+08 |           -1.61104e+06 |                 -2.70579e+10 |           35096 |           33561 |          -1535 |            68657 |     6312.82 |  0.00166856  | 6352   | 6273.5 | 6316   |  6320   |
| 2018-08-09 16:00:00 | 1403867727 | 22065437110463 |    220654   | 1.40387e+09 |            2.75257e+07 |                  3.95259e+11 |          106490 |          110371 |           3881 |           216861 |     6314.68 |  0.000293317 | 6550   | 6182   | 6320   |  6493.5 |
 
`Timestamp:` Timerange the data is grouped and when.

`Size`: Amount of contracts traded during the selected frequency (absolute terms).

`GrossValue`: Amount of Satoshis the trades were worth during the selected frequency (absolute terms). 

`Total_BTC`: Amount of BTC the trades were worth during the selected frequency (absolute terms). 

`Total_USD`: Amount of USD the trades were worth during the selected frequency (absolute terms). 

`ContractsTraded_Size`: Total value in dollars of the contracts traded during the selected frequency in USD (shorts vs longs).

`ContractsTraded_GrossValue`: Total value in satoshis of the contracts traded during the selected frequency in USD (shorts vs longs).

`BearTransacts`: Number of bearish (shorts) transactions during the period.

`BullTransacts`: Number of bullish (longs) transactions during the period.

`WarTransacts`: Bullish minus bearish transactions.

`TotalTransacts`: Total number of transactions done during the period (absolute terms.).

`Price_exp`: Price smoothed with an exponential moving average of 'i' periods.

`LogReturns`: Logarithmic returns from one period to another.

`High`: Highest price reached within the period. 

`Low`: Lowest price reached within the period. 

`Open`: Opening price for the selected period. 

`Close`: Closing price for the selected period. 



### Notes
Depending on the crypto, there might be a lot of data to collect, remember is data from the order book. Therefore, the script will need time and you might run into memory issues stopping the script. 

Feel free to reach me in case you need some help.

 
### Files

**data:** Where the collected/processed data will be stored. You will save one .csv file per date in the desired frequency. The path should be in the previous location from the script.

**source:** Folder containing the magic. 

**mainCSV:** Executable script to collect all the data from the orderbook starting at a specific date. We will carry on with some feature engineering and store it into a csv compressed as gzip file. When executing, we will only need to specify the cryptocurrency we are interested in (Bitcoin by default) and the date (by default is the first available.). 

**mainDB:** Executable script to collect all **raw** data from the orderbook starting at a specific date and automatically store it in a mongoDB (in my case I named it "xbt". When executing, we will only need to follow the steps according to our intentions: 
    
    1. Update database.
    2. Collect RAW data.
    3. Collect PROCESSED data.
    4. Delete collection.
    5. Check warnings (empty files.).


### Part II
https://github.com/aiborra11/BTC-Manipulations



********
