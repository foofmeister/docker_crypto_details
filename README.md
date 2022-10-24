# pull_crypto_details

##This is for creating a docker mysql image and then ingesting crypto details using free coingeck api into a mysql database

###This will create a table called: DB.Time_Data that has all of the pertinent details for cryptos that you specify.  It's currently configured only to look at special coins and all solana tokens.  It grabs current time, usd value, usd market value, and usd 24 trading

## Build Docker Image

* Start Docker Desktop

* Build Image in CLI

```shell
docker build -t local-mysql .
```

* Run Container

```shell
docker run -dp 3306:3306 local-mysql
```
##Ensure you have python requirements
 
* Depending on what version of python you are using, you might need to use pip3


```shell
pip install -r requirements.txt
```

##Run Python
 
* builds DB.eligible_coins
* builds DB.coin_list 


```shell
python crtbl_coin_list.py 
```
