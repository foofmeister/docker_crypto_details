DROP DATABASE IF EXISTS `DB`;
CREATE DATABASE `DB`;
USE `DB`;


DROP TABLE IF EXISTS `ARTIST`;
CREATE TABLE `Dummy` (
  `Dummy_column1` datetime,
  `Dummy_column2` varchar(100) NOT NULL
);


CREATE TABLE IF NOT EXISTS `Time_Data` (
`ID` varchar (100) ,
`usd` float,
`usd_market_cap` float,
`usd_24h_vol` float ,
`last_updated_at` numeric);

CREATE TABLE IF NOT EXISTS `Crypto_Year_Price` (
`ID` varchar (100) ,
`SYMBOL` varchar(100),
`NAME` varchar (255) ,
`PLATFORMS` varchar (255)  ,
`PLATFORM_HASH` varchar(255),
`TYPE` varchar(255),
`TIMESTAMP` varchar(255),
`VALUE` float);



