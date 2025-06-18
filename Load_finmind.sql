CREATE DATABASE finmind_tw;
USE finmind_tw;
# adjprice_to20250227
CREATE TABLE adjprice_to20250227(
	date DATE,
    stock_id VARCHAR(20),
    Trading_Volume BIGINT UNSIGNED,
    Trading_money BIGINT UNSIGNED,
    open DOUBLE,
    max DOUBLE,
    min DOUBLE,
    close DOUBLE,
    spread DOUBLE,
    Trading_turnover INT UNSIGNED);
LOAD DATA INFILE 'C:/Users/e1155_l2c4ye3/Desktop/finmind_data/adjprice_to20250227.csv'
INTO TABLE adjprice_to20250227
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# adjprice_from20250102
CREATE TABLE adjprice_from20250102(
	date DATE,
    stock_id VARCHAR(20),
    Trading_Volume BIGINT UNSIGNED,
    Trading_money BIGINT UNSIGNED,
    open DOUBLE,
    max DOUBLE,
    min DOUBLE,
    close DOUBLE,
    spread DOUBLE,
    Trading_turnover INT UNSIGNED);
LOAD DATA INFILE 'C:/Users/e1155_l2c4ye3/Desktop/finmind_data/adjprice_from20250102.csv'
INTO TABLE adjprice_from20250102
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# monthret_to202503, replace 123 with NULL
CREATE TABLE monthret_to202503(
	stock_id varchar(20),
    month DATE,
    ret DOUBLE);
LOAD DATA INFILE 'C:/Users/e1155_l2c4ye3/Desktop/finmind_data/monthret_to202503.csv'
INTO TABLE monthret_to202503
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
UPDATE monthret_to202503 SET ret = NULL WHERE ret = 123;

# dayret_to20250402, replace 123 with NULL
CREATE TABLE dayret_to20250402(
	stock_id VARCHAR(20),
    date DATE,
    ret DOUBLE);
LOAD DATA INFILE 'C:/Users/e1155_l2c4ye3/Desktop/finmind_data/dayret_to20250402.csv'
INTO TABLE dayret_to20250402
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
UPDATE dayret_to20250402 SET ret = NULL WHERE ret = 123;

# marketcap
CREATE TABLE marketcap(
	date DATE,
    stock_id VARCHAR(20),
    market_value BIGINT);
LOAD DATA INFILE 'C:/Users/e1155_l2c4ye3/Desktop/finmind_data/marketcap.csv'
INTO TABLE marketcap
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# balancesheet
CREATE TABLE balancesheet(
	date DATE,
    stock_id VARCHAR(20),
    type VARCHAR(100),
    value DOUBLE,
    origin_name VARCHAR(50));
LOAD DATA INFILE 'C:/Users/e1155_l2c4ye3/Desktop/finmind_data/balancesheet.csv'
INTO TABLE balancesheet
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# incomestatement
CREATE TABLE incomestatement(
	date DATE,
    stock_id VARCHAR(20),
    type VARCHAR(100),
    value DOUBLE,
    origin_name VARCHAR(50));
LOAD DATA INFILE 'C:/Users/e1155_l2c4ye3/Desktop/finmind_data/incomestatement.csv'
INTO TABLE incomestatement
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# taiex
CREATE TABLE taiex(
	date DATE,
    stock_id VARCHAR(10),
    price DOUBLE);
LOAD DATA INFILE 'C:/Users/e1155_l2c4ye3/Desktop/finmind_data/taiex.csv'
INTO TABLE taiex
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



