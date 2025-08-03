
/*

usage (from .../cbor/ directory): 
mysql -u borarchAdmin -pkBu9pjz2vi < ./script/init/8.ddl-borarch-tables.sql

run on the docker bor-db container:
cat ./script/init/8.ddl-borarch-tables.sql | docker exec -i bor-db mysql -u borAllAdmin -pkBu9pjz2vi borarch

TODO 


yyyymmdd   user          description
--------   ------------  ------------------------------------------------------------
20250513   RMenning      added User table
20250114   RMenning      added MellonHoldingsStaging table for CSV import
20250115   RMenning      added DurationReportStaging and DurationReport tables for duration report CSV import

*/

use borarch;

-- Create the fund_data table
drop table if exists FundClassFee;
CREATE TABLE FundClassFee (
  id INT AUTO_INCREMENT PRIMARY KEY,
  FundCode VARCHAR(10) NOT NULL,
  FundName VARCHAR(100) NOT NULL,
  Class VARCHAR(10) NOT NULL,
  Description VARCHAR(50) NOT NULL,
  Mer DECIMAL(5,2),
  Trailer DECIMAL(5,2),
  PerformanceFee VARCHAR(50),
  MinInvestmentInitial VARCHAR(20),
  MinInvestmentSubsequent VARCHAR(20),
  Currency VARCHAR(3) NOT NULL,
  UNIQUE KEY unique_fund_class (FundCode, Class)
);

-- Create the MellonRawStaging table for raw CSV data (all VARCHAR fields)
DROP TABLE IF EXISTS MellonRawStaging;

CREATE TABLE MellonRawStaging (
  id INT AUTO_INCREMENT PRIMARY KEY,
  AccountNumber VARCHAR(20),
  AccountName VARCHAR(100),
  AccountType VARCHAR(10),
  SourceAccountNumber VARCHAR(20),
  SourceAccountName VARCHAR(100),
  AsOfDate VARCHAR(20),
  MellonSecurityId VARCHAR(50),
  CountryCode VARCHAR(3),
  Country VARCHAR(50),
  Segment VARCHAR(100),
  Category VARCHAR(100),
  Sector VARCHAR(100),
  Industry VARCHAR(100),
  SecurityDescription1 VARCHAR(200),
  SecurityDescription2 VARCHAR(200),
  EmptyField VARCHAR(10),
  AcctBaseCurrencyCode VARCHAR(3),
  ExchangeRate VARCHAR(50),
  IssueCurrencyCode VARCHAR(3),
  SharesPar VARCHAR(50),
  BaseCost VARCHAR(50),
  LocalCost VARCHAR(50),
  BasePrice VARCHAR(50),
  LocalPrice VARCHAR(50),
  BaseMarketValue VARCHAR(50),
  LocalMarketValue VARCHAR(50),
  BaseNetIncomeReceivable VARCHAR(50),
  LocalNetIncomeReceivable VARCHAR(50),
  BaseMarketValueWithAccrual VARCHAR(50),
  CouponRate VARCHAR(50),
  MaturityDate VARCHAR(20),
  BaseUnrealizedGainLoss VARCHAR(50),
  LocalUnrealizedGainLoss VARCHAR(50),
  BaseUnrealizedCurrencyGainLoss VARCHAR(50),
  BaseNetUnrealizedGainLoss VARCHAR(50),
  PercentOfTotal VARCHAR(50),
  ISIN VARCHAR(20),
  SEDOL VARCHAR(20),
  CUSIP VARCHAR(20),
  Ticker VARCHAR(20),
  CMSAccountNumber VARCHAR(20),
  IncomeCurrency VARCHAR(3),
  SecurityIdentifier VARCHAR(50),
  UnderlyingSecurity VARCHAR(100),
  FairValuePriceLevel VARCHAR(20),
  ReportRunDateTime VARCHAR(50),
  FileSource VARCHAR(100),
  ImportDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_id (id),
  INDEX idx_file_source (FileSource)
);

-- Create the MellonFileImport table for tracking import operations
DROP TABLE IF EXISTS MellonFileImport;

CREATE TABLE MellonFileImport (
  id INT AUTO_INCREMENT PRIMARY KEY,
  FileName VARCHAR(255) NOT NULL UNIQUE,
  FileSize BIGINT,
  ImportDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  RecordsImported INT DEFAULT 0,
  RecordsProcessed INT DEFAULT 0,
  Status ENUM('IMPORTED', 'PROCESSED', 'ERROR') DEFAULT 'IMPORTED',
  ErrorMessage TEXT,
  ProcessingDate TIMESTAMP NULL,
  INDEX idx_filename (FileName),
  INDEX idx_status (Status),
  INDEX idx_import_date (ImportDate)
);

-- Create the MellonHoldings table for final processed data
DROP TABLE IF EXISTS MellonHoldings;

CREATE TABLE MellonHoldings (
  id INT AUTO_INCREMENT PRIMARY KEY,
  -- Same structure as MellonHoldingsStaging but for final data
  AccountNumber VARCHAR(20) NOT NULL,
  AccountName VARCHAR(100) NOT NULL,
  AccountType VARCHAR(10) NOT NULL,
  SourceAccountNumber VARCHAR(20),
  SourceAccountName VARCHAR(100),
  AsOfDate DATE NOT NULL,
  MellonSecurityId VARCHAR(50) NOT NULL,
  CountryCode VARCHAR(3),
  Country VARCHAR(50),
  Segment VARCHAR(100),
  Category VARCHAR(100),
  Sector VARCHAR(100),
  Industry VARCHAR(100),
  SecurityDescription1 VARCHAR(200),
  SecurityDescription2 VARCHAR(200),
  AcctBaseCurrencyCode VARCHAR(3) NOT NULL,
  ExchangeRate DECIMAL(15,6),
  IssueCurrencyCode VARCHAR(3),
  SharesPar DECIMAL(20,4),
  BaseCost DECIMAL(20,2),
  LocalCost DECIMAL(20,2),
  BasePrice DECIMAL(15,6),
  LocalPrice DECIMAL(15,6),
  BaseMarketValue DECIMAL(20,2),
  LocalMarketValue DECIMAL(20,2),
  BaseNetIncomeReceivable DECIMAL(20,2),
  LocalNetIncomeReceivable DECIMAL(20,2),
  BaseMarketValueWithAccrual DECIMAL(20,2),
  CouponRate DECIMAL(10,4),
  MaturityDate DATE,
  BaseUnrealizedGainLoss DECIMAL(20,2),
  LocalUnrealizedGainLoss DECIMAL(20,2),
  BaseUnrealizedCurrencyGainLoss DECIMAL(20,2),
  BaseNetUnrealizedGainLoss DECIMAL(20,2),
  PercentOfTotal DECIMAL(10,4),
  ISIN VARCHAR(20),
  SEDOL VARCHAR(20),
  CUSIP VARCHAR(20),
  Ticker VARCHAR(20),
  CMSAccountNumber VARCHAR(20),
  IncomeCurrency VARCHAR(3),
  SecurityIdentifier VARCHAR(50),
  UnderlyingSecurity VARCHAR(100),
  FairValuePriceLevel INT,
  ReportRunDateTime DATETIME,
  FileSource VARCHAR(100) NOT NULL,
  ImportDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  Processed TINYINT(1) DEFAULT 0,
  INDEX idx_account (AccountNumber),
  INDEX idx_security (MellonSecurityId),
  INDEX idx_date (AsOfDate),
  INDEX idx_processed (Processed),
  INDEX idx_file_source (FileSource)
);

-- table MellonMap for mapping various values in MellonHoldings to other values 
DROP TABLE IF EXISTS MellonMap;

CREATE TABLE MellonMap (
  SourceField VARCHAR(50) NOT NULL,
  SourceValue VARCHAR(50) NOT NULL,
  TargetValue VARCHAR(50) NOT NULL,
  PRIMARY KEY (SourceField, SourceValue)
);

-- table for exceptions during ETL (any process any table any field)
DROP TABLE IF EXISTS ProcessException;
CREATE TABLE ProcessException (
  id INT AUTO_INCREMENT PRIMARY KEY,
  Process VARCHAR(50) NOT NULL,
  TableName VARCHAR(50) NOT NULL,
  FieldName VARCHAR(50) NOT NULL,
  Exception VARCHAR(200) NOT NULL,
  InsertUtc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  InsertUserId INT NOT NULL
);


DROP TABLE IF EXISTS MellonDuration;

CREATE TABLE MellonDuration (
  id INT AUTO_INCREMENT PRIMARY KEY,
  -- Header information (added by workflow)
  AccountName VARCHAR(100) NOT NULL,
  ReportDate VARCHAR(20) NOT NULL,
  -- Data columns from CSV
  SecurityNumber VARCHAR(50),
  SecurityDescription VARCHAR(200),
  CUSIP VARCHAR(20),
  SharesOutstanding VARCHAR(50),
  AssetGroup VARCHAR(10),
  TradedMarketValue VARCHAR(50),
  MaturityDate VARCHAR(20),
  Duration VARCHAR(20),
  YieldToMaturity VARCHAR(20),
  YieldCurrent VARCHAR(20),
  MonthsToMaturity VARCHAR(10),
  DaysToMaturity VARCHAR(10),
  -- Metadata
  FileSource VARCHAR(255) NOT NULL,
  ImportDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  Processed TINYINT(1) DEFAULT 0
);


DELIMITER ;
