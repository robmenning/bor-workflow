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

-- Create the MellonHoldingsStaging table for processed data
DROP TABLE IF EXISTS MellonHoldingsStaging;

-- CREATE TABLE MellonHoldingsStaging (
--   id INT AUTO_INCREMENT PRIMARY KEY,
--   AccountNumber VARCHAR(20) NOT NULL,
--   AccountName VARCHAR(100) NOT NULL,
--   AccountType VARCHAR(10) NOT NULL,
--   SourceAccountNumber VARCHAR(20),
--   SourceAccountName VARCHAR(100),
--   AsOfDate DATE NOT NULL,
--   MellonSecurityId VARCHAR(50) NOT NULL,
--   CountryCode VARCHAR(3),
--   Country VARCHAR(50),
--   Segment VARCHAR(100),
--   Category VARCHAR(100),
--   Sector VARCHAR(100),
--   Industry VARCHAR(100),
--   SecurityDescription1 VARCHAR(200),
--   SecurityDescription2 VARCHAR(200),
--   AcctBaseCurrencyCode VARCHAR(3) NOT NULL,
--   ExchangeRate DECIMAL(15,6),
--   IssueCurrencyCode VARCHAR(3),
--   SharesPar DECIMAL(20,4),
--   BaseCost DECIMAL(20,2),
--   LocalCost DECIMAL(20,2),
--   BasePrice DECIMAL(15,6),
--   LocalPrice DECIMAL(15,6),
--   BaseMarketValue DECIMAL(20,2),
--   LocalMarketValue DECIMAL(20,2),
--   BaseNetIncomeReceivable DECIMAL(20,2),
--   LocalNetIncomeReceivable DECIMAL(20,2),
--   BaseMarketValueWithAccrual DECIMAL(20,2),
--   CouponRate DECIMAL(10,4),
--   MaturityDate DATE,
--   BaseUnrealizedGainLoss DECIMAL(20,2),
--   LocalUnrealizedGainLoss DECIMAL(20,2),
--   BaseUnrealizedCurrencyGainLoss DECIMAL(20,2),
--   BaseNetUnrealizedGainLoss DECIMAL(20,2),
--   PercentOfTotal DECIMAL(10,4),
--   ISIN VARCHAR(20),
--   SEDOL VARCHAR(20),
--   CUSIP VARCHAR(20),
--   Ticker VARCHAR(20),
--   CMSAccountNumber VARCHAR(20),
--   IncomeCurrency VARCHAR(3),
--   SecurityIdentifier VARCHAR(50),
--   UnderlyingSecurity VARCHAR(100),
--   FairValuePriceLevel INT,
--   ReportRunDateTime DATETIME,
--   FileSource VARCHAR(100) NOT NULL,
--   ImportDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--   Processed TINYINT(1) DEFAULT 0,
--   INDEX idx_account (AccountNumber),
--   INDEX idx_security (MellonSecurityId),
--   INDEX idx_date (AsOfDate),
--   INDEX idx_processed (Processed),
--   INDEX idx_file_source (FileSource)
-- );

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


-- Create stored procedures in borarch database
DELIMITER //

DROP PROCEDURE IF EXISTS usp_FundClassFee_Load//
CREATE PROCEDURE IF NOT EXISTS usp_FundClassFee_Load(
  -- IN p_FilePath VARCHAR(255),
  -- IN p_MergeType VARCHAR(10)
)
BEGIN
/*
Loads file in p_FilePath into table FundClassFee.
p_FilePath:
  - path to the file to load
  - paths are inside the bor-files container in the incoming/borarch directory
  - files will be accessed by this proc across the bor-network using the bor-files service
  - '/var/lib/mysql-files/fund_data.csv'

p_MergeType:
  - 'append' - appends data to the table, does not overwrite existing data
  - 'replace' - existing primary key attributes are overwritten
  


Usage:
  - load data from file:
    CALL usp_FundClassFee_Load('incoming/fund_class_fee.csv', 'append');

  - replace data in table:
    CALL usp_FundClassFee_Load('incoming/fund_class_fee.csv', 'replace');
    
*/
  
  DECLARE v_message TEXT;
  DECLARE v_context TEXT;
  DECLARE v_sql TEXT;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
      ROLLBACK;
      SET v_message = LEFT(CONCAT('Custom error message for usp_FundClassFee_Load. Message: ', IFNULL(v_message, 'Unknown error'), '. Context: ', IFNULL(v_context, 'Unknown context') ), 128  );
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
  END;

--   SET v_context = 'check file accessibility';
--   SELECT COUNT(*) INTO @file_exists 
--   FROM information_schema.files 
--   WHERE file_path = p_FilePath;

--   IF @file_exists = 0 THEN
--       SET v_message = CONCAT('File not accessible: ', p_FilePath);
--       SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
--   END IF;

--   CREATE TEMPORARY TABLE IF NOT EXISTS temp_FundClassFee (
--     FundCode VARCHAR(10) NOT NULL,
--     FundName VARCHAR(100) NOT NULL,
--     Class VARCHAR(10) NOT NULL,
--     Description VARCHAR(50) NOT NULL,
--     Mer DECIMAL(5,2),
--     Trailer DECIMAL(5,2),
--     PerformanceFee VARCHAR(50),
--     MinInvestmentInitial VARCHAR(20),
--     MinInvestmentSubsequent VARCHAR(20),
--     Currency VARCHAR(3) NOT NULL,
--     UNIQUE KEY unique_fund_class (FundCode, Class)
--   );

-- /*
--   -- LOAD DATA INFILE p_FilePath
--   -- CHARACTER SET utf8mb4
--   -- INTO TABLE temp_FundClassFee
--   -- FIELDS TERMINATED BY ','
--   -- ENCLOSED BY '"'
--   -- LINES TERMINATED BY '\n'
--   -- IGNORE 1 ROWS
--   -- (fund_code, fund_name, class, description, mer, @trailer, @performance_fee, 
--   -- @min_investment_initial, @min_investment_subsequent, currency)
--   -- SET 
--   --     trailer = NULLIF(@trailer, ''),
--   --     performance_fee = NULLIF(@performance_fee, ''),
--   --     min_investment_initial = NULLIF(@min_investment_initial, 'Closed'),
--   --     min_investment_subsequent = NULLIF(@min_investment_subsequent, 'Closed')
--   -- IGNORE 1 LINES;
-- */

--   SET v_sql = CONCAT('
--     LOAD DATA INFILE ''', p_FilePath, '''
--     CHARACTER SET utf8mb4
--     INTO TABLE temp_FundClassFee
--     FIELDS TERMINATED BY '',''
--     ENCLOSED BY ''"''
--     LINES TERMINATED BY ''\n''
--     IGNORE 1 ROWS
--     (fund_code, fund_name, class, description, mer, @trailer, @performance_fee, 
--     @min_investment_initial, @min_investment_subsequent, currency)
--     SET 
--         trailer = NULLIF(@trailer, ''''),
--         performance_fee = NULLIF(@performance_fee, ''''),
--         min_investment_initial = NULLIF(@min_investment_initial, ''Closed''),
--         min_investment_subsequent = NULLIF(@min_investment_subsequent, ''Closed'')');

--   SET @sql = v_sql;
--   PREPARE stmt FROM @sql;
--   EXECUTE stmt;
--   DEALLOCATE PREPARE stmt;

  SELECT FundCode, FundName, Class, Description, Mer, Trailer, PerformanceFee, MinInvestmentInitial, MinInvestmentSubsequent, Currency FROM FundClassFee;

END//

-- Note: usp_MellonHoldings_Load procedure removed - using Python-based ETL instead

-- Note: usp_MellonRawHoldings_Load procedure removed - using Python-based ETL instead

DELIMITER ;
