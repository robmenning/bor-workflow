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

CREATE TABLE MellonHoldingsStaging (
  id INT AUTO_INCREMENT PRIMARY KEY,
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

-- Create the MellonRawStaging table for raw CSV data
DROP TABLE IF EXISTS MellonRawStaging;

CREATE TABLE MellonRawStaging (
  id INT AUTO_INCREMENT PRIMARY KEY,
  raw_data TEXT NOT NULL,
  INDEX idx_id (id)
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
  INDEX idx_account (AccountNumber),
  INDEX idx_security (MellonSecurityId),
  INDEX idx_date (AsOfDate),
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

DROP PROCEDURE IF EXISTS usp_MellonHoldings_Load//
CREATE PROCEDURE usp_MellonHoldings_Load(
  IN p_FilePath VARCHAR(255),
  IN p_FileSource VARCHAR(100)
)
BEGIN
/*
Loads Mellon holdings CSV file into MellonHoldingsStaging table.

Parameters:
  p_FilePath: Path to the CSV file (e.g., 'incoming/mellon-660610007-AAD-20250414.csv')
  p_FileSource: Source identifier for the file (e.g., 'mellon-660610007-AAD-20250414.csv')

Usage:
  CALL usp_MellonHoldings_Load('incoming/mellon-660610007-AAD-20250414.csv', 'mellon-660610007-AAD-20250414.csv');
*/
  
  DECLARE v_message TEXT;
  DECLARE v_context TEXT;
  DECLARE v_sql TEXT;
  DECLARE v_records_imported INT DEFAULT 0;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
      ROLLBACK;
      SET v_message = LEFT(CONCAT('usp_MellonHoldings_Load: error handling. Message: ', IFNULL(v_message, 'Unknown error'), '. Context: ', IFNULL(v_context, 'Unknown context') ), 128);
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
  END;

  SET v_context = 'parameter validation';

  -- Input validation
  IF p_FilePath IS NULL OR p_FilePath = '' THEN
      SET v_message = 'File path cannot be null or empty';
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
  END IF;

  IF p_FileSource IS NULL OR p_FileSource = '' THEN
      SET v_message = 'File source cannot be null or empty';
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
  END IF;

  SET v_context = 'file import tracking';

  -- Record file import attempt
  INSERT INTO MellonFileImport (FileName, Status)
  VALUES (p_FileSource, 'IMPORTED')
  ON DUPLICATE KEY UPDATE 
    ImportDate = CURRENT_TIMESTAMP,
    Status = 'IMPORTED',
    ErrorMessage = NULL;

  SET v_context = 'data loading';

  -- Load data from CSV file
  SET v_sql = CONCAT('
    LOAD DATA INFILE ''', p_FilePath, '''
    CHARACTER SET utf8mb4
    INTO TABLE MellonHoldingsStaging
    FIELDS TERMINATED BY '',''
    ENCLOSED BY ''"''
    LINES TERMINATED BY ''\n''
    IGNORE 1 LINES
    (@AccountNumber, @AccountName, @AccountType, @SourceAccountNumber, @SourceAccountName, 
     @AsOfDate, @MellonSecurityId, @CountryCode, @Country, @Segment, @Category, @Sector, 
     @Industry, @SecurityDescription1, @SecurityDescription2, @EmptyField, @AcctBaseCurrencyCode, 
     @ExchangeRate, @IssueCurrencyCode, @SharesPar, @BaseCost, @LocalCost, @BasePrice, 
     @LocalPrice, @BaseMarketValue, @LocalMarketValue, @BaseNetIncomeReceivable, 
     @LocalNetIncomeReceivable, @BaseMarketValueWithAccrual, @CouponRate, @MaturityDate, 
     @BaseUnrealizedGainLoss, @LocalUnrealizedGainLoss, @BaseUnrealizedCurrencyGainLoss, 
     @BaseNetUnrealizedGainLoss, @PercentOfTotal, @ISIN, @SEDOL, @CUSIP, @Ticker, 
     @CMSAccountNumber, @IncomeCurrency, @SecurityIdentifier, @UnderlyingSecurity, 
     @FairValuePriceLevel, @ReportRunDateTime)
    SET 
        AccountNumber = TRIM(@AccountNumber),
        AccountName = TRIM(@AccountName),
        AccountType = TRIM(@AccountType),
        SourceAccountNumber = NULLIF(TRIM(@SourceAccountNumber), ''''),
        SourceAccountName = NULLIF(TRIM(@SourceAccountName), ''''),
        AsOfDate = STR_TO_DATE(TRIM(@AsOfDate), ''%m/%d/%Y''),
        MellonSecurityId = TRIM(@MellonSecurityId),
        CountryCode = NULLIF(TRIM(@CountryCode), ''''),
        Country = NULLIF(TRIM(@Country), ''''),
        Segment = NULLIF(TRIM(@Segment), ''''),
        Category = NULLIF(TRIM(@Category), ''''),
        Sector = NULLIF(TRIM(@Sector), ''''),
        Industry = NULLIF(TRIM(@Industry), ''''),
        SecurityDescription1 = NULLIF(TRIM(@SecurityDescription1), ''''),
        SecurityDescription2 = NULLIF(TRIM(@SecurityDescription2), ''''),
        AcctBaseCurrencyCode = TRIM(@AcctBaseCurrencyCode),
        ExchangeRate = NULLIF(TRIM(@ExchangeRate), ''''),
        IssueCurrencyCode = NULLIF(TRIM(@IssueCurrencyCode), ''''),
        SharesPar = NULLIF(REPLACE(TRIM(@SharesPar), ''"'', ''''), ''''),
        BaseCost = NULLIF(REPLACE(TRIM(@BaseCost), ''"'', ''''), ''''),
        LocalCost = NULLIF(REPLACE(TRIM(@LocalCost), ''"'', ''''), ''''),
        BasePrice = NULLIF(TRIM(@BasePrice), ''''),
        LocalPrice = NULLIF(TRIM(@LocalPrice), ''''),
        BaseMarketValue = NULLIF(REPLACE(TRIM(@BaseMarketValue), ''"'', ''''), ''''),
        LocalMarketValue = NULLIF(REPLACE(TRIM(@LocalMarketValue), ''"'', ''''), ''''),
        BaseNetIncomeReceivable = NULLIF(REPLACE(TRIM(@BaseNetIncomeReceivable), ''"'', ''''), ''''),
        LocalNetIncomeReceivable = NULLIF(REPLACE(TRIM(@LocalNetIncomeReceivable), ''"'', ''''), ''''),
        BaseMarketValueWithAccrual = NULLIF(REPLACE(TRIM(@BaseMarketValueWithAccrual), ''"'', ''''), ''''),
        CouponRate = NULLIF(TRIM(@CouponRate), ''''),
        MaturityDate = CASE 
            WHEN TRIM(@MaturityDate) = '''' OR TRIM(@MaturityDate) = '' '' THEN NULL
            ELSE STR_TO_DATE(TRIM(@MaturityDate), ''%m/%d/%Y'')
        END,
        BaseUnrealizedGainLoss = NULLIF(REPLACE(TRIM(@BaseUnrealizedGainLoss), ''"'', ''''), ''''),
        LocalUnrealizedGainLoss = NULLIF(REPLACE(TRIM(@LocalUnrealizedGainLoss), ''"'', ''''), ''''),
        BaseUnrealizedCurrencyGainLoss = NULLIF(REPLACE(TRIM(@BaseUnrealizedCurrencyGainLoss), ''"'', ''''), ''''),
        BaseNetUnrealizedGainLoss = NULLIF(REPLACE(TRIM(@BaseNetUnrealizedGainLoss), ''"'', ''''), ''''),
        PercentOfTotal = NULLIF(REPLACE(REPLACE(TRIM(@PercentOfTotal), '' '', ''''), ''('', ''''), ''''),
        ISIN = NULLIF(TRIM(@ISIN), ''''),
        SEDOL = NULLIF(TRIM(@SEDOL), ''''),
        CUSIP = NULLIF(TRIM(@CUSIP), ''''),
        Ticker = NULLIF(TRIM(@Ticker), ''''),
        CMSAccountNumber = NULLIF(TRIM(@CMSAccountNumber), ''''),
        IncomeCurrency = NULLIF(TRIM(@IncomeCurrency), ''''),
        SecurityIdentifier = NULLIF(TRIM(@SecurityIdentifier), ''''),
        UnderlyingSecurity = NULLIF(TRIM(@UnderlyingSecurity), ''''),
        FairValuePriceLevel = NULLIF(TRIM(@FairValuePriceLevel), ''''),
        ReportRunDateTime = CASE 
            WHEN TRIM(@ReportRunDateTime) = '''' OR TRIM(@ReportRunDateTime) = '' '' THEN NULL
            ELSE STR_TO_DATE(TRIM(@ReportRunDateTime), ''%m/%d/%Y %h:%i:%s %p'')
        END,
        FileSource = ''', p_FileSource, ''',
        Processed = FALSE');

  SET @sql = v_sql;
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  -- Get count of imported records
  SELECT ROW_COUNT() INTO v_records_imported;

  SET v_context = 'update import tracking';

  -- Update file import record with success
  UPDATE MellonFileImport 
  SET RecordsImported = v_records_imported,
      Status = 'IMPORTED',
      ProcessingDate = CURRENT_TIMESTAMP
  WHERE FileName = p_FileSource;

  -- Return summary
  SELECT 
    p_FileSource as FileSource,
    v_records_imported as RecordsImported,
    'SUCCESS' as Status,
    CONCAT('Successfully imported ', v_records_imported, ' records from ', p_FileSource) as Message;

END//

DROP PROCEDURE IF EXISTS usp_MellonRawHoldings_Load//

CREATE PROCEDURE usp_MellonRawHoldings_Load(
    IN p_FilePath VARCHAR(255)
)
BEGIN
    DECLARE v_message TEXT;
    DECLARE v_context TEXT;
    DECLARE v_full_path VARCHAR(500);
    DECLARE v_record_count INT;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SET v_message = LEFT(CONCAT('usp_MellonRawHoldings_Load: error handling. Message: ', IFNULL(v_message, 'Unknown error'), '. Context: ', IFNULL(v_context, 'Unknown context') ), 128);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END;

    SET v_context = 'parameter validation';
    
    -- Input validation
    IF p_FilePath IS NULL OR p_FilePath = '' THEN
        SET v_message = 'File path cannot be null or empty';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    SET v_context = 'file import tracking';
    SET v_full_path = CONCAT('/var/lib/mysql-files/', p_FilePath);
    
    -- Insert or update file import record
    INSERT INTO MellonFileImport (FileName, Status) 
    VALUES (p_FilePath, 'IMPORTED')
    ON DUPLICATE KEY UPDATE Status = 'IMPORTED', ImportDate = NOW();
    
    SET v_context = 'data loading';
    
    -- Clear previous raw data for this file (if any)
    DELETE FROM MellonRawStaging;
    
    -- Load raw CSV data into staging table
    -- Note: MySQL doesn't support variables in LOAD DATA INFILE paths
    -- We need to handle this differently based on the file path
    IF p_FilePath = 'ftpetl/incoming/mellon-660610007-AAD-20250414.csv' THEN
        LOAD DATA INFILE '/var/lib/mysql-files/ftpetl/incoming/mellon-660610007-AAD-20250414.csv' 
        INTO TABLE MellonRawStaging 
        FIELDS TERMINATED BY '\n' 
        LINES TERMINATED BY '\n' 
        (@raw_line) 
        SET raw_data = @raw_line;
    ELSEIF p_FilePath = 'ftpetl/incoming/mellon-660600027-AAD-20250414.csv' THEN
        LOAD DATA INFILE '/var/lib/mysql-files/ftpetl/incoming/mellon-660600027-AAD-20250414.csv' 
        INTO TABLE MellonRawStaging 
        FIELDS TERMINATED BY '\n' 
        LINES TERMINATED BY '\n' 
        (@raw_line) 
        SET raw_data = @raw_line;
    ELSEIF p_FilePath = 'ftpetl/incoming/mellon-660600017-AAD-20250414.csv' THEN
        LOAD DATA INFILE '/var/lib/mysql-files/ftpetl/incoming/mellon-660600017-AAD-20250414.csv' 
        INTO TABLE MellonRawStaging 
        FIELDS TERMINATED BY '\n' 
        LINES TERMINATED BY '\n' 
        (@raw_line) 
        SET raw_data = @raw_line;
    ELSE
        SET v_message = CONCAT('Unsupported file path: ', p_FilePath);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Get record count
    SELECT COUNT(*) INTO v_record_count FROM MellonRawStaging;
    
    SET v_context = 'update import record';
    
    -- Update import record with completion status
    UPDATE MellonFileImport 
    SET Status = 'PROCESSED', 
        RecordsImported = v_record_count
    WHERE FileName = p_FilePath;

END//

DELIMITER ;
