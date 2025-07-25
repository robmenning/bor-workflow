USE borarch;

DELIMITER //

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
        AsOfDate = STR_TO_DATE(TRIM(@AsOfDate), ''%c/%e/%Y''),
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
            ELSE STR_TO_DATE(TRIM(@MaturityDate), ''%c/%e/%Y'')
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
            ELSE STR_TO_DATE(TRIM(@ReportRunDateTime), ''%c/%e/%Y %h:%i:%s %p'')
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

DELIMITER ; 