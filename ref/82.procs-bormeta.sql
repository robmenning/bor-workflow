/*

usage (from .../cbor/ directory): 
mysql -u bormetaAdmin -pkBu9pjz2vi < ./script/init/82.procs-bormeta.sql

run on the docker bor-db container:
cat ./script/init/82.procs-bormeta.sql | docker exec -i bor-db mysql -u borAllAdmin -pkBu9pjz2vi bormeta

*/

-- =============================================================================
-- Mellon Holdings Integration Database Objects
-- =============================================================================
-- 
-- This file contains stored procedures for integrating Mellon Holdings data
-- from borarch.MellonHoldings into the borinst database tables.
--
-- Usage: Execute this file in the bor-db container to create the necessary
-- database objects for Mellon Holdings data integration.
-- 
-- Usage examples:
-- -- Integrate all Mellon holdings data
-- CALL usp_mellon_hold_integrate(1, 'FULL_INTEGRATION', NULL, NULL, NULL, NULL);
-- 
-- -- Integrate specific account holdings
-- CALL usp_mellon_hold_integrate(1, 'ACCOUNT_INTEGRATION', '660600017');
-- 
-- -- Integrate holdings for specific date range
-- CALL usp_mellon_hold_integrate(1, 'DATE_RANGE_INTEGRATION', NULL, '2024-01-01', '2024-12-31');
--
-- Dependencies:
-- - borarch.MellonHoldings table (source data)
-- - bormeta.MellonMap table (mapping data)
-- - borinst.Port table (target portfolio data)
-- - borinst.DateVal table (target date dimension)
-- - borinst.Instr table (target instrument data)
-- - borinst.InstrIdenVal table (target instrument identifiers)
-- - borinst.InstrAttributeVal table (target instrument attributes)
-- - borinst.InstrPrice table (target instrument price data)
-- - borinst.InstrValMultVal table (target instrument multipliers)
-- - borinst.FxPrice table (target foreign exchange rates)
-- - borinst.PortHoldTrade table (target holding data)
-- - borinst.PortCostTrade table (target cost data)
-- - borinst.PortValTrade table (target value data)
-- - borinst.PortAccruedTrade table (target accrued data)
-- - borinst.User table (for user validation)
-- - borinst.Currency table (for currency validation)
-- - borinst.PriceSide table (for price side validation)
-- - borinst.PriceTime table (for price time validation)
-- - borinst.DataSource table (for data source validation)
-- - bormeta.SQLErrorLog table (for error logging)
-- 
-- !!!!!! Cost, MarketValue, Accrued in LOCAL currency, not base currency (CAD) !!!!!!
--
-- Parameters:
-- - p_UserId: User ID for integration
-- - p_integrationType: Type of integration (FULL_INTEGRATION, ACCOUNT_INTEGRATION, DATE_RANGE_INTEGRATION, INCREMENTAL_INTEGRATION)
-- - p_accountNumber: Account number for ACCOUNT_INTEGRATION type
-- - p_startDate: Start date for DATE_RANGE_INTEGRATION type
-- - p_endDate: End date for DATE_RANGE_INTEGRATION type
-- - p_batchSize: Batch size for integration

-- Exceptions: 
-- when exceptions are encountered, insert into ProcessException, and proceed on case by case basis

-- Specific exceptions:
-- * More than one exchange rate for the same currency
-- Process: 'bormeta.usp_mellon_hold_integrate'
-- TableName: 'borarch.MellonHoldings'
-- FieldName: 'ExchangeRate'
-- Exception: 'ExchangeRate is not unique for {AsOfDate} and {IssueCurrencyCode}. AVG used.'
-- * Incomecurrency <> issuecurrencycode
-- * MellonSecurityId is not unique for a given AccountNumber


-- Security IDs:
-- Instr has id and UnqId which is intended to be one of the available market identifiers for convenience,  4 of which may come in this file. 
-- For eash incoming security the "incoming UnqId" will be created in this order of priority:
-- 1. ISIN
-- 2. CUSIP
-- 3. SEDOL
-- 4. Ticker
-- 5. MellonSecurityId
-- if none of these are available, MellonSecurityId will be used as "incoming UnqId"
-- if an incoming UnqId is found in Instr.UnqId, the Instr record will be updated with the incoming values
-- if an incoming UnqId is not found in Instr.UnqId, but the ISIN, CUSIP, SEDOL, or Ticker is found in InstrIden.code, the Instr record 
-- will be will be updated with the incoming values and the other IDs will be used to update InstrIden.code.
-- if an incoming UnqId is not found in Instr.UnqId or in InstrIden.code, an Instr record and InstrIden records will be created with the incoming values.


-- Assumptions:
-- when MellonSecurityId = 'CASH' it is AcctBaseCurrencyCode
-- using BaseCost > 0

-- Incoming records will be merged into the target tables using the new incoming values if different from the existing values, updating the InsertUtc and InsertUserId fields.  

-- Summary of InstrAttributeVal.code values inserted in this file:
-- 'country' - Country code (CountryData)
-- 'sectype' - Security type (VarcharData) - from MellonMap
-- 'seccat' - Security category (VarcharData)
-- 'sector' - Sector (VarcharData)
-- 'indust' - Industry (VarcharData)
-- 'name' - Security name (VarcharData)
-- 'descr' - Security description (VarcharData)
-- 'coupon' - Coupon rate (FloatData)
-- 'maturity' - Maturity date (DateData)
-- 'underly' - Underlying instrument (VarcharData)
-- Summary of InstrIden.code values inserted in this file:
-- 'ISIN' - ISIN (VarcharData)
-- 'CUSIP' - CUSIP (VarcharData)
-- 'SEDOL' - SEDOL (VarcharData)
-- 'TICKER' - Ticker (VarcharData)
-- 'MELLON' - MellonSecurityId (VarcharData)

-- source columns and their target columns with merge rules
/*
AccountNumber > Port.id
AccountName > Port.Name
use MellonMap.SourceField = 'AccountName' to insert into Port.UnqId
AccountType > na
SourceAccountNumber > na
SourceAccountName > na
AsOfDate > used for all tables' DateValId field
MellonSecurityId > InstrIden.code = 'MELLON'
CountryCode > InstrAttributeVal.code = 'COUNTRY', CountryData
Country > na
Segment > use MellonMap.SourceField = 'Segment' to insert into InstrAttributeVal.code = 'sectype'
Category > InstrAttributeVal.code = 'seccat', VarcharData
Sector > InstrAttributeVal.code = 'sector', VarcharData
Industry > InstrAttributeVal.code = 'indust', VarcharData
SecurityDescription1 > concat with SecurityDescription2 into Instr.Name; InstrAttributeVal.code = 'name', VarcharData   
SecurityDescription2 > concat with SecurityDescription1 into Instr.Description; InstrAttributeVal.code = 'description', VarcharData
AcctBaseCurrencyCode > Port.CurrencyCode
ExchangeRate > FxPrice.Price; InstrId = {IssueCurrencyCode}; PriceSideCode = 'MID'; PriceTimeCode = 'CLOSE'; CurrencyCode = IssueCurrencyCode; Price; DataSourceId = 6
IssueCurrencyCode > Instr.CurrencyCode (direct field, not InstrAttributeVal)
SharesPar > PortHoldTrade.Quantity
BaseCost > PortCostTrade.Cost
LocalCost > na
BasePrice > na
LocalPrice > InstrPrice.Price; InstrId = {Instr.id determined in Instr merge step}; PriceSideCode = 'MID'; PriceTimeCode = 'CLOSE'; CurrencyCode = IssueCurrencyCode; Price; DataSourceId = 6
BaseMarketValue > PortValTrade.MarketValueDir; InstrId = {Instr.id determined in Instr merge step}; CurrencyCode = IssueCurrencyCode; FxRate = 1; QuantityDir = {SharesPar};
LocalMarketValue > na
BaseNetIncomeReceivable > PortAccruedTrade.Accrued; InstrId = {Instr.id determined in Instr merge step}; CurrencyCode = IssueCurrencyCode; Accrued; DataSourceId = 6
LocalNetIncomeReceivable > na
BaseMarketValueWithAccrual > na
CouponRate > InstrAttributeVal.code = 'coupon', DecimalData
MaturityDate > InstrAttributeVal.code = 'maturity', DateData
BaseUnrealizedGainLoss > na
LocalUnrealizedGainLoss > na
BaseUnrealizedCurrencyGainLoss > na
BaseNetUnrealizedGainLoss > na
PercentOfTotal > na
ISIN > InstrIden.code = 'ISIN'
SEDOL > InstrIden.code = 'SEDOL'
CUSIP > InstrIden.code = 'CUSIP'
Ticker > InstrIden.code = 'TICKER'
CMSAccountNumber > na
IncomeCurrency > na
SecurityIdentifier > na
UnderlyingSecurity > InstrAttributeVal.code = 'underly', InstrId

*/

-- =============================================================================

USE bormeta;

-- =============================================================================
-- STORED PROCEDURES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Mellon Holdings Integration Procedure
-- Integrates data from borarch.MellonHoldings into borinst tables
-- -----------------------------------------------------------------------------

use bormeta;

DELIMITER //

DROP PROCEDURE IF EXISTS usp_mellon_hold_integrate//

CREATE PROCEDURE usp_mellon_hold_integrate(
    IN p_UserId SMALLINT UNSIGNED,
    IN p_integrationType VARCHAR(50),
    IN p_accountNumber VARCHAR(50),
    IN p_startDate DATE,
    IN p_endDate DATE,
    IN p_batchSize INT
)
BEGIN

/*
================================================================================
    -- Integrates Mellon Holdings data from borarch.MellonHoldings into borinst tables


  p_UserId - User ID performing the integration (for audit trail)

   
usage in mysql console:
CALL usp_mellon_hold_integrate(1, 'FULL_INTEGRATION', NULL, NULL, NULL, 1000);
CALL usp_mellon_hold_integrate(1, 'ACCOUNT_INTEGRATION', '660600017', NULL, NULL, 500);
CALL usp_mellon_hold_integrate(1, 'DATE_RANGE_INTEGRATION', NULL, '2024-01-01', '2024-12-31', 1000);
CALL usp_mellon_hold_integrate(1, 'INCREMENTAL_INTEGRATION', NULL, '2024-12-01', '2024-12-31', 100);
  
CHANGE HISTORY:
  yyyymmdd   user          description
  --------   ------------  ------------------------------------------------------------
  20250729   RMenning      Initial creation - duration report integration framework
================================================================================
*/

    
    DECLARE v_countIncomingInstr SMALLINT UNSIGNED;
    DECLARE v_minInstrIdLock SMALLINT UNSIGNED DEFAULT 0;
    DECLARE v_maxInstrIdLock SMALLINT UNSIGNED;

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;
    DECLARE v_recordsProcessed INT DEFAULT 0;
    DECLARE v_recordsInserted INT DEFAULT 0;
    DECLARE v_recordsUpdated INT DEFAULT 0;
    DECLARE v_recordsSkipped INT DEFAULT 0;
    DECLARE v_startTime TIMESTAMP;
    DECLARE v_endTime TIMESTAMP;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.SQLErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_mellon_hold_integrate', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', p_UserId, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_startTime = NOW();
    SET v_context = 'default parameter value setting';

    -- Set default values if parameters are NULL
    SET p_batchSize = COALESCE(p_batchSize, 1000);
    SET p_integrationType = COALESCE(p_integrationType, 'FULL_INTEGRATION');
    
    SET v_context = 'parameter validation';

    -- Input validation
    IF p_UserId IS NULL THEN
        SET v_message = 'UserId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    IF p_integrationType IS NULL OR TRIM(p_integrationType) = '' THEN
        SET v_message = 'Integration type cannot be null or empty';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    IF p_batchSize <= 0 OR p_batchSize > 10000 THEN
        SET p_batchSize = 1000;
    END IF;

    -- Validate integration type
    IF p_integrationType NOT IN ('FULL_INTEGRATION', 'ACCOUNT_INTEGRATION', 'DATE_RANGE_INTEGRATION', 'INCREMENTAL_INTEGRATION') THEN
        SET v_message = CONCAT('Invalid integration type: ', p_integrationType, '. Valid types: FULL_INTEGRATION, ACCOUNT_INTEGRATION, DATE_RANGE_INTEGRATION, INCREMENTAL_INTEGRATION');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    -- Validate account number if provided
    IF p_integrationType = 'ACCOUNT_INTEGRATION' AND (p_accountNumber IS NULL OR TRIM(p_accountNumber) = '') THEN
        SET v_message = 'Account number is required for ACCOUNT_INTEGRATION type';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    -- Validate date range if provided
    IF p_integrationType = 'DATE_RANGE_INTEGRATION' AND (p_startDate IS NULL OR p_endDate IS NULL) THEN
        SET v_message = 'Start date and end date are required for DATE_RANGE_INTEGRATION type';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    IF p_startDate IS NOT NULL AND p_endDate IS NOT NULL AND p_startDate > p_endDate THEN
        SET v_message = 'Start date cannot be after end date';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    -- Validate user exists
    IF NOT EXISTS (SELECT 1 FROM borinst.User WHERE id = p_UserId) THEN
        SET v_message = CONCAT('Invalid UserId: ', p_UserId);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    -- merge into Port

    SET v_context = 'merge into Port';


    insert into borinst.Port (id, UnqId, AccountId, Name, Description, CurrencyCode, CountryCode, InsertUtc, InsertUserId)
    select  ROW_NUMBER() OVER (ORDER BY m.AccountNumber) + (select IFNULL(MAX(id), 0) from borinst.Port),
            m.UnqId, 1, m.AccountName, m.AccountNumber, m.AcctBaseCurrencyCode, m.CountryCode, UTC_TIMESTAMP(), p_UserId
    from    (
            select  distinct mm.TargetValue as UnqId, mh.AccountNumber, mh.AccountName, mh.AcctBaseCurrencyCode, 'CA' as CountryCode -- !! CountryCode is for the security in the table, check this
            from    borarch.MellonHoldings mh
                    -- use MellonMap.SourceField = 'AccountName' to insert into Port.UnqId
                    left join bormeta.MellonMap mm
                        on mm.SourceField = 'AccountName'
                        and mm.SourceValue = mh.AccountName
            where   mh.AccountNumber is not null
            ) as m
            left join borinst.Port p
                on p.UnqId = m.UnqId
    where   p.id is null
    ON DUPLICATE KEY UPDATE Name = VALUES(Name), Description = VALUES(Description), CurrencyCode = VALUES(CurrencyCode), CountryCode = VALUES(CountryCode), InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

/*
CREATE TABLE IF NOT EXISTS PortAttributeVal (
  PortId smallint UNSIGNED NOT NULL,
  PortAttributeCode varchar(6) NOT NULL,
  VarcharData varchar(500) NULL,
  IntData int NULL,
  FloatData float NULL,
  DateData date NULL,
  BooleanData BIT NULL,
  CountryData char(2) NULL,
  CurrencyData char(3) NULL,
  InsertUtc datetime NOT NULL,
  InsertUserId smallint UNSIGNED NOT NULL,
  PRIMARY KEY (PortId, PortAttributeCode)
);


PENDER ALT ABS RTN      | 2000        |
| AccountName | PENDER CORP BOND FD     | 500         |
| AccountName | PENDER SM CAP OPP FD 
*/

    -- PortAttributeVal
    SET v_context = 'merge into PortAttributeVal';

    insert into borinst.PortAttributeVal (PortId, PortAttributeCode, VarcharData, InsertUtc, InsertUserId)
    select  p.id, 'MELTYP', attval, UTC_TIMESTAMP(), p_UserId
    from    (
            select  distinct mm.TargetValue as UnqId, 
                    (
                        CASE
                            WHEN mh.AccountName LIKE '%PENDER ALT ABS RTN%' THEN 'BALANCED'
                            WHEN mh.AccountName LIKE '%PENDER CORP BOND FD%' THEN 'BOND'
                            WHEN mh.AccountName LIKE '%PENDER SM CAP OPP FD%' THEN 'EQUITY'
                            ELSE 'OTHER'
                        END
                    ) as attval
            from    borarch.MellonHoldings mh
                    -- use MellonMap.SourceField = 'AccountName' to  Port.UnqId
                    left join bormeta.MellonMap mm
                        on mm.SourceField = 'AccountName'
                        and mm.SourceValue = mh.AccountName
            where   mh.AccountNumber is not null
            ) as m
            left join borinst.Port p
                on p.UnqId = m.UnqId
    ON DUPLICATE KEY UPDATE VarcharData = VALUES(VarcharData), InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId
    ;

    -- merge into DateValId
    SET v_context = 'merge into DateVal';

    insert into borinst.DateVal (id, InsertUtc, InsertUserId)
    select  mh.AsOfDate, UTC_TIMESTAMP(), p_UserId
    from    (
            select  distinct AsOfDate
            from    borarch.MellonHoldings
            where   AsOfDate is not null
            ) as mh
            left join borinst.DateVal dv
                on dv.id = mh.AsOfDate
    where   dv.id is null
    ON DUPLICATE KEY UPDATE InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    SET v_context = 'create and populate temp table for Instr ids determination';

    -- create temporary table for ID determination (see header)
    DROP TABLE IF EXISTS MellonIdenMap;
    CREATE TEMPORARY TABLE IF NOT EXISTS MellonIdenMap (
        MellonSecurityId VARCHAR(50) NOT NULL,
        MellonUnqId VARCHAR(50) NULL,
        -- InstrId SMALLINT UNSIGNED NULL,
        -- InstrUnqId VARCHAR(50) NULL,
        -- CurrencyCode CHAR(3) NULL,
        ISIN VARCHAR(50) NULL,
        SEDOL VARCHAR(50) NULL,
        CUSIP VARCHAR(50) NULL,
        Ticker VARCHAR(50) NULL, 
        PRIMARY KEY (MellonSecurityId)
    );

    INSERT INTO MellonIdenMap (MellonSecurityId, MellonUnqId, ISIN, SEDOL, CUSIP, Ticker)
    SELECT  distinct 
            MellonSecurityId, 
            CASE 
                WHEN MellonSecurityId = 'CASH' THEN AcctBaseCurrencyCode 
                ELSE MellonSecurityId 
            END AS MellonUnqId, 
            ISIN, SEDOL, CUSIP, Ticker
            -- , 
            -- -- COALESCE(MellonSecurityId, ISIN, CUSIP, SEDOL, Ticker) AS MellonUnqId, -- would prefer to have ISIN at top but it's not unique
            -- AccountNumber, AccountName, AsOfDate, -- MellonSecurityId, -- removed duplicate
            -- CountryCode, Segment, Category, Sector, Industry, 
            -- SecurityDescription1, SecurityDescription2, AcctBaseCurrencyCode, ExchangeRate, IssueCurrencyCode, SharesPar, 
            -- BaseCost, LocalCost, BasePrice, LocalPrice, BaseMarketValue, LocalMarketValue, BaseNetIncomeReceivable, 
            -- LocalNetIncomeReceivable, BaseMarketValueWithAccrual, CouponRate, MaturityDate, BaseUnrealizedGainLoss, 
            -- LocalUnrealizedGainLoss, BaseUnrealizedCurrencyGainLoss, BaseNetUnrealizedGainLoss, PercentOfTotal, 
            -- ISIN, SEDOL, CUSIP, Ticker, CMSAccountNumber, IncomeCurrency, SecurityIdentifier, UnderlyingSecurity
        FROM borarch.MellonHoldings 
        WHERE COALESCE(IFNULL(TRIM(Sector), ''), 'EXPENSES') <> 'EXPENSES'
        and   BaseCost > 0
        and   MellonSecurityId not like '%REC%'
        and   MellonSecurityId not like '%PAY%'
        and   MellonSecurityId not like '%URL%'
        and   IFNULL(TRIM(IssueCurrencyCode), '') <> '' -- !! confirm all of these
        -- and   SecurityDescription1 not like '%still%' -- look at source. this appears to be weird data (same isin, different mellonsecirytid, different descr)
        -- and   ticker <> 'UNIT28'
        ;
        -- LEFT JOIN (
        --     SELECT  
        --         i.id AS InstrId, 
        --         i.UnqId AS UnqId, 
        --         i.CurrencyCode,
        --         iden.ISIN, 
        --         iden.CUSIP, 
        --         iden.SEDOL, 
        --         iden.TICKER, 
        --         iden.MELLON
        --     FROM borinst.Instr i
        --     LEFT JOIN (
        --         SELECT  
        --             idv.InstrId, 
        --             MAX(CASE WHEN idv.InstrIdenCode = 'ISIN' THEN idv.VarcharData END) AS ISIN,
        --             MAX(CASE WHEN idv.InstrIdenCode = 'CUSIP' THEN idv.VarcharData END) AS CUSIP,
        --             MAX(CASE WHEN idv.InstrIdenCode = 'SEDOL' THEN idv.VarcharData END) AS SEDOL,
        --             MAX(CASE WHEN idv.InstrIdenCode = 'TICKER' THEN idv.VarcharData END) AS TICKER,
        --             MAX(CASE WHEN idv.InstrIdenCode = 'MELLON' THEN idv.VarcharData END) AS MELLON
        --         FROM borinst.InstrIdenVal idv
        --         WHERE idv.InstrIdenCode IN ('ISIN', 'CUSIP', 'SEDOL', 'TICKER', 'MELLON')
        --         GROUP BY idv.InstrId
        --         ) AS iden 
        --         ON i.id = iden.InstrId
        -- ) AS iden 
        -- ON  mh.MellonUnqId = iden.UnqId
        --     OR mh.ISIN = iden.ISIN
        --     OR mh.CUSIP = iden.CUSIP
        --     OR mh.SEDOL = iden.SEDOL
        --     OR mh.TICKER = iden.TICKER
        --     OR mh.MellonSecurityId = iden.MELLON;


    -- there is a data issue that is causing problems: 
        -- there are duplicate ISIN and Ticker values for the same MellonSecurityId
        -- for now the solution is to update the ISIN and Ticker values to be unique by appending an integer to the end of the value

    -- select * from MellonIdenMap where isin like '%_%';

    update MellonIdenMap mm
    join (
        select  
            mh.mellonsecurityid,
            mh.isin,
            concat(mh.isin, '_', row_number() over ()) as newisin
        from    borarch.MellonHoldings mh
                join (
                    select isin, count(distinct mellonsecurityid)
                    from    borarch.MellonHoldings 
                    where isin is not null
                    group by isin
                    having count(distinct mellonsecurityid) > 1
                ) dup
                    on dup.isin = mh.isin
        ) as newisin
        on mm.mellonsecurityid = newisin.mellonsecurityid
    set mm.isin = newisin.newisin;

    -- Update duplicate CUSIPs
    update MellonIdenMap mm
    join (
        select  
            mh.mellonsecurityid,
            mh.cusip,
            concat(mh.cusip, '_', row_number() over ()) as newcusip
        from    borarch.MellonHoldings mh
                join (
                    select cusip, count(distinct mellonsecurityid)
                    from    borarch.MellonHoldings 
                    where cusip is not null
                    group by cusip
                    having count(distinct mellonsecurityid) > 1
                ) dup
                    on dup.cusip = mh.cusip
        ) as newcusip
        on mm.mellonsecurityid = newcusip.mellonsecurityid
    set mm.cusip = newcusip.newcusip;

    -- Update duplicate SEDOLs
    update MellonIdenMap mm
    join (
        select  
            mh.mellonsecurityid,
            mh.sedol,
            concat(mh.sedol, '_', row_number() over ()) as newsedol
        from    borarch.MellonHoldings mh
                join (
                    select sedol, count(distinct mellonsecurityid)
                    from    borarch.MellonHoldings 
                    where sedol is not null
                    group by sedol
                    having count(distinct mellonsecurityid) > 1
                ) dup
                    on dup.sedol = mh.sedol
        ) as newsedol
        on mm.mellonsecurityid = newsedol.mellonsecurityid
    set mm.sedol = newsedol.newsedol;

    -- Update duplicate Tickers
    update MellonIdenMap mm
    join (
        select  
            mh.mellonsecurityid,
            mh.ticker,
            concat(mh.ticker, '_', row_number() over ()) as newticker
        from    borarch.MellonHoldings mh
                join (
                    select ticker, count(distinct mellonsecurityid)
                    from    borarch.MellonHoldings 
                    where ticker is not null
                    group by ticker
                    having count(distinct mellonsecurityid) > 1
                ) dup
                    on dup.ticker = mh.ticker
        ) as newticker
        on mm.mellonsecurityid = newticker.mellonsecurityid
    set mm.ticker = newticker.newticker;

    -- select mellonsecurityid, isin, cusip, sedol, ticker from MellonIdenMap order by isin asc;
    
    -- select * from MellonIdenMap where isin like '%_%';



    -- select MellonSecurityId, MellonUnqId, InstrId, InstrUnqId, CurrencyCode, ISIN, CUSIP, SEDOL, Ticker from MellonIdenMap limit 50;

    -- determine the max(id) for Instr, then use mysql gap lock to lock range if ids based on the number of rows in MellonIdenMap, then insert
    SELECT IFNULL(MAX(id), 0) INTO v_minInstrIdLock FROM borinst.Instr;
    SELECT IFNULL(COUNT(*), 0) INTO v_countIncomingInstr FROM MellonIdenMap;
    SET v_maxInstrIdLock = v_minInstrIdLock + v_countIncomingInstr; 
    -- select v_minInstrIdLock, v_countIncomingInstr, v_maxInstrIdLock;

    
    START TRANSACTION;
        -- lock the range of ids 
        -- Lock a range of id values in borinst.Instr to prevent concurrent inserts in this id range.
        
        -- SELECT v_minInstrIdLock AS minInstrIdLock, v_maxInstrIdLock AS maxInstrIdLock;

        SELECT id FROM borinst.Instr WHERE id >= v_minInstrIdLock AND id < v_maxInstrIdLock FOR UPDATE;


        -- select MellonSecurityId, MellonUnqId, InstrId, InstrUnqId, CurrencyCode, ISIN, CUSIP, SEDOL, Ticker from MellonIdenMap order by MellonUnqId asc;
        -- select * from MellonIdenMap where securitydescription1 like '%still%' ;

        SET v_context = 'insert into Instr';

        
        INSERT INTO borinst.Instr (id, UnqId, Name, Description, CurrencyCode, InsertUtc, InsertUserId)
        select  ROW_NUMBER() OVER (ORDER BY m.MellonUnqId) + v_minInstrIdLock, 
                m.MellonUnqId, 
                m.SecurityDescription1, 
                m.SecurityDescription2, 
                m.IssueCurrencyCode, 
                UTC_TIMESTAMP(), 
                p_UserId
        from    (
                select  distinct 
                        mm.MellonUnqId, -- use the converted MellonSecurityId from MellonIdenMap
                        mh.SecurityDescription1, 
                        mh.SecurityDescription2, 
                        mh.IssueCurrencyCode
                from    MellonIdenMap mm 
                        join borarch.MellonHoldings mh 
                            on mh.MellonSecurityId = mm.MellonSecurityId
                        left join borinst.Instr i 
                            on i.UnqId = mm.MellonUnqId
                where   i.id is null
                ) as m
        ON DUPLICATE KEY UPDATE Name = VALUES(Name), Description = VALUES(Description), CurrencyCode = VALUES(CurrencyCode), InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    COMMIT;

    SET v_context = 'merge into InstrIdenVal';
    
    insert into borinst.InstrIdenVal (InstrId, InstrIdenCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'MELLON', mm.MellonSecurityId, UTC_TIMESTAMP(), p_UserId 
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
    where   mm.MellonSecurityId is not null
    on duplicate key update VarcharData = mm.MellonSecurityId, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    
    insert into borinst.InstrIdenVal (InstrId, InstrIdenCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'ISIN', mm.ISIN, UTC_TIMESTAMP(), p_UserId 
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
    where   mm.ISIN is not null
    on duplicate key update VarcharData = mm.ISIN, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId
    ;

    INSERT INTO borinst.InstrIdenVal (InstrId, InstrIdenCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'CUSIP', mm.CUSIP, UTC_TIMESTAMP(), p_UserId 
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
    where   mm.CUSIP is not null
    ON DUPLICATE KEY UPDATE VarcharData = mm.CUSIP, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    INSERT INTO borinst.InstrIdenVal (InstrId, InstrIdenCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'SEDOL', mm.SEDOL, UTC_TIMESTAMP(), p_UserId 
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
    where   mm.SEDOL is not null
    ON DUPLICATE KEY UPDATE VarcharData = mm.SEDOL, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;
    
    INSERT INTO borinst.InstrIdenVal (InstrId, InstrIdenCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'TICKER', mm.Ticker, UTC_TIMESTAMP(), p_UserId 
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
    where   mm.Ticker is not null
    ON DUPLICATE KEY UPDATE VarcharData = mm.Ticker, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;


    SET v_context = 'merge into InstrAttributeVal';
    
-- CountryCode > InstrAttributeVal.code = 'COUNTRY', CountryData
    insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, CountryData, InsertUtc, InsertUserId)
    select  distinct i.id, 'country', mh.CountryCode, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.CountryCode is not null
    on duplicate key update CountryData = mh.CountryCode, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'sectype', map.TargetValue, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
                    -- use MellonMap.SourceField = 'AccountName' to insert into Port.UnqId
            left join bormeta.MellonMap map
                on map.SourceField = 'segment'
                and map.SourceValue = mh.Segment
    on duplicate key update VarcharData = map.TargetValue, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'seccat', mh.Category, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.Category is not null
    on duplicate key update VarcharData = mh.Category, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'sector', mh.Sector, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.Sector is not null
    on duplicate key update VarcharData = mh.Sector, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    
-- Industry > InstrAttributeVal.code = 'indust', VarcharData
insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'indust', mh.Industry, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.Industry is not null
    on duplicate key update VarcharData = mh.Industry, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;
    
    insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'name', mh.SecurityDescription1, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.SecurityDescription1 is not null
    on duplicate key update VarcharData = mh.SecurityDescription1, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'descr', mh.SecurityDescription2, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.SecurityDescription2 is not null
    on duplicate key update VarcharData = mh.SecurityDescription2, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, FloatData, InsertUtc, InsertUserId)
    select  distinct i.id, 'coupon', mh.CouponRate, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.CouponRate is not null
    on duplicate key update FloatData = mh.CouponRate, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, DateData, InsertUtc, InsertUserId)
    select  distinct i.id, 'maturity', mh.MaturityDate, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.MaturityDate is not null
    on duplicate key update DateData = mh.MaturityDate, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    
    insert into borinst.InstrAttributeVal (InstrId, InstrAttributeCode, VarcharData, InsertUtc, InsertUserId)
    select  distinct i.id, 'underly', mh.UnderlyingSecurity, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.UnderlyingSecurity is not null
    on duplicate key update VarcharData = mh.UnderlyingSecurity, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    insert into borinst.FxPrice (DateValId, BaseCurrencyCode, FxCurrencyCode, PriceSideCode, PriceTimeCode, Price, DataSourceId, InsertUtc, InsertUserId)
    select  distinct mh.AsOfDate, 'CAD', mh.IssueCurrencyCode, 'MID', 'CLOSE', mh.ExchangeRate, 6, UTC_TIMESTAMP(), p_UserId
    from    borarch.MellonHoldings mh
    where   mh.IssueCurrencyCode is not null 
    --   and mh.IssueCurrencyCode <> 'CAD' -- exclude CAD to CAD (1:1 rate)
    and     mh.ExchangeRate is not null
    on duplicate key update Price = mh.ExchangeRate, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    set v_context = 'merge into PortHoldTrade';

    -- first delete all records for incoming date and p.id
    delete pht from borinst.PortHoldTrade pht
            join borinst.Port p
                on pht.PortId = p.id
            join bormeta.MellonMap map 
                on map.SourceField = 'AccountName'
                and map.TargetValue = p.UnqId
            join borarch.MellonHoldings mh 
                on mh.AccountName = map.SourceValue
            where   mh.AsOfDate >= p_StartDate
            and     mh.AsOfDate <= p_EndDate
            ;

    INSERT INTO borinst.PortHoldTrade (DateValId, PortId, InstrId, Quantity, InsertUtc, InsertUserId)
    select  mhs.AsOfDate, mhs.PortId, mhs.InstrId, mhs.sumSharesPar, UTC_TIMESTAMP(), p_UserId
    from    (
            select  mh.AsOfDate, p.id as PortId, i.id as InstrId, sum(mh.SharesPar) as sumSharesPar
            from    borinst.Port p
                    join bormeta.MellonMap map 
                        on map.SourceField = 'AccountName'
                        and map.TargetValue = p.UnqId
                    join borarch.MellonHoldings mh 
                        on mh.AccountName = map.SourceValue
                    join MellonIdenMap mm 
                        on mh.MellonSecurityId = mm.MellonSecurityId
                    join borinst.Instr i 
                        on i.UnqId = mm.MellonUnqId
            where   mh.SharesPar is not null
            group by mh.AsOfDate, p.id, i.id
            ) as mhs order by mhs.InstrId
    ON DUPLICATE KEY UPDATE Quantity = mhs.sumSharesPar, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId
    ;

    -- select * from borinst.PortHoldTradeHist where instrid = 327;

    set v_context = 'merge into PortCostTrade';

    -- first delete all records for incoming date and p.id
    delete pct from borinst.PortCostTrade pct
            join borinst.Port p
                on pct.PortId = p.id
            join bormeta.MellonMap map 
                on map.SourceField = 'AccountName'
                and map.TargetValue = p.UnqId
            join borarch.MellonHoldings mh 
                on mh.AccountName = map.SourceValue
            where   mh.AsOfDate >= p_StartDate
            and     mh.AsOfDate <= p_EndDate
            ;

    -- CostFXRate = CostLocal / CostBase    
    INSERT INTO borinst.PortCostTrade (DateValId, PortId, InstrId, CurrencyCode, Cost, CostFxRate, InsertUtc, InsertUserId)
    select  mhs.AsOfDate, mhs.PortId, mhs.InstrId, mhs.IssueCurrencyCode, mhs.sumLocalCost, mhs.sumCostFxRate, UTC_TIMESTAMP(), p_UserId
    from    (
            select  mh.AsOfDate, p.id as PortId, i.id as InstrId, mh.IssueCurrencyCode, sum(mh.LocalCost) as sumLocalCost,
                    sum(mh.LocalCost) / sum(mh.BaseCost) as sumCostFxRate
            from    borinst.Port p
                    join bormeta.MellonMap map 
                        on map.SourceField = 'AccountName'
                        and map.TargetValue = p.UnqId
                    join borarch.MellonHoldings mh 
                        on mh.AccountName = map.SourceValue
                    join MellonIdenMap mm 
                        on mh.MellonSecurityId = mm.MellonSecurityId
                    join borinst.Instr i 
                        on i.UnqId = mm.MellonUnqId
            where   mh.BaseCost is not null
            group by mh.AsOfDate, p.id, i.id, mh.IssueCurrencyCode
            ) as mhs order by mhs.InstrId, mhs.IssueCurrencyCode
    ON DUPLICATE KEY UPDATE Cost = mhs.sumLocalCost, CostFxRate = mhs.sumCostFxRate, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    set v_context = 'merge into InstrPrice';

    insert into borinst.InstrPrice (DateValId, InstrId, PriceSideCode, PriceTimeCode, CurrencyCode, Price, DataSourceId, InsertUtc, InsertUserId)
    select  distinct mh.AsOfDate, i.id, 'MID', 'CLOSE', mh.IssueCurrencyCode, mh.LocalPrice, 6, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    where   mh.LocalPrice is not null
    on duplicate key update Price = mh.LocalPrice, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    set v_context = 'merge into InstrValMultVal';

    INSERT INTO borinst.InstrValMultVal (InstrId, InstrValMultCode, Value, InsertUtc, InsertUserId)
    select  distinct i.id, 'price', (case when mh.Segment = 'FIXED INCOME' then .01 else 1 end), UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    on duplicate key update Value = (case when mh.Segment = 'FIXED INCOME' then .01 else 0 end), InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    insert into borinst.InstrValMultVal (InstrId, InstrValMultCode, Value, InsertUtc, InsertUserId)
    select  distinct i.id, 'quantity', 1, UTC_TIMESTAMP(), p_UserId
    from    borinst.Instr i
            join MellonIdenMap mm 
                on i.UnqId = mm.MellonUnqId
            join borarch.MellonHoldings mh 
                on mh.MellonSecurityId = mm.MellonSecurityId
    on duplicate key update Value = 1, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    set v_context = 'merge into PortValTrade';
    
    -- first delete all records for incoming date and p.id
    delete pvt from borinst.PortValTrade pvt
            join borinst.Port p
                on pvt.PortId = p.id
            join bormeta.MellonMap map 
                on map.SourceField = 'AccountName'
                and map.TargetValue = p.UnqId
            join borarch.MellonHoldings mh 
                on mh.AccountName = map.SourceValue
            where   mh.AsOfDate >= p_StartDate
            and     mh.AsOfDate <= p_EndDate
            ;

    insert into borinst.PortValTrade (DateValId, PortId, InstrId, SourcePath, SourcePathUnqId, SourceInstrId, PathLevel, CurrencyCode, QuantityDir, QuantityIndir, MarketValueDir, MarketValueIndir, InsertUtc, InsertUserId)
    select  mvs.AsOfDate, mvs.PortId, mvs.InstrId, mvs.PortId, p.UnqId, mvs.InstrId, 1, mvs.IssueCurrencyCode, mvs.sumSharesPar, mvs.sumSharesPar, mvs.sumLocalMarketValue, mvs.sumLocalMarketValue, UTC_TIMESTAMP(), p_UserId
    from    (
            select  mh.AsOfDate, p.id as PortId, i.id as InstrId, mh.IssueCurrencyCode, sum(IFNULL(mh.SharesPar, 0)) as sumSharesPar, sum(IFNULL(mh.LocalMarketValue, 0)) as sumLocalMarketValue
            from    borinst.Port p
                    join bormeta.MellonMap map 
                        on map.SourceField = 'AccountName'
                        and map.TargetValue = p.UnqId
                    join borarch.MellonHoldings mh 
                        on mh.AccountName = map.SourceValue
                    join MellonIdenMap mm 
                        on mh.MellonSecurityId = mm.MellonSecurityId
                    join borinst.Instr i 
                        on i.UnqId = mm.MellonUnqId
            where   mh.SharesPar is not null and mh.LocalMarketValue is not null
            group by mh.AsOfDate, p.id, i.id, mh.IssueCurrencyCode
            ) as mvs
            join borinst.Port p
                on p.id = mvs.PortId
            order by mvs.InstrId, mvs.IssueCurrencyCode
    ON DUPLICATE KEY UPDATE QuantityDir = VALUES(QuantityDir), MarketValueDir = VALUES(MarketValueDir), InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    set v_context = 'merge into PortAccruedTrade';

    insert into borinst.PortAccruedTrade (DateValId, PortId, InstrId, CurrencyCode, Accrued, InsertUtc, InsertUserId)
    select  sa.AsOfDate, sa.PortId, sa.InstrId, sa.IssueCurrencyCode, sa.sumAccrued, UTC_TIMESTAMP(), p_UserId
    from    (
            select  mh.AsOfDate, p.id as PortId, i.id as InstrId, mh.IssueCurrencyCode, sum(IFNULL(mh.LocalNetIncomeReceivable, 0)) as sumAccrued
            from    borinst.Port p
                    join bormeta.MellonMap map 
                        on map.SourceField = 'AccountName'
                        and map.TargetValue = p.UnqId
                    join borarch.MellonHoldings mh 
                        on mh.AccountName = map.SourceValue
                    join MellonIdenMap mm 
                        on mh.MellonSecurityId = mm.MellonSecurityId
                    join borinst.Instr i 
                        on i.UnqId = mm.MellonUnqId
            where   IFNULL(mh.LocalNetIncomeReceivable, 0) <> 0
            group by mh.AsOfDate, p.id, i.id, mh.IssueCurrencyCode
            ) as sa 
    ON DUPLICATE KEY UPDATE Accrued = sa.sumAccrued, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;


    SET v_context = 'integration execution';

    -- TODO: Add integration logic here
    -- This will include:
    -- 1. Data validation and transformation
    -- 2. Portfolio creation/update logic
    -- 3. Instrument creation/update logic
    -- 4. Portfolio-instrument relationship management
    -- 5. Tax lot data integration
    -- 6. Error handling and logging
    -- 7. Transaction management


    -- temporary for creating a balanced fun from two loaded portfolios:: 
    -- 1. create Instr for two of the loaded portfolios as funds.
    -- 2. create InstrIden for two ports as funds
    -- 3. create InstrPort to link new Instrs , etc...

    SET v_endTime = NOW();
    
    -- Return integration summary
    -- SELECT 
    --     p_integrationType as IntegrationType,
    --     p_accountNumber as AccountNumber,
    --     p_startDate as StartDate,
    --     p_endDate as EndDate,
    --     v_recordsProcessed as RecordsProcessed,
    --     v_recordsInserted as RecordsInserted,
    --     v_recordsUpdated as RecordsUpdated,
    --     v_recordsSkipped as RecordsSkipped,
    --     v_startTime as StartTime,
    --     v_endTime as EndTime,
    --     TIMESTAMPDIFF(SECOND, v_startTime, v_endTime) as DurationSeconds,
    --     'Integration completed successfully' as Status;

END//

DROP PROCEDURE IF EXISTS usp_mellon_duration_integrate//

CREATE PROCEDURE usp_mellon_duration_integrate(
    IN p_UserId INT
)
BEGIN
/*
================================================================================

  p_UserId - User ID performing the integration (for audit trail)

DEPENDENCIES:
  - MellonDuration table in borarch database (raw duration data)
  - Instrument table in borinst database (security master)
  - Portfolio table in borinst database (portfolio holdings)
  - ProcessException table in borarch database (error logging)

USAGE EXAMPLES:
  -- Integrate all unprocessed duration data for a specific user
  CALL bormeta.usp_mellon_duration_integrate(1);

  
CHANGE HISTORY:
  yyyymmdd   user          description
  --------   ------------  ------------------------------------------------------------
  20250801   RMenning      Initial creation - duration report integration framework
================================================================================
*/

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;
    DECLARE v_recordsProcessed INT DEFAULT 0;
    DECLARE v_instrumentsCreated INT DEFAULT 0;
    DECLARE v_portfolioRecords INT DEFAULT 0;
    DECLARE v_startTime TIMESTAMP;
    DECLARE v_endTime TIMESTAMP;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.SQLErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_mellon_duration_integrate', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', p_UserId, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_startTime = NOW();
    SET v_context = 'parameter validation';

    -- Input validation
    IF p_UserId IS NULL THEN
        SET v_message = 'UserId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;


    SET v_context = 'data validation';
    
    -- Create temporary table for duration-instrument mapping
    DROP TABLE IF EXISTS DurationInstrMap;
    CREATE TEMPORARY TABLE DurationInstrMap (
        ReportDate DATE NOT NULL,
        AccountName VARCHAR(100) NOT NULL,
        SecurityNumber VARCHAR(50) NOT NULL,
        CUSIP VARCHAR(20),
        InstrId SMALLINT UNSIGNED,
        InstrUnqId VARCHAR(50),
        InstrCusip VARCHAR(20),
        InstrMellon VARCHAR(50),
        MatchType VARCHAR(20)
    );
    
    
    INSERT INTO DurationInstrMap (
        ReportDate,
        AccountName,
        SecurityNumber,
        CUSIP,
        InstrId,
        InstrUnqId,
        InstrCusip,
        InstrMellon,
        MatchType
    )
    SELECT  md.reportdate,
            md.accountname, 
            md.securitynumber, 
            md.cusip, 
            i.InstrId,
            i.InstrUnqId,
            i.InstrCusip,
            i.InstrMellon,
            CASE 
                WHEN i.InstrUnqId = md.securitynumber THEN 'UNQID'
                WHEN i.InstrCusip = md.cusip THEN 'CUSIP'
                WHEN i.InstrMellon = md.securitynumber THEN 'MELLON'
                ELSE 'NO_MATCH'
            END as MatchType
    FROM    borarch.MellonDuration md 
            LEFT JOIN (
                        SELECT  i.id as InstrId,
                                i.UnqId as InstrUnqId,
                                iid.CUSIP as InstrCusip,
                                iid.MELLON as InstrMellon
                        FROM    borinst.Instr i
                                LEFT JOIN (
                                        SELECT  Instrid,
                                                MAX(CASE WHEN InstrIdenCode = 'CUSIP' THEN VarcharData ELSE NULL END) as CUSIP,
                                                MAX(CASE WHEN InstrIdenCode = 'MELLON' THEN VarcharData ELSE NULL END) as MELLON
                                        FROM    borinst.InstrIdenVal 
                                        WHERE   InstrIdenCode IN ('CUSIP', 'MELLON')
                                        GROUP BY InstrId
                                        ) as iid
                                    ON iid.InstrId = i.id
                        ) as i
                ON (
                    i.InstrUnqId = md.securitynumber
                    OR 
                    i.InstrCusip = md.cusip
                    OR 
                    i.InstrMellon = md.securitynumber
                    );

    -- select * from DurationInstrMap where MatchType = 'NO_MATCH';
    -- select * from DurationInstrMap where InstrId is null;

    -- select count(*) from DurationInstrMap;
    -- select count(*) from borarch.MellonDuration;

    -- insert exception for row count mismatch
    INSERT INTO bormeta.ProcessException (Process, TableName, FieldName, Exception, InsertUserId) 
    SELECT  'usp_mellon_duration_integrate', 
            'borarch.MellonDuration',   
            'data validatoin', 
            CONCAT('Row count mismatch for date: ', md.ReportDate, ' - Source: ', md.mdrc, ' vs Temp: ', IFNULL(dim.dimrc, 0)), 
            p_UserId
    FROM    (
            SELECT  ReportDate, COUNT(*) as mdrc 
            FROM borarch.MellonDuration 
            GROUP BY ReportDate
            ) as md
            LEFT JOIN (
            SELECT  ReportDate, COUNT(*) as dimrc 
            FROM DurationInstrMap 
            GROUP BY ReportDate
            ) as dim
                ON md.ReportDate = dim.ReportDate
    WHERE   md.mdrc <> IFNULL(dim.dimrc, 0);

    -- insert exception for each row that has no match
    INSERT INTO bormeta.ProcessException (Process, TableName, FieldName, Exception, InsertUserId) 
    SELECT  'usp_mellon_duration_integrate', 
            'borarch.MellonDuration',   
            'data validatoin', 
            CONCAT('No existing instrument for date: ', dim.ReportDate, ' - Account: ', dim.AccountName, ' - SecurityNumber: ', dim.SecurityNumber, '. No data imported.') as Exception, 
            p_UserId
    FROM   DurationInstrMap dim
    where   dim.MatchType = 'NO_MATCH';


    -- insert exception for more than one distinct Duration value in borarch.MellonDuration for a SecurityNumber
    INSERT INTO bormeta.ProcessException (Process, TableName, FieldName, Exception, InsertUserId) 
    SELECT  'usp_mellon_duration_integrate', 
            'borarch.MellonDuration',   
            'data validatoin', 
            CONCAT('More than one distinct Duration value for ReportDate: ', md.ReportDate, ' - SecurityNumber: ', md.SecurityNumber, '. Average applied.') as Exception, 
            p_UserId
    FROM    (
            SELECT  ReportDate, SecurityNumber, COUNT(DISTINCT Duration) as DurationCount
            FROM borarch.MellonDuration
            GROUP BY ReportDate, SecurityNumber
            HAVING DurationCount > 1
            ) as md;

    -- insert exception for more than one distinct YieldToMaturity value in borarch.MellonDuration for a SecurityNumber
    INSERT INTO bormeta.ProcessException (Process, TableName, FieldName, Exception, InsertUserId) 
    SELECT  'usp_mellon_duration_integrate', 
            'borarch.MellonDuration',   
            'data validatoin', 
            CONCAT('More than one distinct YieldToMaturity value for ReportDate: ', md.ReportDate, ' - SecurityNumber: ', md.SecurityNumber, '. Average applied.') as Exception, 
            p_UserId
    FROM    (
            SELECT  ReportDate, SecurityNumber, COUNT(DISTINCT YieldToMaturity) as YieldToMaturityCount
            FROM borarch.MellonDuration
            GROUP BY ReportDate, SecurityNumber
            HAVING YieldToMaturityCount > 1
            ) as md;

    -- insert exception for more than one distinct YieldCurrent value in borarch.MellonDuration for a SecurityNumber
    INSERT INTO bormeta.ProcessException (Process, TableName, FieldName, Exception, InsertUserId) 
    SELECT  'usp_mellon_duration_integrate', 
            'borarch.MellonDuration',   
            'data validatoin', 
            CONCAT('More than one distinct CurrentYield value for ReportDate: ', md.ReportDate, ' - SecurityNumber: ', md.SecurityNumber, '. Average applied.') as Exception, 
            p_UserId
    FROM    (
            SELECT  ReportDate, SecurityNumber, COUNT(DISTINCT YieldCurrent) as YieldCurrentCount
            FROM borarch.MellonDuration
            GROUP BY ReportDate, SecurityNumber
            HAVING YieldCurrentCount > 1
            ) as md;

/*
insert IGNORE into InstrRiskmeasure (code, Name, Description, InsertUtc, InsertUserId) values ('MODDUR', 'Modified Duration', 'Interest rate sensitivity of a bond', utc_timestamp(), 1);

CREATE TABLE IF NOT EXISTS InstrRiskmeasureVal (
  DateValId date NOT NULL,
  InstrId smallint UNSIGNED NOT NULL,
  InstrRiskmeasurecode varchar(8) NOT NULL,
  Value decimal(21, 6) NOT NULL,
  InsertUtc datetime NOT NULL,
  InsertUserId smallint UNSIGNED NOT NULL,
  PRIMARY KEY (DateValId, InstrId, InstrRiskmeasureCode)
);

insert IGNORE into InstrValmeasure (code, Name, Description, InsertUtc, InsertUserId) values ('YTM', 'Yield to Maturity', 'Yield to maturity', utc_timestamp(), 1);

CREATE TABLE IF NOT EXISTS InstrValmeasureVal (
  DateValId date NOT NULL,
  InstrId smallint UNSIGNED NOT NULL,
  InstrValmeasureCode varchar(8) NOT NULL,
  Value decimal(21, 6) NOT NULL,
  InsertUtc datetime NOT NULL, 
  InsertUserId smallint UNSIGNED NOT NULL,
  PRIMARY KEY (DateValId, InstrId, InstrValmeasureCode)
);
*/

    -- merge into InstrRiskmeasureVal
    SET v_context = 'merge into InstrRiskmeasureVal';

    -- select * from borarch.MellonDuration limit 20 ;
    -- select * from DurationInstrMap limit 20 ;

    insert into borinst.InstrRiskmeasureVal (DateValId, InstrId, InstrRiskmeasureCode, Value, InsertUtc, InsertUserId)
    select  ReportDate,
            InstrId,
            InstrRiskmeasureCode,
            avdur,
            UTC_TIMESTAMP() as InsertUtc,
            p_UserId as InsertUserId
    from    (
            select  md.ReportDate as ReportDate,
                    dim.InstrId as InstrId,
                    'MODDUR' as InstrRiskmeasureCode,
                    IFNULL(AVG(md.Duration), 0) as avdur
            from    borarch.MellonDuration md
                    join DurationInstrMap dim
                        on md.ReportDate = dim.ReportDate
                        and md.SecurityNumber = dim.SecurityNumber
            where   dim.MatchType <> 'NO_MATCH'
            group by md.ReportDate, dim.InstrId
            ) as md_avg
    ON DUPLICATE KEY UPDATE Value = md_avg.avdur, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;
    
    SET v_context = 'merge into InstrValmeasureVal';

    insert into borinst.InstrValmeasureVal (DateValId, InstrId, InstrValmeasureCode, Value, InsertUtc, InsertUserId)
    select  ReportDate,
            InstrId,
            InstrValmeasureCode,
            avytm,
            UTC_TIMESTAMP() as InsertUtc,
            p_UserId as InsertUserId
    from    (
            select  md.ReportDate as ReportDate,
                    dim.InstrId as InstrId,
                    'YTM' as InstrValmeasureCode,
                    IFNULL(AVG(md.YieldToMaturity), 0) as avytm
            from    borarch.MellonDuration md
                    join DurationInstrMap dim
                        on md.ReportDate = dim.ReportDate
                        and md.SecurityNumber = dim.SecurityNumber
            where   dim.MatchType <> 'NO_MATCH'
            group by md.ReportDate, dim.InstrId
            ) as ytm_avg
    ON DUPLICATE KEY UPDATE Value = ytm_avg.avytm, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;

    insert into borinst.InstrValmeasureVal (DateValId, InstrId, InstrValmeasureCode, Value, InsertUtc, InsertUserId)
    select  ReportDate,
            InstrId,
            InstrValmeasureCode,
            avycur,
            UTC_TIMESTAMP() as InsertUtc,
            p_UserId as InsertUserId
    from    (
            select  md.ReportDate as ReportDate,
                    dim.InstrId as InstrId,
                    'YCUR' as InstrValmeasureCode,
                    IFNULL(AVG(md.YieldCurrent), 0) as avycur
            from    borarch.MellonDuration md
                    join DurationInstrMap dim
                        on md.ReportDate = dim.ReportDate
                        and md.SecurityNumber = dim.SecurityNumber
            where   dim.MatchType <> 'NO_MATCH'
            group by md.ReportDate, dim.InstrId
            ) as ycur_avg
    ON DUPLICATE KEY UPDATE Value = ycur_avg.avycur, InsertUtc = UTC_TIMESTAMP(), InsertUserId = p_UserId;
    
    
    SET v_endTime = NOW();
    
    -- Return summary results
    SELECT 
        v_recordsProcessed as RecordsProcessed,
        v_instrumentsCreated as InstrumentsCreated,
        v_portfolioRecords as PortfolioRecordsCreated,
        TIMESTAMPDIFF(SECOND, v_startTime, v_endTime) as DurationSeconds,
        'Integration completed successfully' as Status;

END//

DELIMITER ;


