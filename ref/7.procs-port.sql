-- usage in localhost mysql:
-- mysql -u borinstAdmin -pkBu9pjz2vi borinst < ./script/init/7.procs-port.sql

-- usage in docker container:
-- cat ./script/init/7.procs-port.sql | docker exec -i bor-db mysql -u borAllAdmin -pkBu9pjz2vi borinst


USE borinst;

DELIMITER //

DROP PROCEDURE IF EXISTS usp_Port_Get//
CREATE PROCEDURE usp_Port_Get(
    IN p_PortId SMALLINT UNSIGNED, -- optional filter
    IN p_AccountId SMALLINT UNSIGNED, -- optional filter
    IN p_Name VARCHAR(80), -- optional filter
    IN p_CurrencyCode CHAR(3), -- optional filter
    IN p_CountryCode CHAR(2), -- optional filter
    IN p_OrderKey VARCHAR(50), -- optional sort key
    IN p_OrderDir VARCHAR(4), -- optional sort direction
    IN p_Limit SMALLINT UNSIGNED, -- optional limit
    IN p_Offset SMALLINT UNSIGNED, -- optional offset
    IN p_UserId SMALLINT UNSIGNED -- user executing the procedure
)
BEGIN
    -- returns Port information subject to the filters

    -- usage in mysql console:
    -- mysql -u borinstAdmin -pkBu9pjz2vi borinst
    -- usage in docker container:
    -- docker exec -i bor-db mysql -u borinstAdmin -pkBu9pjz2vi borinst
    -- then...
    -- get all 
    -- CALL usp_Port_Get(null, null, null, null, null, 'id', 'ASC', 20, 0, 1);
    -- get all for accountid = 1
    -- CALL usp_Port_Get(null, 1, null, null, null, 'Name', 'ASC', 20, 0, 1);
    -- get all for portid = 1
    -- CALL usp_Port_Get(1, null, null, null, null, 'Name', 'ASC', 20, 0, 1);
    -- get all for name = 'Global Equity'
    -- CALL usp_Port_Get(null, null, 'Global Equity', null, null, 'Name', 'ASC', 20, 0, 1);
    -- get all for currency = USD
    -- CALL usp_Port_Get(null, null, null, 'USD', null, 'Name', 'ASC', 20, 0, 1);
    
    DECLARE v_message TEXT;
    DECLARE v_context TEXT;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.ErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_Port_Get', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', p_UserId, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_context = 'default parameter value setting';

    -- Set default values if parameters are NULL
    SET p_OrderKey = COALESCE(p_OrderKey, 'Name');
    SET p_OrderDir = COALESCE(p_OrderDir, 'ASC');
    SET p_Limit = COALESCE(p_Limit, 20);
    SET p_Offset = COALESCE(p_Offset, 0);
    
    SET v_context = 'parameter validation';

    -- Validate parameters
    IF p_OrderDir NOT IN ('ASC', 'DESC') THEN
        SET v_message = 'Invalid OrderDir parameter';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    IF p_OrderKey NOT IN ('id', 'Name', 'AccountId', 'CurrencyCode', 'CountryCode', 'InsertUtc') THEN
        SET v_message = 'Invalid OrderKey parameter';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    SET v_context = 'query execution';

    -- Single static SQL statement
    SELECT 
        p.id,
        p.UnqId,
        p.AccountId,
        p.Name,
        p.Description,
        p.CurrencyCode,
        p.CountryCode,
        p.InsertUtc,
        p.InsertUserId
    FROM Port p
    WHERE (p_PortId IS NULL OR p.id = p_PortId)
      AND (p_AccountId IS NULL OR p.AccountId = p_AccountId)
      AND (p_Name IS NULL OR p.Name = p_Name)
      AND (p_CurrencyCode IS NULL OR p.CurrencyCode = p_CurrencyCode)
      AND (p_CountryCode IS NULL OR p.CountryCode = p_CountryCode)
    ORDER BY 
        CASE p_OrderKey
            WHEN 'Name' THEN p.Name 
            WHEN 'AccountId' THEN p.AccountId
            WHEN 'CurrencyCode' THEN p.CurrencyCode
            WHEN 'CountryCode' THEN p.CountryCode
            WHEN 'InsertUtc' THEN p.InsertUtc 
            ELSE p.id
        END,
        CASE p_OrderDir
            WHEN 'DESC' THEN 'DESC'
            ELSE 'ASC'
        END
    LIMIT p_Limit OFFSET p_Offset;
END//

DROP PROCEDURE IF EXISTS usp_Port_Insert//
CREATE PROCEDURE usp_Port_Insert(
    IN p_AccountId SMALLINT UNSIGNED,
    IN p_Name VARCHAR(80),
    IN p_Description VARCHAR(500),
    IN p_CurrencyCode CHAR(3),
    IN p_CountryCode CHAR(2),
    IN p_UserId SMALLINT UNSIGNED
)
BEGIN
    -- usage in mysql console:
    -- SET @newId = 0; CALL usp_Port_Insert(1, 'Global Equity', 'Global equity portfolio', 'USD', 'US', 1, @newId); SELECT @newId;

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;
    DECLARE v_new_port_id SMALLINT UNSIGNED;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.ErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_Port_Insert', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', p_UserId, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_context = 'parameter validation';
    
    -- Validate required parameters
    IF p_AccountId IS NULL THEN
        SET v_message = 'AccountId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF p_Name IS NULL OR TRIM(p_Name) = '' THEN
        SET v_message = 'Name is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF p_Description IS NULL THEN
        SET v_message = 'Description is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF p_CurrencyCode IS NULL THEN
        SET v_message = 'CurrencyCode is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF p_CountryCode IS NULL THEN
        SET v_message = 'CountryCode is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF p_UserId IS NULL THEN
        SET v_message = 'UserId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Validate foreign keys
    SET v_context = 'foreign key validation';
    
    IF NOT EXISTS (SELECT 1 FROM Account WHERE id = p_AccountId) THEN
        SET v_message = CONCAT('Invalid AccountId: ', p_AccountId);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM Currency WHERE code = p_CurrencyCode) THEN
        SET v_message = CONCAT('Invalid CurrencyCode: ', p_CurrencyCode);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM Country WHERE code = p_CountryCode) THEN
        SET v_message = CONCAT('Invalid CountryCode: ', p_CountryCode);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM User WHERE id = p_UserId) THEN
        SET v_message = CONCAT('Invalid UserId: ', p_UserId);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check for duplicate name within the same account
    IF EXISTS (SELECT 1 FROM Port WHERE AccountId = p_AccountId AND Name = p_Name) THEN
        SET v_message = CONCAT('A portfolio with name "', p_Name, '" already exists for this account');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    SET v_context = 'insert execution';
    
    -- Set @UserId for triggers
    SET @UserId = p_UserId;
    
    START TRANSACTION;
    
    -- Insert the new port
    INSERT INTO Port (
        AccountId,
        Name,
        Description,
        CurrencyCode,
        CountryCode,
        InsertUtc,
        InsertUserId
    ) VALUES (
        p_AccountId,
        p_Name,
        p_Description,
        p_CurrencyCode,
        p_CountryCode,
        UTC_TIMESTAMP(),
        p_UserId
    );
    
    -- Get the new ID
    SET v_new_port_id = LAST_INSERT_ID();
    
    COMMIT;


    -- return the new port record
    select AccountId, Name, Description, CurrencyCode, CountryCode, InsertUtc, InsertUserId
    from Port
    where id = v_new_port_id;
    
    -- Clear @UserId
    SET @UserId = NULL;
END//

DROP PROCEDURE IF EXISTS usp_Port_Update//
CREATE PROCEDURE usp_Port_Update(
    IN p_PortId SMALLINT UNSIGNED,
    IN p_AccountId SMALLINT UNSIGNED,
    IN p_Name VARCHAR(80),
    IN p_Description VARCHAR(500),
    IN p_CurrencyCode CHAR(3),
    IN p_CountryCode CHAR(2),
    IN p_UserId SMALLINT UNSIGNED
)
BEGIN
    -- usage in mysql console:
    -- CALL usp_Port_Update(1, 1, 'Global Equity Updated', 'Updated description', 'USD', 'US', 1);

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;
    DECLARE v_current_account_id SMALLINT UNSIGNED;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.ErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_Port_Update', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', p_UserId, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_context = 'parameter validation';
    
    -- Validate required parameters
    IF p_PortId IS NULL THEN
        SET v_message = 'PortId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF p_UserId IS NULL THEN
        SET v_message = 'UserId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check if port exists
    IF NOT EXISTS (SELECT 1 FROM Port WHERE id = p_PortId) THEN
        SET v_message = CONCAT('Port with ID ', p_PortId, ' does not exist');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Get current account ID for the port
    SELECT AccountId INTO v_current_account_id FROM Port WHERE id = p_PortId;
    
    -- Validate foreign keys if provided
    SET v_context = 'foreign key validation';
    
    IF p_AccountId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Account WHERE id = p_AccountId) THEN
        SET v_message = CONCAT('Invalid AccountId: ', p_AccountId);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF p_CurrencyCode IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Currency WHERE code = p_CurrencyCode) THEN
        SET v_message = CONCAT('Invalid CurrencyCode: ', p_CurrencyCode);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF p_CountryCode IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Country WHERE code = p_CountryCode) THEN
        SET v_message = CONCAT('Invalid CountryCode: ', p_CountryCode);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM User WHERE id = p_UserId) THEN
        SET v_message = CONCAT('Invalid UserId: ', p_UserId);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check for duplicate name within the same account
    IF p_Name IS NOT NULL AND p_Name <> '' AND EXISTS (
        SELECT 1 FROM Port 
        WHERE AccountId = COALESCE(p_AccountId, v_current_account_id) 
        AND Name = p_Name 
        AND id <> p_PortId
    ) THEN
        SET v_message = CONCAT('A portfolio with name "', p_Name, '" already exists for this account');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    SET v_context = 'update execution';
    
    -- Set @UserId for triggers
    SET @UserId = p_UserId;
    
    START TRANSACTION;
    
    -- Update the port with only the provided values
    UPDATE Port
    SET 
        AccountId = COALESCE(p_AccountId, AccountId),
        Name = COALESCE(p_Name, Name),
        Description = COALESCE(p_Description, Description),
        CurrencyCode = COALESCE(p_CurrencyCode, CurrencyCode),
        CountryCode = COALESCE(p_CountryCode, CountryCode)
    WHERE id = p_PortId;
    
    COMMIT;

    -- return the updated port record
    select AccountId, Name, Description, CurrencyCode, CountryCode, InsertUtc, InsertUserId
    from Port
    where id = p_PortId;

    -- Clear @UserId
    SET @UserId = NULL;
END//

DROP PROCEDURE IF EXISTS usp_Port_Delete//
CREATE PROCEDURE usp_Port_Delete(
    IN p_PortId SMALLINT UNSIGNED,
    IN p_UserId SMALLINT UNSIGNED
)
BEGIN
    -- usage in mysql console:
    -- CALL usp_Port_Delete(1, 1);

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.ErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_Port_Delete', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', p_UserId, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_context = 'parameter validation';
    
    -- Validate required parameters
    IF p_PortId IS NULL THEN
        SET v_message = 'PortId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    IF p_UserId IS NULL THEN
        SET v_message = 'UserId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check if port exists
    IF NOT EXISTS (SELECT 1 FROM Port WHERE id = p_PortId) THEN
        SET v_message = CONCAT('Port with ID ', p_PortId, ' does not exist');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Validate user exists
    IF NOT EXISTS (SELECT 1 FROM User WHERE id = p_UserId) THEN
        SET v_message = CONCAT('Invalid UserId: ', p_UserId);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check for related records that would prevent deletion
    SET v_context = 'dependency validation';
    
    -- Check PortInstr
    IF EXISTS (SELECT 1 FROM PortInstr WHERE PortId = p_PortId) THEN
        SET v_message = CONCAT('Cannot delete Port with ID ', p_PortId, ' because it has related instruments');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check PortUser
    IF EXISTS (SELECT 1 FROM PortUser WHERE PortId = p_PortId) THEN
        SET v_message = CONCAT('Cannot delete Port with ID ', p_PortId, ' because it has related users');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check PortCashflow
    IF EXISTS (SELECT 1 FROM PortCashflow WHERE PortId = p_PortId) THEN
        SET v_message = CONCAT('Cannot delete Port with ID ', p_PortId, ' because it has related cashflows');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check PortTran
    IF EXISTS (SELECT 1 FROM PortTran WHERE PortId = p_PortId) THEN
        SET v_message = CONCAT('Cannot delete Port with ID ', p_PortId, ' because it has related transactions');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check AccountPort
    IF EXISTS (SELECT 1 FROM AccountPort WHERE PortId = p_PortId) THEN
        SET v_message = CONCAT('Cannot delete Port with ID ', p_PortId, ' because it has related account associations');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check InstrPort
    IF EXISTS (SELECT 1 FROM InstrPort WHERE PortId = p_PortId) THEN
        SET v_message = CONCAT('Cannot delete Port with ID ', p_PortId, ' because it has related instrument associations');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    SET v_context = 'delete execution';
    
        -- Set @UserId for triggers
    SET @UserId = p_UserId;
    
    START TRANSACTION;
    
    -- return the deleted port record
    select AccountId, Name, Description, CurrencyCode, CountryCode, InsertUtc, InsertUserId
    from Port
    where id = p_PortId;


    -- Delete the port
    DELETE FROM Port WHERE id = p_PortId;
    
    COMMIT;

        
    -- Clear @UserId
    SET @UserId = NULL;
END//

DROP PROCEDURE IF EXISTS usp_PortValTradeExpand//
CREATE PROCEDURE usp_PortValTradeExpand(
    IN p_DateValId date,
    IN p_PortId SMALLINT UNSIGNED,
    IN p_Return BOOLEAN,
    IN p_UserId SMALLINT UNSIGNED
)
BEGIN

-- ---------------------------------------------------------------
/*
Stored procedure to compute and store the expanded values for a portfolio from and to borinst.PortValTrade, given the same p_PortId's
PathLevel = 1 values. All existing rows for the p_DateValId and p_PortId with PathLevel > 1 values are 
replaced with the new values. If the expansion fails, the original values are not deleted.

usage in mysql console:
call usp_PortValTradeExpand('2025-04-14', 10001, 1, 1);
call usp_PortValTradeExpand('2025-04-14', 10002, 1, 1);
call usp_PortValTradeExpand('2025-04-14', 10003, 1, 1);



usage from docker:
docker exec -i bor-db mysql -u borAllAdmin -pkBu9pjz2vi bormeta -e "call usp_PortValTradeExpand('2025-04-14', 10001, 1, 1);"
docker exec -i bor-db mysql -u borAllAdmin -pkBu9pjz2vi bormeta -e "call usp_PortValTradeExpand('2025-04-14', 10002, 1, 1);"
docker exec -i bor-db mysql -u borAllAdmin -pkBu9pjz2vi bormeta -e "call usp_PortValTradeExpand('2025-04-14', 10003, 1, 1);"

Parameters: 
    p_DateValId: the date to assemble values for
    p_PortId: the id of the portfolio to assemble values for. 
    p_Return: 1 return rows of all PathLevel after inserting the new values.

The InstrPort table is the bridge between a pooled fund holding and the portfolio holding the assets of the pooled fund.
It is used to expand the pooled fund positions.

misc helper code:
select PortId, InstrId, SourcePath, PathLevel, CurrencyCode, QuantityDir, QuantityIndir, Price, MarketValueDir, MarketValueIndir from PortValTrade limit 20;

check that direct and indirect reconcile:
select PortId, sum(case when PathLevel = 1 then MarketValueDir else 0 end) as mv1, sum(MarketValueIndir) as mvind from PortValTrade group by PortId;


*/
-- ---------------------------------------------------------------

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;

    -- DECLARE EXIT HANDLER FOR SQLEXCEPTION
    -- BEGIN
    --     ROLLBACK;
    --     SET v_message = LEFT(CONCAT('usp_PortValTradeGet: error handling. Message: ', IFNULL(v_message, 'Unknown error'), '. Context: ', IFNULL(v_context, 'Unknown context') ), 128);
    --     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    -- END;

    SET v_context = 'parameter validation';

    -- Validate required parameters
    IF p_DateValId IS NULL THEN
        SET v_message = 'DateValId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    
    IF p_PortId IS NULL THEN
        SET v_message = 'PortId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    START TRANSACTION;

    -- delete 
    -- from    PortValTrade 
    -- where   DateValId = '2025-04-14' 
    -- and     PortId = 10001
    -- and     PathLevel > 1;
    
    -- update  PortValTrade 
    -- set     MarketValueIndir = 0,
    --         QuantityIndir = 0
    -- where   DateValId = '2025-04-14' 
    -- and     PortId = 10001 
    -- and     PathLevel = 1;

    
    SET v_context = 'delete L>1 rows';

    -- select UTC_TIMESTAMP() as pvt_HIST_before_delete;
    -- select  *
    -- from    PortValTradeHist
    -- where   DateValId = p_DateValId
    -- and     PortId = p_PortId   
    -- order by ModUtc desc;


    delete 
    from    PortValTrade 
    where   DateValId = p_DateValId
    and     PortId = p_PortId 
    and     PathLevel > 1;

    -- !!!! delete history rows for port, date so doesn't get too large
    SET v_context = 'delete history rows for port, date so doesn''t get too large';

    delete 
    from    PortValTradeHist 
    where   DateValId = p_DateValId
    and     PortId = p_PortId;


    SET v_context = 'update L1 rows';

    -- select UTC_TIMESTAMP() as pvt_HIST_before_update_L1;
    -- select  *
    -- from    PortValTradeHist
    -- where   DateValId = p_DateValId
    -- and     PortId = p_PortId   
    -- order by ModUtc desc;

    update  PortValTrade 
    set     MarketValueIndir = 0,
            QuantityIndir = 0
    where   DateValId = p_DateValId 
    and     PortId = p_PortId 
    and     PathLevel = 1;

    -- select UTC_TIMESTAMP() as pvt_HIST_after_update_L1;
    -- select  *
    -- from    PortValTradeHist
    -- where   DateValId = p_DateValId
    -- and     PortId = p_PortId   
    -- order by ModUtc desc;

    SET v_context = 'insert L>1 rows recursively';

    -- approach 1: start with L1, expand in recursive. issue is that the 'expanded' position isn't altered after/when expansion to: 1) indicate it has been
    -- expanded, 2) show <>0 MV for and NULL MV_Indir.
    insert into PortValTrade (DateValId, PortId, InstrId, SourcePath, SourcePathUnqId, SourceInstrId, PathLevel, CurrencyCode, QuantityDir, QuantityIndir, MarketValueDir, MarketValueIndir, InsertUtc, InsertUserId)
        WITH RECURSIVE ctePVT (DateValId, PortId, InstrId, SourcePath, SourcePathUnqId, SourceInstrId, PathLevel, CurrencyCode, QuantityDir, QuantityIndir, MarketValueDir, MarketValueIndir) AS (
            
            select  pvp.DateValId,
                    pvp.PortId,
                    CAST(pvp.InstrId AS CHAR) as InstrId,
                    CAST(pvp.PortId AS CHAR(20)) as SourcePath,
                    pc.UnqId as SourcePathUnqId,
                    CAST(pvp.InstrId AS CHAR) as SourceInstrId,
                    pvp.PathLevel,
                    pvp.CurrencyCode,
                    -- pvp.FxRate,
                    pvp.QuantityDir,
                    pvp.QuantityIndir,
                    -- pvp.Price,
                    pvp.MarketValueDir,
                    pvp.MarketValueIndir -- will/should be null for all levels = 1
                    -- pvp.Cost,
                    -- pvp.Book,
                    -- pvp.BookAmor,
                    -- pvp.LevExp,
                    -- pvp.LevCap,
                    -- pvp.PortTaxLotId,
                    -- pvp.PortTranId,
                    -- pvp.ExpireEventId,
                    -- pvp.InsertUtc,
                    -- pvp.InsertUserId,
                    -- 1.0 as scale_factor -- consider adding
            FROM    PortValTrade pvp
                    join Port pc
                        on pc.id = pvp.PortId
            WHERE   pvp.DateValId = p_DateValId
            AND     pvp.PortId = p_PortId
            AND     pvp.PathLevel = 1

            UNION ALL

            SELECT  pvp.DateValId,
                    pvp.PortId,
                    pvc.InstrId,
                    CONCAT(pvp.SourcePath, '/', CAST(pvc.PortId AS CHAR(20))) as SourcePath,
                    CONCAT(pvp.SourcePathUnqId, '/', pc.UnqId) as SourcePathUnqId,
                    pvp.InstrId as SourceInstrId,
                    -- CAST(exp.PortId AS CHAR) as SourcePath,
                    pvp.PathLevel + 1 as PathLevel,
                    pvc.CurrencyCode,
                    -- pvc.FxRate,
                    -- pvc.QuantityDir * (pvp.MarketValueDir / summv.MarketValueDir),
                    -- pvc.QuantityDir * (pvp.MarketValueDir / summv.MarketValueDir),
                    pvc.QuantityDir * (pvp.MarketValueDir / summv.MarketValueDir),
                    pvc.QuantityDir * (pvp.MarketValueDir / summv.MarketValueDir),
                    -- pvc.Price,
                    -- case when ip.InstrId is not null then pvc.MarketValueDir * (pvp.MarketValueDir / summv.MarketValueDir) else null end as MarketValueDir,
                    -- case when ip.InstrId is not null then pvc.MarketValueDir * (pvp.MarketValueDir / summv.MarketValueDir) else null end as MarketValueInDir
                    pvc.MarketValueDir * (pvp.MarketValueDir / summv.MarketValueDir),
                    pvc.MarketValueDir * (pvp.MarketValueDir / summv.MarketValueDir)
                    -- exp.MarketValueDir * (pvp.MarketValueDir / summv.MarketValueDir)
                    -- child.Cost * pe.scale_factor,
                    -- child.Book * pe.scale_factor,
                    -- child.BookAmor * pe.scale_factor,
                    -- child.LevExp * pe.scale_factor,
                    -- child.LevCap * pe.scale_factor,
                    -- child.PortTaxLotId,
                    -- child.PortTranId,
                    -- child.ExpireEventId,
                    -- child.InsertUtc,
                    -- child.InsertUserId,
                    -- exp.scale_factor * (exp.MarketValueDir / sum(exp.MarketValueDir)) as scale_factor -- consider adding
            FROM    ctePVT pvp
                    join InstrPort ip 
                        on ip.InstrId = pvp.InstrId
                    join PortValTrade pvc 
                        on pvc.DateValId = p_DateValId
                        and pvc.PortId = ip.PortId
                        and pvc.PathLevel = 1
                    join Port pc
                        on pc.id = pvc.PortId
                    join    (
                            select  spvt.PortId,
                                    sum(spvt.MarketValueDir) as MarketValueDir
                            from    PortValTrade spvt
                            where   spvt.DateValId = p_DateValId
                            and     spvt.PathLevel = 1
                            group by spvt.PortId
                            ) as summv
                        on summv.PortId = ip.PortId
            where   pvp.PathLevel < 5  -- prevent infinite recursion
        )
        SELECT 
            DateValId,
            PortId,
            InstrId,
            SourcePath,
            SourcePathUnqId,
            SourceInstrId,
            PathLevel,
            CurrencyCode,
            -- FxRate,
            QuantityDir,
            QuantityIndir,
            -- Price,
            MarketValueDir,
            MarketValueIndir,
            -- Cost,
            -- Book,
            -- BookAmor,
            -- LevExp,
            -- LevCap,
            -- PortTaxLotId,
            -- PortTranId,
            -- ExpireEventId,
            UTC_TIMESTAMP(),
            p_UserId
        FROM ctePVT
        where PathLevel > 1
        order by PathLevel, SourcePath, InstrId
        ;

    -- select UTC_TIMESTAMP() as pvt_after_insert;
    -- select  *
    -- from    PortValTrade
    -- where   DateValId = p_DateValId
    -- and     PortId = p_PortId   
    -- order by DateValId,
    --         PortId,
    --         PathLevel,
    --         SourcePath,
    --         InstrId;

    -- select UTC_TIMESTAMP() as pvt_HIST_before_update_times;
    -- select  *
    -- from    PortValTradeHist
    -- where   DateValId = p_DateValId
    -- and     PortId = p_PortId   
    -- order by ModUtc desc;

    -- -- !!! to avoid a PK error when DML on PortValTrade, and subsequent triggered inserts into PortValTradeHist, 
    -- -- !!! we will update the ModUtc to be one second before the current ModUtc for the port, date being worked on 
    -- update  PortValTradeHist 
    -- set     InsertUtc = DATE_ADD(InsertUtc, INTERVAL -10 SECOND),
    --         ModUtc = DATE_ADD(ModUtc, INTERVAL -10 SECOND)
    -- where   DateValId = p_DateValId 
    -- and     PortId = p_PortId
    -- and     PathLevel > 1;

    -- select UTC_TIMESTAMP() as pvt_HIST_after_update_times;
    -- select  *
    -- from    PortValTradeHist
    -- where   DateValId = p_DateValId
    -- and     PortId = p_PortId   
    -- order by ModUtc desc;


    -- select UTC_TIMESTAMP() as before_last_update;

    SET v_context = 'update L1 rows to show which have been expanded';

        -- update L1 values to show which have been expanded by self joining onto PortValTrade's L>1 rows for the ame p_PortId to check 
        -- if the L1 row's InstrId is present in the L>1 rows' InstrId column. if so, set the L1 row's MarketValueIndir to 0.
        update  PortValTrade pvp
        left join    (   -- this table shows us SourceInstrId's that have been expanded and have non-zero MarketValueIndir
                select  distinct SourceInstrId
                from    PortValTrade 
                where   DateValId = p_DateValId
                and     PortId = p_PortId
                and     MarketValueIndir <> 0
                and     PathLevel > 1
                ) as expanded
                on expanded.SourceInstrId = pvp.InstrId
        set     pvp.MarketValueIndir = case when expanded.SourceInstrId is not null then 0 else pvp.MarketValueDir end,
                pvp.QuantityIndir = case when expanded.SourceInstrId is not null then 0 else pvp.QuantityDir end,
                pvp.MarketValueDir = case when PathLevel > 1 then 0 else pvp.MarketValueDir end,
                pvp.QuantityDir = case when PathLevel > 1 then 0 else pvp.QuantityDir end
        where   pvp.DateValId = p_DateValId
        and     pvp.PortId = p_PortId;

    COMMIT;

    if p_Return = 1 then
        select  DateValId,
                PortId,
                InstrId,
                SourcePath,
                SourcePathUnqId,
                SourceInstrId,
                PathLevel,
                CurrencyCode,
                -- FxRate,
                QuantityDir,
                QuantityIndir,
                -- Price, 
                MarketValueDir, 
                MarketValueIndir
        from    PortValTrade
        where   DateValId = p_DateValId
        and     PortId = p_PortId   
        order by DateValId,
                PortId,
                PathLevel,
                SourcePath,
                InstrId;
    end if;


        -- -- approach 2: 
    -- -- L1 is already in the table, start root set with L2. 
    -- -- root set will now have two sets of rows for each expanded InstrId created by making a cross product of the InstrPort rows and (1 as factor union 0 as factor): 
    --     -- 1) factor 1 rows: the intended L2 rows with scaled MVDir, MVIndir, QtyDir, QtyIndir. this result set uses the expanding InstrIds, SourcePath's, and PathLevel.
    --     -- 2) factor 0 rows: the intended L2 rows with scaled MVDir, QtyDir but 0 MVIndir, QtyIndir. this result set uses the expanded InstrIds, SourcePath's, and PathLevel.
    --         -- these rows will be summed to create a zero holding for the expanded InstrId, SourcePath, and PathLevel to be used when inserting back into 
    --         -- the PortValTrade table's ON DUPLICATE KEY UPDATE statement to ensure proper treatment of expanded positions. .
    
    --     -- incomplete, will go with approach 1 with an update to set the MVIndir to 0 for the expanded InstrId, SourcePath, and PathLevel.
    -- insert into PortValTrade (DateValId, PortId, InstrId, SourcePath, PathLevel, CurrencyCode, FxRate, QuantityDir, QuantityIndir, Price, MarketValueDir, MarketValueIndir)
    --     WITH RECURSIVE ctePVT (DateValId, PortId, factor, InstrId, SourcePath, PathLevel, CurrencyCode, FxRate, QuantityDir, QuantityIndir, Price, MarketValueDir, MarketValueIndir) AS (
            
    --         select  pvp.DateValId,
    --                 pvp.PortId,
    --                 ip.factor,
    --                 -- CAST(pvp.InstrId AS CHAR) as InstrId,
    --                 case when ip.factor = 1 then pvc.InstrId else pvp.InstrId end as InstrId,
    --                 -- CAST(pvp.PortId AS CHAR(20)) as SourcePath,
    --                 case when ip.factor = 1 then CONCAT(pvp.PortId, '/', CAST(pvc.PortId AS CHAR(20))) else pvp.SourcePath end as SourcePath,
    --                 case when ip.factor = 1 then 2 else 1 end as PathLevel,
    --                 pvp.CurrencyCode,
    --                 pvp.FxRate,
    --                 pvp.QuantityDir,
    --                 pvp.QuantityIndir,
    --                 pvp.Price,
    --                 case when ip.factor = 1 then pvc.MarketValueDir * (pvp.MarketValueDir / summv.MarketValueDir) else pvp.MarketValueDir end as MarketValueDir,
    --                 case when ip.factor = 1 then pvc.MarketValueDir * (pvp.MarketValueDir / summv.MarketValueDir) else 0 end as MarketValueIndir
    --                 -- pvp.Cost,
    --                 -- pvp.Book,
    --                 -- pvp.BookAmor,
    --                 -- pvp.LevExp,
    --                 -- pvp.LevCap,
    --                 -- pvp.PortTaxLotId,
    --                 -- pvp.PortTranId,
    --                 -- pvp.ExpireEventId,
    --                 -- pvp.InsertUtc,
    --                 -- pvp.InsertUserId,
    --                 -- 1.0 as scale_factor -- consider adding
    --         FROM    PortValTrade pvp
    --                 left join (
    --                             select  ip.InstrId,
    --                                     ip.PortId,
    --                                     ip2.factor
    --                             from    InstrPort ip
    --                                     join (
    --                                     values ROW(1), ROW(0)
    --                                     ) as ip2(factor)
    --                             ) as ip -- table is a cross product of InstrPort rows and (1 as factor union 0 as factor) so that this doubles the rows from pvc (PortVal child) but with the 
    --                                         -- rows associated with the factor = 0 have the pvp.InstrId (!instead of pvc.InstrId!) and zeroes for QuantityDir, QuantityIndir, MarketValueDir, MarketValueIndir 
    --                                         -- which are summed to create a zero holding the InstrId, SourcePath's market value so that when inserted outside of the cte, the on duplicate will
    --                                         -- update the quanty and mv fields to zero in the MarketValueIndir column. this ensures that the 
    --                     on ip.InstrId = pvp.InstrId
    --                 left join PortValTrade pvc
    --                     on pvc.PortId = ip.PortId
    --                     and pvc.PathLevel = 1
    --                 left join    (
    --                         select  spvt.PortId,
    --                                 sum(spvt.MarketValueDir) as MarketValueDir
    --                         from    PortValTrade spvt
    --                         where   spvt.DateValId = p_DateValId
    --                         and     spvt.PathLevel = 1
    --                         group by spvt.PortId
    --                         ) as summv
    --                     on summv.PortId = pvc.PortId
    --         WHERE   pvp.DateValId = p_DateValId
    --         AND     pvp.PortId = p_PortId
    --         AND     pvp.PathLevel = 1

    --         UNION ALL

    --         SELECT  pvp.DateValId,
    --                 pvp.PortId,
    --                 ip.factor,
    --                 -- CAST(pvp.InstrId AS CHAR) as InstrId,
    --                 case when ip.factor = 1 then pvc.InstrId else pvp.InstrId end as InstrId,
    --                 -- CAST(pvp.PortId AS CHAR(20)) as SourcePath,
    --                 case when ip.factor = 1 then CONCAT(pvp.PortId, '/', CAST(pvc.PortId AS CHAR(20))) else pvp.SourcePath end as SourcePath,
    --                 case when ip.factor = 1 then 2 else 1 end as PathLevel,
    --                 pvp.CurrencyCode,
    --                 pvp.FxRate,
    --                 NULL as QuantityDir,
    --                 pvp.QuantityDir * (pvp.MarketValueDir / summv.MarketValueDir),
    --                 pvp.Price,
    --                 case when ip.factor = 1 then pvc.MarketValueDir * (pvp.MarketValueDir / summv.MarketValueDir) else pvp.MarketValueDir end as MarketValueDir,
    --                 case when ip.factor = 1 then pvc.MarketValueDir * (pvp.MarketValueDir / summv.MarketValueDir) else 0 end as MarketValueIndir
    --                 -- exp.MarketValueDir * (hold.MarketValueDir / summv.MarketValueDir)
    --                 -- child.Cost * pe.scale_factor,
    --                 -- child.Book * pe.scale_factor,
    --                 -- child.BookAmor * pe.scale_factor,
    --                 -- child.LevExp * pe.scale_factor,
    --                 -- child.LevCap * pe.scale_factor,
    --                 -- child.PortTaxLotId,
    --                 -- child.PortTranId,
    --                 -- child.ExpireEventId,
    --                 -- child.InsertUtc,
    --                 -- child.InsertUserId,
    --                 -- exp.scale_factor * (exp.MarketValueDir / sum(exp.MarketValueDir)) as scale_factor -- consider adding
    --         FROM    ctePVT pvp
    --                 join (
    --                             select  ip.InstrId,
    --                                     ip.PortId,
    --                                     ip2.factor
    --                             from    InstrPort ip
    --                                     join (
    --                                     values ROW(1), ROW(0)
    --                                     ) as ip2(factor)
    --                             ) as ip 
    --                     on ip.InstrId = pvp.InstrId
    --                 join PortValTrade pvc 
    --                     on pvc.DateValId = p_DateValId
    --                     and pvc.PortId = ip.PortId
    --                     and pvc.PathLevel = 1 -- consider getting all levels, recompute all now
    --                 join    (
    --                         select  spvt.PortId,
    --                                 sum(spvt.MarketValueDir) as MarketValueDir
    --                         from    PortValTrade spvt
    --                         where   spvt.DateValId = p_DateValId
    --                         and     spvt.PathLevel = 1
    --                         group by spvt.PortId
    --                         ) as summv
    --                     on summv.PortId = pvc.PortId
    --         where   pvp.PathLevel < 10  -- prevent infinite recursion
    --         and     pvp.factor = 1
    --     )
    --     SELECT 
    --         DateValId,
    --         PortId,
    --         -- factor,
    --         InstrId,
    --         SourcePath,
    --         PathLevel,
    --         CurrencyCode,
    --         FxRate,
    --         QuantityDir,
    --         QuantityIndir,
    --         Price,
    --         MarketValueDir,
    --         MarketValueIndir
    --         -- Cost,
    --         -- Book,
    --         -- BookAmor,
    --         -- LevExp,
    --         -- LevCap,
    --         -- PortTaxLotId,
    --         -- PortTranId,
    --         -- ExpireEventId,
    --         -- InsertUtc,
    --         -- InsertUserId
    --     FROM ctePVT
    --     where factor = 1
    --     union all
    --     SELECT 
    --         DateValId,
    --         PortId,
    --         -- factor,
    --         InstrId,
    --         SourcePath,
    --         PathLevel,
    --         CurrencyCode,
    --         FxRate,
    --         SUM(QuantityDir) as QuantityDir,
    --         SUM(QuantityIndir) as QuantityIndir,
    --         Price,
    --         SUM(MarketValueDir) as MarketValueDir,
    --         SUM(MarketValueIndir) as MarketValueIndir
    --         -- Cost,
    --         -- Book,
    --         -- BookAmor,
    --         -- LevExp,
    --         -- LevCap,
    --         -- PortTaxLotId,
    --         -- PortTranId,
    --         -- ExpireEventId,
    --         -- InsertUtc,
    --         -- InsertUserId
    --     FROM ctePVT
    --     where factor = 0
    --     group by DateValId, PortId, factor, InstrId, SourcePath, PathLevel, CurrencyCode, FxRate, Price
    --     on duplicate key update QuantityDir = (QuantityDir), 
    --                             QuantityIndir = (QuantityIndir),
    --                             MarketValueDir = (MarketValueDir),
    --                             MarketValueIndir = (MarketValueIndir);
        

    
END//

-- ==========================
DROP PROCEDURE IF EXISTS usp_PortValTradeAttributes//
CREATE PROCEDURE usp_PortValTradeAttributes(
    IN p_DateValId date,
    IN p_PortId SMALLINT UNSIGNED
)
BEGIN

-- ---------------------------------------------------------------
/*
Returns the contents of the PortValTrade table for the given p_DateValId and p_PortId along with 
columns containing the Instr Name and a specific (for now) list of attributes.


Parameters: 
    p_DateValId: the date to assemble values for
    p_PortId: the id of the portfolio to assemble values for. 

Usage examples:

call usp_PortValTradeAttributes('2025-04-14', 10001);
call usp_PortValTradeAttributes('2025-04-14', 10002);
call usp_PortValTradeAttributes('2025-04-14', 10003);




*/
-- ---------------------------------------------------------------

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.ErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_PortValTradeAttributes', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', 1, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_context = 'parameter validation';

    -- Validate required parameters
    IF p_DateValId IS NULL THEN
        SET v_message = 'DateValId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    
    IF p_PortId IS NULL THEN
        SET v_message = 'PortId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;


    select  pvt.DateValId,
            pvt.PortId,
            p.UnqId as PortUnqId,
            p.Name as PortName,
            p.CurrencyCode as BaseCurrency,
            pvt.InstrId,
            i.UnqId as InstrUnqId,
            pvt.SourcePath,
            pvt.SourcePathUnqId,
            pvt.PathLevel,
            pvt.CurrencyCode as LocalCurrency,
            -- pvt.FxRate,
            pvt.QuantityDir,
            pvt.QuantityIndir,
            -- pvt.Price,
            pvt.MarketValueDir as MarketValueDirLocal,
            pvt.MarketValueDir / COALESCE(fx.Price, 1.0) as MarketValueDirBase,
            pvt.MarketValueIndir as MarketValueIndirLocal,
            pvt.MarketValueIndir / COALESCE(fx.Price, 1.0) as MarketValueIndirBase,
            i.Name,
            i.Description,
            ia.Country,
            i.CurrencyCode as Currency,
            ia.Sectype,
            ia.Seccat,
            ia.Sector,
            ia.Indust
    from    PortValTrade pvt
            join Instr i 
                on i.id = pvt.InstrId
            join Port p
                on p.id = pvt.PortId
            LEFT JOIN FxPrice fx
                ON fx.DateValId = pvt.DateValId
                AND fx.FxCurrencyCode = pvt.CurrencyCode
                AND fx.PriceSideCode = 'MID'
                AND fx.PriceTimeCode = 'CLOSE'
            left join (
                select  InstrId,
                        max(case when InstrAttributeCode = 'country' then CountryData end) as Country,
                        max(case when InstrAttributeCode = 'sectype' then VarcharData end) as Sectype,
                        max(case when InstrAttributeCode = 'seccat' then VarcharData end) as Seccat,
                        max(case when InstrAttributeCode = 'sector' then VarcharData end) as Sector,
                        max(case when InstrAttributeCode = 'indust' then VarcharData end) as Indust
                from    InstrAttributeVal 
                group by InstrId
            ) ia
                on ia.InstrId = pvt.InstrId
    where   pvt.DateValId = p_DateValId
    and     pvt.PortId = p_PortId;

END//

-- -----------------------------------------------------------------------------
-- usp_PortCostTradeAttributes
-- Returns the contents of the PortCostTrade and PortValTrade tables for the given p_DateValId and p_PortId along with 
-- columns containing the Instr Name and a specific (for now) list of attributes.
-- PortCostTrade and PortValTrade are in the portfolio Base currency which will be retrieved and returned from Port.CurrencyCode.
-- Uses Instr.CurrencyCode and FxPrice.Price to convert the Cost and MarketValue to the local currency for two additional columns.
-- Returns GainLoss, GainLossBase, and GainLossLocal columns.
-- -----------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS usp_PortCostTradeAttributes//
CREATE PROCEDURE usp_PortCostTradeAttributes(
    IN p_DateValId date,
    IN p_PortId SMALLINT UNSIGNED
)
BEGIN

-- ---------------------------------------------------------------
/*
Returns the contents of the PortCostTrade and PortValTrade tables for the given p_DateValId and p_PortId along with 
columns containing the Instr Name and a specific (for now) list of attributes.
PortCostTrade and PortValTrade are in the portfolio Base currency which will be retrieved and returned from Port.CurrencyCode.
Uses Instr.CurrencyCode and FxPrice.Price to convert the Cost and MarketValue to the local currency for two additional columns.
Returns GainLoss, GainLossBase, and GainLossLocal columns.

Parameters: 
    p_DateValId: the date to assemble values for
    p_PortId: the id of the portfolio to assemble values for. 

Usage examples:

call usp_PortCostTradeAttributes('2025-04-14', 1);
call usp_PortCostTradeAttributes('2025-04-14', 2);
call usp_PortCostTradeAttributes('2025-04-14', 3);

*/
-- ---------------------------------------------------------------

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.ErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_PortValTradeAttributes', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', 1, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_context = 'parameter validation';

    -- Validate required parameters
    IF p_DateValId IS NULL THEN
        SET v_message = 'DateValId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    IF p_PortId IS NULL THEN
        SET v_message = 'PortId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    -- Main query combining PortCostTrade and PortValTrade with currency conversion
    SELECT  pct.DateValId,
            pct.PortId,
            p.CurrencyCode as BaseCurrency,
            p.UnqId as PortUnqId,
            p.Name as PortName,
            pct.InstrId,
            i.UnqId as InstrUnqId,
            i.Name as InstrName,
            i.Description as InstrDescription,
            i.CurrencyCode as LocalCurrency,
            pvt.QuantityDir as QuantityDir,
            COALESCE(fx.Price, 1.0) as FxRate,
            pct.CostFxRate,
            pct.Cost as CostLocal,
            pct.Cost / pct.CostFxRate as CostBase,
            pvt.MarketValueDir as MarketValueLocal,
            pvt.MarketValueDir / COALESCE(fx.Price, 1.0) as MarketValueBase,
            pvt.MarketValueDir - pct.Cost as GainLossLocal,
            (pvt.MarketValueDir / COALESCE(fx.Price, 1.0)) - (pct.Cost / pct.CostFxRate) as GainLossBase,
            -- Currency gain/loss (difference between base and local)
            (((pvt.MarketValueDir/fx.Price)-(pct.Cost/pct.CostFxRate))-((pvt.MarketValueDir-pct.Cost)/fx.Price)) as CurrencyGainLoss,
            -- FX rate used for conversion
            -- warning if FX missing and 1 used in its place
            CASE WHEN fx.Price IS NULL THEN 'Warning: FX rate missing, using 1.0' ELSE '' END as FxRateWarning,
            -- Instrument attributes
            ia.Country,
            ia.Sectype,
            ia.Seccat,
            ia.Sector,
            ia.Indust
    FROM    PortCostTrade pct
            JOIN PortValTrade pvt 
                ON pvt.DateValId = pct.DateValId 
                AND pvt.PortId = pct.PortId 
                AND pvt.InstrId = pct.InstrId
                AND pvt.PathLevel = 1  -- Only level 1 for cost comparison
            JOIN Instr i 
                ON i.id = pct.InstrId
            JOIN Port p
                ON p.id = pct.PortId
            -- FX rate for currency conversion (from base currency to local currency)
            LEFT JOIN FxPrice fx
                ON fx.DateValId = pct.DateValId
                AND fx.FxCurrencyCode = pct.CurrencyCode
                AND fx.PriceSideCode = 'MID'
                AND fx.PriceTimeCode = 'CLOSE'
            -- Instrument attributes
            LEFT JOIN (
                SELECT  InstrId,
                        max(case when InstrAttributeCode = 'country' then CountryData end) as Country,
                        max(case when InstrAttributeCode = 'sectype' then VarcharData end) as Sectype,
                        max(case when InstrAttributeCode = 'seccat' then VarcharData end) as Seccat,
                        max(case when InstrAttributeCode = 'sector' then VarcharData end) as Sector,
                        max(case when InstrAttributeCode = 'indust' then VarcharData end) as Indust
                FROM    InstrAttributeVal 
                GROUP BY InstrId
            ) ia
                ON ia.InstrId = pct.InstrId
    WHERE   pct.DateValId = p_DateValId
    AND     pct.PortId = p_PortId
    ORDER BY i.Name, pct.InstrId;

END//

DROP PROCEDURE IF EXISTS usp_PortDurationTradeAttributes//
CREATE PROCEDURE usp_PortDurationTradeAttributes(
    IN p_DateValId date,
    IN p_PortId SMALLINT UNSIGNED
)
BEGIN

-- ---------------------------------------------------------------
/*
Returns the contents of the PortValTrade table with 'MODDUR' from InstrValmeasureVal for the given p_DateValId and p_PortId along with 
columns containing the Instr Name and a specific (for now) list of attributes.


Parameters: 
    p_DateValId: the date to assemble values for
    p_PortId: the id of the portfolio to assemble values for. 

Usage examples:

call usp_PortDurationTradeAttributes('2025-04-14', 1);
call usp_PortDurationTradeAttributes('2025-04-14', 2);
call usp_PortDurationTradeAttributes('2025-04-14', 3);




*/
-- ---------------------------------------------------------------

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.ErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_PortDurationTradeAttributes', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', 1, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_context = 'parameter validation';

    -- Validate required parameters
    IF p_DateValId IS NULL THEN
        SET v_message = 'DateValId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    
    IF p_PortId IS NULL THEN
        SET v_message = 'PortId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;


    select  pvt.DateValId,
            pvt.PortId,
            p.UnqId as PortUnqId,
            p.Name as PortName,
            p.CurrencyCode as BaseCurrency,
            pvt.InstrId,
            i.UnqId as InstrUnqId,
            pvt.SourcePath,
            pvt.SourcePathUnqId,
            pvt.PathLevel,
            pvt.CurrencyCode as LocalCurrency,
            pvt.QuantityDir,
            pvt.QuantityIndir,
            pvt.MarketValueDir as MarketValueDirLocal,
            pvt.MarketValueDir / COALESCE(fx.Price, 1.0) as MarketValueDirBase,
            pvt.MarketValueIndir as MarketValueIndirLocal,
            pvt.MarketValueIndir / COALESCE(fx.Price, 1.0) as MarketValueIndirBase,
            ivm.Value as ModDur,
            i.Name,
            i.Description,
            ia.Country,
            i.CurrencyCode as Currency,
            ia.Sectype,
            ia.Seccat,
            ia.Sector,
            ia.Indust
    from    PortValTrade pvt
            join Instr i 
                on i.id = pvt.InstrId
            join Port p
                on p.id = pvt.PortId
            LEFT JOIN FxPrice fx
                ON fx.DateValId = pvt.DateValId
                AND fx.FxCurrencyCode = pvt.CurrencyCode
                AND fx.PriceSideCode = 'MID'
                AND fx.PriceTimeCode = 'CLOSE'
            left join (
                select  InstrId,
                        max(case when InstrAttributeCode = 'country' then CountryData end) as Country,
                        max(case when InstrAttributeCode = 'sectype' then VarcharData end) as Sectype,
                        max(case when InstrAttributeCode = 'seccat' then VarcharData end) as Seccat,
                        max(case when InstrAttributeCode = 'sector' then VarcharData end) as Sector,
                        max(case when InstrAttributeCode = 'indust' then VarcharData end) as Indust
                from    InstrAttributeVal 
                group by InstrId
            ) ia
                on ia.InstrId = pvt.InstrId
            left join InstrValmeasureVal ivm
                on ivm.DateValId = pvt.DateValId
                and ivm.InstrId = pvt.InstrId
                and ivm.InstrValmeasureCode = 'MODDUR'
    where   pvt.DateValId = p_DateValId
    and     pvt.PortId = p_PortId;

END//

-- -----------------------------------------------------------------------------
-- Factset Output Holdings Procedure
-- Processes and outputs holdings data for Factset integration
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS usp_FactsetOutHold//

CREATE PROCEDURE usp_FactsetOutHold(
    IN p_DateValId date,
    IN p_PortId SMALLINT UNSIGNED
)
BEGIN

-- ---------------------------------------------------------------
/*
Stored procedure to process and output holdings data for Factset integration.
This procedure retrieves portfolio holdings data and formats it for external
Factset system consumption.

usage in mysql console:
call usp_FactsetOutHold('2025-04-14', 1);
call usp_FactsetOutHold('2025-04-14', null);


usage from docker:
docker exec -i bor-db mysql -u borAllAdmin -pkBu9pjz2vi bormeta -e "call usp_FactsetOutHold('2025-04-14', 1);"

Parameters: 
    p_DateValId: the date to assemble holdings data for
    p_PortId: the id of the portfolio to assemble holdings data for. if null, all portfolios are included.



*/
-- ---------------------------------------------------------------

    DECLARE v_message TEXT;
    DECLARE v_context TEXT;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO bormeta.ErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_FactsetOutHold', v_context, 
            'ERROR', 0, 'Foreign key constraint violation or other database error', 1, ROW_COUNT()
        );
        
        RESIGNAL;
    END;

    SET v_context = 'parameter validation';

    -- Validate required parameters
    IF p_DateValId IS NULL THEN
        SET v_message = 'DateValId is required';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;


    -- Validate portfolio exists if value is provided
    IF p_PortId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Port WHERE id = p_PortId) THEN
        SET v_message = CONCAT('Port with ID ', p_PortId, ' does not exist');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    -- Validate date exists in DateVal
    IF NOT EXISTS (SELECT 1 FROM DateVal WHERE id = p_DateValId) THEN
        SET v_message = CONCAT('DateVal with ID ', p_DateValId, ' does not exist');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    SET v_context = 'data processing';

    select  p.id as PortId,
            p.UnqId as PortUnqId,
            p.Name as PortName,
            DATE_FORMAT(p_DateValId, '%Y%m%d') as DateValId,
            i.UnqId as SYMBOL,  
            i.Name as NAME,
            ia.sector as ASSET_CLASS,
            pvt.QuantityDir as SHARES,
            ip.Price as PRICE,
            ip.CurrencyCode as PRICEISO,
            0 as FEE,
            fx.Price as FXRATE, -- test
            -- pc.CurrencyCode as costcurr -- test,
            pc.Cost * fx.Price as TOT_COST,
            pvt.MarketValueDir * fx.Price as EMV,
            pc.Cost as TOT_COST_BASE,
            pac.Accrued * fx.Price as ACCR,
            pvt.MarketValueDir as EMV_BASE,
            pac.Accrued as ACCR_BASE,
            pc.Cost as TOT_COST_BASE,
            0 as FEE_BASE
    from    Port p
            join PortValTrade pvt
                on pvt.PortId = p.id
                and pvt.DateValId = p_DateValId
            join Instr i
                on i.id = pvt.InstrId
                and pvt.PathLevel = 1
            left join FxPrice fx
                on fx.BaseCurrencyCode = 'CAD'
                and fx.FxCurrencyCode = pvt.CurrencyCode
                and fx.DateValId = p_DateValId
                and fx.PriceSideCode = 'MID'
                and fx.PriceTimeCode = 'CLOSE'
                and fx.DataSourceId = 6 -- use mellon for now
            join (
                select  InstrId,
                        max(case when InstrAttributeCode = 'country' then CountryData end) as country,
                        max(case when InstrAttributeCode = 'sectype' then VarcharData end) as sectype,
                        max(case when InstrAttributeCode = 'seccat' then VarcharData end) as category,
                        max(case when InstrAttributeCode = 'sector' then VarcharData end) as sector,
                        max(case when InstrAttributeCode = 'indust' then VarcharData end) as industry
                from    InstrAttributeVal 
                group by InstrId
            ) ia
                on ia.InstrId = pvt.InstrId
            left join InstrPrice ip
                on ip.InstrId = pvt.InstrId
                and ip.DateValId = p_DateValId
                and ip.PriceSideCode = 'MID'
                and ip.PriceTimeCode = 'CLOSE'
                and ip.DataSourceId = 6 -- use mellon for now
            left join PortCostTrade pc
                on pc.PortId = pvt.PortId
                and pc.InstrId = pvt.InstrId
                and pc.DateValId = p_DateValId
            left join PortAccruedTrade pac
                on pac.PortId = pvt.PortId
                and pac.InstrId = pvt.InstrId
                and pac.DateValId = p_DateValId
    where   (p.id = p_PortId or p_PortId is null);


END//

-- -----------------------------------------------------------------------------
-- usp_Port_Get_Attributes
-- Returns the contents of the Port table for the given p_PortId with PortAttributeVal information for all attributes
-- in PortAttribute.
-- -----------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS usp_Port_Get_Attributes//

CREATE PROCEDURE usp_Port_Get_Attributes(
    IN p_PortId SMALLINT UNSIGNED
)
BEGIN
    /*
    ====================================================================================
    Procedure: usp_Port_Get_Attributes
    Description: Retrieves portfolio attributes for a given portfolio ID
    Parameters:
        p_PortId - Portfolio ID to retrieve attributes for
    Returns: Portfolio attributes including type, description, and other metadata
    Author: System
    Created: 2025-01-27
    Usage: 
    call usp_Port_Get_Attributes(1);
    ====================================================================================
    */
    
    -- Variable declarations (must come before handlers)
    DECLARE v_context VARCHAR(100) DEFAULT 'usp_Port_Get_Attributes';
    DECLARE v_message TEXT;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO bormeta.ErrorLog (
            error_utc, procedure_name, error_context, sql_state, 
            error_code, error_message, user_id, affected_rows
        ) VALUES (
            UTC_TIMESTAMP(), 'usp_Port_Get_Attributes', v_context, 
            'ERROR', 0, CONCAT('Error in usp_Port_Get_Attributes - PortId: ', IFNULL(p_PortId, 'NULL')), IFNULL(p_PortId, 1), ROW_COUNT()
        );
        RESIGNAL;
    END;
    
    SET v_context = 'parameter validation';
    
    -- Parameter validation
    IF p_PortId IS NULL THEN
        SET v_message = 'PortId parameter cannot be NULL';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    -- Check if portfolio exists
    IF NOT EXISTS (SELECT 1 FROM borinst.Port WHERE id = p_PortId) THEN
        SET v_message = CONCAT('Portfolio with ID ', p_PortId, ' does not exist');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;
    
    SET v_context = 'selecting portfolio attributes';
    
    SELECT  p.id as PortId,
            p.UnqId as PortUnqId,
            p.Name as PortName,
            p.Description as PortDescription,
            p.CurrencyCode as BaseCurrencyCode,
            p.CountryCode,
            pa.code as PortAttributeCode,
            pa.Name as PortAttributeName,
            pa.Description as PortAttributeDescription,
            pa.AttributeTypeCode as PortAttributeTypeCode,
            pav.VarcharData,
            pav.IntData,
            pav.FloatData,
            pav.DateData,
            pav.BooleanData,
            pav.CountryData,
            pav.CurrencyData
    FROM    borinst.Port p
            LEFT JOIN borinst.PortAttributeVal pav 
                ON pav.PortId = p.id
            LEFT JOIN borinst.PortAttribute pa
                ON pa.code = pav.PortAttributeCode
    WHERE   p.id = p_PortId
    ORDER BY pav.PortAttributeCode;
    
    
END//




DELIMITER ;