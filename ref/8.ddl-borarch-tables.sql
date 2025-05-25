/*

usage (from .../cbor/ directory): 
mysql -u borarchAdmin -pkBu9pjz2vi < ./script/init/8.ddl-borarch-tables.sql

run on the docker bor-db container:
cat ./script/init/8.ddl-borarch-tables.sql | docker exec -i bor-db mysql -u borAllAdmin -pkBu9pjz2vi borarch

TODO 


yyyymmdd   user          description
--------   ------------  ------------------------------------------------------------
20250513   RMenning      added User table

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


use bormeta;

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

  SELECT * FROM temp_FundClassFee;

END//

DELIMITER ;

use borarch;

DROP TABLE IF EXISTS holdweb;
CREATE TABLE holdweb (
    date DATE NOT NULL,
    fund_name VARCHAR(128) NOT NULL,
    fund_code VARCHAR(16) NOT NULL,
    issuer VARCHAR(255) NOT NULL,
    issue VARCHAR(255),
    currency VARCHAR(8),
    units BIGINT,
    cost DECIMAL(20,2),
    mv DECIMAL(20,2)
);
