-- usage 
-- (from .../src/ directory): 
-- mysql -u root -pc7Azy < ./script/init/1.ddl-create-db.sql

-- run on the docker bor-db container:
-- cat ./script/init/1.ddl-create-db.sql | docker exec -i bor-db mysql -u root -pc7Azy
-- Add this setting to allow trigger creation without SUPER privilege
SET GLOBAL log_bin_trust_function_creators = 1;


-- bor database ---------------------------------------------------
DROP DATABASE IF EXISTS bor;  
CREATE DATABASE IF NOT EXISTS bor
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

use bor;

-- Check if the admin user exists, and create it if it doesn't
-- drop user if exists 'borAdmin'@'localhost';
-- CREATE USER IF NOT EXISTS 'borAdmin'@'localhost' IDENTIFIED BY 'Fc_JeY*szZsrVcVY*n82';
-- ALTER USER 'borAdmin'@'localhost' IDENTIFIED WITH mysql_native_password BY 'Fc_JeY*szZsrVcVY*n82'; -- for prisma:
-- GRANT ALL PRIVILEGES ON bor.* TO 'borAdmin'@'localhost';
-- GRANT SUPER ON *.* TO 'borAdmin'@'localhost'; -- for mysql 8.0
-- GRANT CREATE ON *.* TO 'borAdmin'@'localhost'; -- FOR PRISMA

-- NOTE: mysql_native_password is required for Prisma ORM which is used the application for OAuth2 authentication.
-- Create user that can connect from any host
CREATE USER IF NOT EXISTS 'borAdmin'@'%' IDENTIFIED WITH mysql_native_password BY 'Aye3aBYrXF';
GRANT ALL PRIVILEGES ON bor.* TO 'borAdmin'@'%';
FLUSH PRIVILEGES;

-- create a "normal" MySQL user for the application with typical grants for a user
CREATE USER IF NOT EXISTS 'borSvcUser'@'%' IDENTIFIED BY 'xYFhNWJZ4g';
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON bor.* TO 'borSvcUser'@'%';
FLUSH PRIVILEGES;


-- borinst database ---------------------------------------------------
DROP DATABASE IF EXISTS borinst;

CREATE DATABASE IF NOT EXISTS borinst
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- create a "normal" MySQL user for the application with typical grants for a user
CREATE USER IF NOT EXISTS 'borinstAdmin'@'%' IDENTIFIED BY 'kBu9pjz2vi';
GRANT ALL PRIVILEGES ON borinst.* TO 'borinstAdmin'@'%';
FLUSH PRIVILEGES;

-- svc user
CREATE USER IF NOT EXISTS 'borinstSvcUser'@'%' IDENTIFIED BY 'u67nyNgomZ';
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON borinst.* TO 'borinstSvcUser'@'%';
FLUSH PRIVILEGES;


-- borarch database for archive and not vital files 
-- ---------------------------------------------------
DROP DATABASE IF EXISTS borarch;
CREATE DATABASE IF NOT EXISTS borarch
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- create an admin and service user for the borarch database
CREATE USER IF NOT EXISTS 'borarchAdmin'@'%' IDENTIFIED BY 'kBu9pjz2vi';
GRANT ALL PRIVILEGES ON borarch.* TO 'borarchAdmin'@'%';
FLUSH PRIVILEGES;

-- create a service user for the borarch database
CREATE USER IF NOT EXISTS 'borarchSvcUser'@'%' IDENTIFIED BY 'u67nyNgomZ';
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON borarch.* TO 'borarchSvcUser'@'%';
FLUSH PRIVILEGES;


-- bormeta database for metadata about the bor system
-- ---------------------------------------------------
DROP DATABASE IF EXISTS bormeta;
CREATE DATABASE IF NOT EXISTS bormeta
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- create an admin and service user for the bormeta database
CREATE USER IF NOT EXISTS 'bormetaAdmin'@'%' IDENTIFIED BY 'kBu9pjz2vi';
GRANT ALL PRIVILEGES ON bormeta.* TO 'bormetaAdmin'@'%';
FLUSH PRIVILEGES;

-- create a service user for the bormeta database
CREATE USER IF NOT EXISTS 'bormetaSvcUser'@'%' IDENTIFIED BY 'u67nyNgomZ';
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON bormeta.* TO 'bormetaSvcUser'@'%';
FLUSH PRIVILEGES;


-- all database access users ---------------------------------------------------
-- create a user that has access to all databases with admin privileges
CREATE USER IF NOT EXISTS 'borAllAdmin'@'%' IDENTIFIED BY 'kBu9pjz2vi';
GRANT ALL PRIVILEGES ON *.* TO 'borAllAdmin'@'%';
FLUSH PRIVILEGES;

-- create a user that has access to all databases with service privileges
CREATE USER IF NOT EXISTS 'borAllSvc'@'%' IDENTIFIED BY 'u67nyNgomZ';
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON *.* TO 'borAllSvc'@'%';
FLUSH PRIVILEGES;

-- sample connection using docker container:
-- docker exec -it bor-db mysql -u borAdmin -pAye3aBYrXF -e "USE bor; SELECT COUNT(*) FROM Role;"
-- docker exec -it bor-db mysql -u borAllAdmin -pborAllAdmin -e "USE bor; SELECT COUNT(*) FROM Role;"
-- docker exec -it bor-db mysql -u borAllSvc -pborAllSvc -e "USE bor; SELECT COUNT(*) FROM Role;"

-- Show created databases to verify
SHOW DATABASES;
