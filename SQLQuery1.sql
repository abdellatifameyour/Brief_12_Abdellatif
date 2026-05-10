CREATE DATABASE DataWarehouse;
USE DataWarehouse;
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

if OBJECT_ID('Bronze.account', 'U') IS NOT NULL 
Drop TABLE Bronze.account

create table Bronze.account (
account_number INT,
account_name VARCHAR(100),
account_type VARCHAR(100),
currency VARCHAR(100)
);

create table Bronze.store (
store_code VARCHAR(255),
country VARCHAR(255),
region VARCHAR(255) 
);



create table Bronze.storemaster (
store_code NVARCHAR(50),
store_name NVARCHAR(50),
store_type NVARCHAR(50)
);

create table Bronze.account_mapping(
AccountNumber INT,
AccountName NVARCHAR(50),
PLLine NVARCHAR(50),
StatementType NVARCHAR(50),
SortOrder NVARCHAR(50),
Notes NVARCHAR(100)
);


create table Bronze.gltransaction (
transaction_id INT,
transaction_date DATE ,
store_code NVARCHAR (100),
account_number NVARCHAR(100),
amount_local DECIMAL(18,2),
currency NVARCHAR(50),
document_number NVARCHAR(50),
M_description NVARCHAR(100)
);

BULK INSERT Bronze.account
from 'C:\Users\info\Downloads\Compressed\data\account.csv'
WITH (
    FIRSTROW = 2,              -- ignorer header
    FIELDTERMINATOR = ',',     -- séparateur CSV
    TABLOCK
);



bulk insert Bronze.store 
from 'C:\Users\info\Downloads\Compressed\data\store.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);



BULK INSERT Bronze.account_mapping
from 'C:\Users\info\Downloads\Compressed\data\account_mapping.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);




bulk insert Bronze.storemaster
from 'C:\Users\info\Downloads\Compressed\data\store_master.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);




bulk insert Bronze.gltransaction
from 'C:\Users\info\Downloads\Compressed\data\transaction.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);

select * from Bronze.account
select * from Bronze.gltransaction
select * from Bronze.storemaster
select * from Bronze.store
select * from Bronze.account_mapping


create table silver.account (
account_number INT,
account_name VARCHAR(100),
account_type VARCHAR(100),
currency VARCHAR(100)
);

create table silver.store (
store_code VARCHAR(255),
country VARCHAR(255),
region VARCHAR(255) 
);

create table silver.storemaster (
store_code NVARCHAR(50),
store_name NVARCHAR(50),
store_type NVARCHAR(50)
);

create table silver.account_mapping(
AccountNumber INT,
AccountName NVARCHAR(50),
PLLine NVARCHAR(50),
StatementType NVARCHAR(50),
SortOrder NVARCHAR(50),
Notes NVARCHAR(100)
);


create table silver.gltransaction (
transaction_id INT,
transaction_date DATE ,
store_code NVARCHAR (100),
account_number NVARCHAR(100),
amount_local DECIMAL(18,2),
currency NVARCHAR(50),
document_number NVARCHAR(50),
M_description NVARCHAR(100)
);

TRUNCATE TABLE silver.store;
        INSERT INTO silver.store (
         store_code,
         country,
         region 
        )
        SELECT
            UPPER(TRIM(store_code)),
            TRIM(country),
            TRIM(region)
        FROM bronze.store;



TRUNCATE TABLE silver.storemaster;
        INSERT INTO silver.storemaster (
            store_code,
            store_name,
            store_type
        )
        SELECT
            UPPER(TRIM(store_code)),
            TRIM(store_name),
            TRIM(store_type)
        FROM bronze.storemaster;

        --------------------------------------------------
        -- 4. GL TRANSACTION (IMPORTANT)
        --------------------------------------------------
        TRUNCATE TABLE silver.gltransaction;
        INSERT INTO silver.gltransaction (
            transaction_id,
            transaction_date,
            store_code,
            account_number,
            amount_local,
            currency,
            document_number,
            M_description
        )
        SELECT
            transaction_id,
            CAST(transaction_date AS DATE),
            UPPER(TRIM(store_code)),
            account_number,
            amount_local,
            UPPER(TRIM(currency)),
            TRIM(document_number),
            TRIM(M_description)
        FROM bronze.gltransaction
        WHERE transaction_id IS NOT NULL;

        --------------------------------------------------
        -- 5. ACCOUNT MAPPING (IMPORTANT)
        --------------------------------------------------

        TRUNCATE TABLE silver.account_mapping;
        INSERT INTO silver.account_mapping (
            AccountNumber,
            AccountName,
            PLLine,
            StatementType,
            SortOrder,
            Notes
        )
        SELECT
            AccountNumber,
            TRIM(AccountName),
            TRIM(PLLine),
            TRIM(StatementType),
            TRIM(SortOrder),
            TRIM(Notes)
        FROM Bronze.account_mapping;



        TRUNCATE TABLE silver.account;
        INSERT INTO silver.account (
            account_number,
            account_name,
            account_type,
            currency
        )
        SELECT
            account_number,
            TRIM(account_name),
            TRIM(account_type),
            TRIM(currency)
        FROM Bronze.account;

select * from silver.account
select * from silver.gltransaction
select * from silver.storemaster
select * from silver.store
select * from silver.account_mapping



CREATE VIEW gold.dimaccount AS
SELECT
a.account_number,
a.account_name,
a.account_type,
a.currency ,
am.AccountName,
am.PLLine ,
am.StatementType ,
am.SortOrder ,
am.Notes
from silver.account as a
LEFT JOIN  silver.account_mapping as am
on a.account_number=am.AccountNumber;

SELECT * FROM gold.dimaccount;


CREATE VIEW gold.dimstore AS
SELECT
  s.store_code,
  s.country,
  s.region,
  sm.store_name,
  sm.store_type
from silver.store as s
left join silver.storemaster as sm
on s.store_code=sm.store_code;

select *from gold.dimstore


CREATE VIEW gold.fact_gl AS
SELECT
    t.transaction_id,
    t.transaction_date,
    t.store_code,
    t.account_number,
    t.amount_local,

    t.currency AS transaction_currency,

    t.document_number,
    t.M_description,

    vs.country,
    vs.region,
    vs.store_name,
    vs.store_type,

    va.account_name,
    va.account_type,

    va.currency AS account_currency,

    va.PLLine,
    va.StatementType,
    va.SortOrder,
    va.Notes

FROM silver.gltransaction AS t

LEFT JOIN gold.dimstore AS vs
    ON t.store_code = vs.store_code

LEFT JOIN gold.dimaccount AS va
    ON t.account_number = va.account_number;

SELECT *FROM gold.fact_gl

SELECT
    account_number,
    COUNT(*) AS nb
FROM silver.account
GROUP BY account_number
HAVING COUNT(*) > 1;

SELECT
    store_code,
    COUNT(*) AS nb
FROM silver.store
GROUP BY store_code
HAVING COUNT(*) > 1;

SELECT
    transaction_id,
    COUNT(*) AS nb
FROM silver.gltransaction
GROUP BY transaction_id
HAVING COUNT(*) > 1;

SELECT *
FROM silver.gltransaction
WHERE account_number IS NULL;