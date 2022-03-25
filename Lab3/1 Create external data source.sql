/* 

GOAL: load CSV files into a SQL dedicated pool using Polybase.
The CSV files are place in a public Azure storage account.

Polybase technology uses few entities to make possible the copy:
i) DATABASE SCOPED CREDENTIAL --> to encapsulate the storage account key, in case connecting to a private storage account
ii) EXTERNAL DATA SOURCE      --> pointer to the Azure storage account where the files are stored
iii) EXTERNAL FILE FORMAT     --> description of the format of the files (CSV, parquet, delta)
iv) CREATE EXTERNAL TABLE     --> the virtualization of the file, combining the above entities. This is a pointer to the file.

Obs: Because the storage account is public, there is no need to authenticate to the storage account using a credential.
If I were to connect to a private storage account (owned by my company), 
then I would need to autheticate using the storage account's primary key.

*/

-- To understand this process, please analyze the following lines until line 55 -- don't run the code.

-- STEP 0: run the below query if you are loading data from a private storage account
-- A: Create a master key.
-- Only necessary if one doesn't already exist.
-- The master key is required to encrypt the credential secret
-- Connect to SQLPool01

-- check if the db already has a master key:
SELECT *
FROM sys.symmetric_keys AS SK
WHERE SK.name = '##MS_DatabaseMasterKey##';

--if not, create it:
CREATE MASTER KEY;

-- B: Create a database scoped credential
-- IDENTITY: Provide any string, it is not used for authentication to Azure storage.
-- SECRET: Provide your Azure storage account key.
-- Obs: SAS cannot be used with PolyBase in SQL Server, APS or Azure Synapse Analytics.  That's why we use the account key
CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH
    IDENTITY = 'user', 
    SECRET = '<azure_storage_account_key>' -- account key of the storage account
;

-- C: Create an external data source
-- TYPE: HADOOP - PolyBase uses Hadoop APIs to access data in Azure blob storage.
-- LOCATION: Provide Azure storage account name and blob container name.
-- CREDENTIAL: Provide the credential created in the previous step.
-- Notice that we call the credential in the EXTERNAL DATA SOURCE statement.
-- For our lab, you will see that we omit this field as we are reading from a public dataset.
CREATE EXTERNAL DATA SOURCE AzureStorage
WITH (
    TYPE = HADOOP,
    LOCATION = 'wasbs://<blob_container_name>@<azure_storage_account_name>.blob.core.windows.net',
    CREDENTIAL = AzureStorageCredential
);

--=======================================================
-- The lab starts here
--=======================================================
-- STEP 1.1:
-- Connect to SQLPool01, database SQLPool01
CREATE EXTERNAL DATA SOURCE AzureStorage
WITH
(  
    TYPE = HADOOP -- PolyBase uses Hadoop APIs to access data in ADLS
,   LOCATION = 'wasbs://contosoretaildw-tables@contosoretaildw.blob.core.windows.net/' -- connectivity protocol and path to the external data source
);
-- Check that this is created under Data > SQLPOOL01 > External resources > External data sources

-- STEP 1.2:
CREATE EXTERNAL FILE FORMAT TextFileFormat
WITH
(   FORMAT_TYPE = DELIMITEDTEXT
,    FORMAT_OPTIONS    (   FIELD_TERMINATOR = '|'
                    ,    STRING_DELIMITER = ''
                    ,    DATE_FORMAT         = 'yyyy-MM-dd HH:mm:ss.fff'
                    ,    USE_TYPE_DEFAULT = FALSE
                    )
);
-- Check that this is created under SQLPOOL01 > External resources > External file formats

-- STEP 2: Create External Tables
-- At this stage we will "load" the data from the storage account into external tables
-- Run the script "2 Create External tables.sql"

-- STEP 3: CTAS = Create Tables As Select
-- Now we will load the data from external table into SQLPool01 (our dedicated SQL pool)
-- After this, the data will reside in the SQL MPP engine, and you can benefit from all the enterprise DW facilities
-- Run the script "3 CTAS tables for Data load.sql"

-- STEP 4: track loading progreess
-- Run the script "4 Track loading"

-- STEP 5: post-loading activities
-- Run the script "5 Post loading activities"

-- END of the lab

-- tutorial links: https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-load-from-azure-blob-storage-with-polybase
-- https://github.com/microsoft/sql-server-samples/blob/master/samples/databases/contoso-data-warehouse/load-contoso-data-warehouse-to-sql-data-warehouse.sql

