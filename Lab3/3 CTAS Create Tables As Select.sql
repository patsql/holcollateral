/*
We will load the data from external table into SQLPool01 (dedicated SQL pool).
After this step, the data will reside in the SQL MPP engine, and you can benefit from all the enterprise DW facilities.
This will take 4.5 minutes.
Run the code starting at line 20 and while it runs read the following:

Important: I am running the load under the default resource class = smallrc, 
which gives to my current user access to 3% of the resources (DWU).
Recommended: run loads with a user who is part of a higher resource class.
The higheest RC, xlargerc, gives access to 70% of the resources.

You can check what resource class a request is using, as well as the active requests:  
SELECT resource_class, * FROM sys.dm_pdw_exec_requests as r
WHERE 	r.[label] like 'CTAS%';

resource: https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/resource-classes-for-workload-management#dynamic-resource-classes

*/

-- After the script completes, go back to the script "1 Create external data source.sql"

-- Connect to SQLPool01
CREATE SCHEMA [cso];
GO

-- Load the data
-- To load the data we use one CTAS statement per table.
-- After running the below statements, check the tables under SQLpool01 > Tables

CREATE TABLE [cso].[DimAccount]            
WITH (DISTRIBUTION = ROUND_ROBIN)   
AS SELECT * FROM [asb].[DimAccount]             
OPTION (LABEL = 'CTAS : Load [cso].[DimAccount]');


CREATE TABLE [cso].[DimChannel]            WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimChannel]             OPTION (LABEL = 'CTAS : Load [cso].[DimChannel]             ');
CREATE TABLE [cso].[DimCurrency]           WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimCurrency]            OPTION (LABEL = 'CTAS : Load [cso].[DimCurrency]            ');
CREATE TABLE [cso].[DimCustomer]           WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimCustomer]            OPTION (LABEL = 'CTAS : Load [cso].[DimCustomer]            ');
CREATE TABLE [cso].[DimDate]               WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimDate]                OPTION (LABEL = 'CTAS : Load [cso].[DimDate]                ');
CREATE TABLE [cso].[DimEmployee]           WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimEmployee]            OPTION (LABEL = 'CTAS : Load [cso].[DimEmployee]            ');
CREATE TABLE [cso].[DimEntity]             WITH (DISTRIBUTION = HASH([EntityKey]   ) ) AS SELECT * FROM [asb].[DimEntity]              OPTION (LABEL = 'CTAS : Load [cso].[DimEntity]              ');
CREATE TABLE [cso].[DimGeography]          WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimGeography]           OPTION (LABEL = 'CTAS : Load [cso].[DimGeography]           ');
CREATE TABLE [cso].[DimMachine]            WITH (DISTRIBUTION = HASH([MachineKey]  ) ) AS SELECT * FROM [asb].[DimMachine]             OPTION (LABEL = 'CTAS : Load [cso].[DimMachine]             ');
CREATE TABLE [cso].[DimOutage]             WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimOutage]              OPTION (LABEL = 'CTAS : Load [cso].[DimOutage]              ');
CREATE TABLE [cso].[DimProduct]            WITH (DISTRIBUTION = HASH([ProductKey]  ) ) AS SELECT * FROM [asb].[DimProduct]             OPTION (LABEL = 'CTAS : Load [cso].[DimProduct]             ');
CREATE TABLE [cso].[DimProductCategory]    WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimProductCategory]     OPTION (LABEL = 'CTAS : Load [cso].[DimProductCategory]     ');
CREATE TABLE [cso].[DimScenario]           WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimScenario]            OPTION (LABEL = 'CTAS : Load [cso].[DimScenario]            ');
CREATE TABLE [cso].[DimPromotion]          WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimPromotion]           OPTION (LABEL = 'CTAS : Load [cso].[DimPromotion]           ');
CREATE TABLE [cso].[DimProductSubcategory] WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimProductSubcategory]  OPTION (LABEL = 'CTAS : Load [cso].[DimProductSubcategory]  ');
CREATE TABLE [cso].[DimSalesTerritory]     WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimSalesTerritory]      OPTION (LABEL = 'CTAS : Load [cso].[DimSalesTerritory]      ');
CREATE TABLE [cso].[DimStore]              WITH (DISTRIBUTION = ROUND_ROBIN        )   AS SELECT * FROM [asb].[DimStore]               OPTION (LABEL = 'CTAS : Load [cso].[DimStore]               ');
CREATE TABLE [cso].[FactITMachine]         WITH (DISTRIBUTION = HASH([MachineKey]  ) ) AS SELECT * FROM [asb].[FactITMachine]          OPTION (LABEL = 'CTAS : Load [cso].[FactITMachine]          ');
CREATE TABLE [cso].[FactITSLA]             WITH (DISTRIBUTION = HASH([MachineKey]  ) ) AS SELECT * FROM [asb].[FactITSLA]              OPTION (LABEL = 'CTAS : Load [cso].[FactITSLA]              ');
CREATE TABLE [cso].[FactInventory]         WITH (DISTRIBUTION = HASH([ProductKey]  ) ) AS SELECT * FROM [asb].[FactInventory]          OPTION (LABEL = 'CTAS : Load [cso].[FactInventory]          ');
CREATE TABLE [cso].[FactOnlineSales]       WITH (DISTRIBUTION = HASH([ProductKey]  ) ) AS SELECT * FROM [asb].[FactOnlineSales]        OPTION (LABEL = 'CTAS : Load [cso].[FactOnlineSales]        ');
CREATE TABLE [cso].[FactSales]             WITH (DISTRIBUTION = HASH([ProductKey]  ) ) AS SELECT * FROM [asb].[FactSales]              OPTION (LABEL = 'CTAS : Load [cso].[FactSales]              ');
CREATE TABLE [cso].[FactSalesQuota]        WITH (DISTRIBUTION = HASH([ProductKey]  ) ) AS SELECT * FROM [asb].[FactSalesQuota]         OPTION (LABEL = 'CTAS : Load [cso].[FactSalesQuota]         ');
CREATE TABLE [cso].[FactStrategyPlan]      WITH (DISTRIBUTION = HASH([EntityKey])  )   AS SELECT * FROM [asb].[FactStrategyPlan]       OPTION (LABEL = 'CTAS : Load [cso].[FactStrategyPlan]       ');
CREATE TABLE [cso].[FactExchangeRate]      WITH (DISTRIBUTION = HASH([ExchangeRateKey])  ) AS SELECT * FROM [asb].[FactExchangeRate]    OPTION (LABEL = 'CTAS : Load [cso].[FactExchangeRate]       ');

