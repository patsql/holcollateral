/*
Just like we maintain a SQL database, for a SQL pool we have to rebuild indexes and update statistics.

1) Optimize columnstore compression
By default, dedicated SQL pools store the table as a clustered columnstore index. 
After a load completes, some of the data rows might not be compressed into the columnstore.

To optimize query performance and columnstore compression after a load, 
rebuild the table to force the columnstore index to compress all the rows.
*/

-- This takes 45 seconds
-- Connect to SQLPool01
GO
ALTER INDEX ALL ON [cso].[FactStrategyPlan]         REBUILD;
ALTER INDEX ALL ON [cso].[DimAccount]               REBUILD;
ALTER INDEX ALL ON [cso].[DimChannel]               REBUILD;
ALTER INDEX ALL ON [cso].[DimCurrency]              REBUILD;
ALTER INDEX ALL ON [cso].[DimCustomer]              REBUILD;
ALTER INDEX ALL ON [cso].[DimDate]                  REBUILD;
ALTER INDEX ALL ON [cso].[DimEmployee]              REBUILD;
ALTER INDEX ALL ON [cso].[DimEntity]                REBUILD;
ALTER INDEX ALL ON [cso].[DimGeography]             REBUILD;
ALTER INDEX ALL ON [cso].[DimMachine]               REBUILD;
ALTER INDEX ALL ON [cso].[DimOutage]                REBUILD;
ALTER INDEX ALL ON [cso].[DimProduct]               REBUILD;
ALTER INDEX ALL ON [cso].[DimProductCategory]       REBUILD;
ALTER INDEX ALL ON [cso].[DimScenario]              REBUILD;
ALTER INDEX ALL ON [cso].[DimPromotion]             REBUILD;
ALTER INDEX ALL ON [cso].[DimProductSubcategory]    REBUILD;
ALTER INDEX ALL ON [cso].[DimSalesTerritory]        REBUILD;
ALTER INDEX ALL ON [cso].[DimStore]                 REBUILD;
ALTER INDEX ALL ON [cso].[FactITMachine]            REBUILD;
ALTER INDEX ALL ON [cso].[FactITSLA]                REBUILD;
ALTER INDEX ALL ON [cso].[FactInventory]            REBUILD;
ALTER INDEX ALL ON [cso].[FactOnlineSales]          REBUILD;
ALTER INDEX ALL ON [cso].[FactSales]                REBUILD;
ALTER INDEX ALL ON [cso].[FactSalesQuota]           REBUILD;
ALTER INDEX ALL ON [cso].[FactExchangeRate]         REBUILD;


/*
2) Optimize statistics
It's best to create single-column statistics immediately after a load. 
If you know certain columns aren't going to be in query predicates, you can skip creating statistics on those columns.
If you create single-column statistics on every column, it might take a long time to rebuild all the statistics.
If you decide to create single-column statistics on every column of every table, 
you can use the stored procedure code sample prc_sqldw_create_stats from here:https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-statistics#use-a-stored-procedure-to-create-statistics-on-all-columns-in-a-sql-pool

The following example is a good starting point for creating statistics. 
It creates single-column statistics on each column in the dimension table, and on each joining column in the fact tables. 
You can always add single or multi-column statistics to other fact table columns later on.
*/

-- This takes 3 minutes

CREATE STATISTICS [stat_cso_DimMachine_DecommissionDate] ON [cso].[DimMachine]([DecommissionDate]);
CREATE STATISTICS [stat_cso_DimMachine_ETLLoadID] ON [cso].[DimMachine]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimMachine_LastModifiedDate] ON [cso].[DimMachine]([LastModifiedDate]);
CREATE STATISTICS [stat_cso_DimMachine_LoadDate] ON [cso].[DimMachine]([LoadDate]);
CREATE STATISTICS [stat_cso_DimMachine_MachineDescription] ON [cso].[DimMachine]([MachineDescription]);
CREATE STATISTICS [stat_cso_DimMachine_MachineHardware] ON [cso].[DimMachine]([MachineHardware]);
CREATE STATISTICS [stat_cso_DimMachine_MachineKey] ON [cso].[DimMachine]([MachineKey]);
CREATE STATISTICS [stat_cso_DimMachine_MachineLabel] ON [cso].[DimMachine]([MachineLabel]);
CREATE STATISTICS [stat_cso_DimMachine_MachineName] ON [cso].[DimMachine]([MachineName]);
CREATE STATISTICS [stat_cso_DimMachine_MachineOS] ON [cso].[DimMachine]([MachineOS]);
CREATE STATISTICS [stat_cso_DimMachine_MachineSoftware] ON [cso].[DimMachine]([MachineSoftware]);
CREATE STATISTICS [stat_cso_DimMachine_MachineSource] ON [cso].[DimMachine]([MachineSource]);
CREATE STATISTICS [stat_cso_DimMachine_MachineType] ON [cso].[DimMachine]([MachineType]);
CREATE STATISTICS [stat_cso_DimMachine_ServiceStartDate] ON [cso].[DimMachine]([ServiceStartDate]);
CREATE STATISTICS [stat_cso_DimMachine_Status] ON [cso].[DimMachine]([Status]);
CREATE STATISTICS [stat_cso_DimMachine_StoreKey] ON [cso].[DimMachine]([StoreKey]);
CREATE STATISTICS [stat_cso_DimMachine_UpdateDate] ON [cso].[DimMachine]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimMachine_VendorName] ON [cso].[DimMachine]([VendorName]);
CREATE STATISTICS [stat_cso_DimOutage_ETLLoadID] ON [cso].[DimOutage]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimOutage_LoadDate] ON [cso].[DimOutage]([LoadDate]);
CREATE STATISTICS [stat_cso_DimOutage_OutageDescription] ON [cso].[DimOutage]([OutageDescription]);
CREATE STATISTICS [stat_cso_DimOutage_OutageKey] ON [cso].[DimOutage]([OutageKey]);
CREATE STATISTICS [stat_cso_DimOutage_OutageLabel] ON [cso].[DimOutage]([OutageLabel]);
CREATE STATISTICS [stat_cso_DimOutage_OutageName] ON [cso].[DimOutage]([OutageName]);
CREATE STATISTICS [stat_cso_DimOutage_OutageSubType] ON [cso].[DimOutage]([OutageSubType]);
CREATE STATISTICS [stat_cso_DimOutage_OutageSubTypeDescription] ON [cso].[DimOutage]([OutageSubTypeDescription]);
CREATE STATISTICS [stat_cso_DimOutage_OutageType] ON [cso].[DimOutage]([OutageType]);
CREATE STATISTICS [stat_cso_DimOutage_OutageTypeDescription] ON [cso].[DimOutage]([OutageTypeDescription]);
CREATE STATISTICS [stat_cso_DimOutage_UpdateDate] ON [cso].[DimOutage]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimProductCategory_ETLLoadID] ON [cso].[DimProductCategory]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimProductCategory_LoadDate] ON [cso].[DimProductCategory]([LoadDate]);
CREATE STATISTICS [stat_cso_DimProductCategory_ProductCategoryDescription] ON [cso].[DimProductCategory]([ProductCategoryDescription]);
CREATE STATISTICS [stat_cso_DimProductCategory_ProductCategoryKey] ON [cso].[DimProductCategory]([ProductCategoryKey]);
CREATE STATISTICS [stat_cso_DimProductCategory_ProductCategoryLabel] ON [cso].[DimProductCategory]([ProductCategoryLabel]);
CREATE STATISTICS [stat_cso_DimProductCategory_ProductCategoryName] ON [cso].[DimProductCategory]([ProductCategoryName]);
CREATE STATISTICS [stat_cso_DimProductCategory_UpdateDate] ON [cso].[DimProductCategory]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimScenario_ETLLoadID] ON [cso].[DimScenario]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimScenario_LoadDate] ON [cso].[DimScenario]([LoadDate]);
CREATE STATISTICS [stat_cso_DimScenario_ScenarioDescription] ON [cso].[DimScenario]([ScenarioDescription]);
CREATE STATISTICS [stat_cso_DimScenario_ScenarioKey] ON [cso].[DimScenario]([ScenarioKey]);
CREATE STATISTICS [stat_cso_DimScenario_ScenarioLabel] ON [cso].[DimScenario]([ScenarioLabel]);
CREATE STATISTICS [stat_cso_DimScenario_ScenarioName] ON [cso].[DimScenario]([ScenarioName]);
CREATE STATISTICS [stat_cso_DimScenario_UpdateDate] ON [cso].[DimScenario]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimPromotion_DiscountPercent] ON [cso].[DimPromotion]([DiscountPercent]);
CREATE STATISTICS [stat_cso_DimPromotion_EndDate] ON [cso].[DimPromotion]([EndDate]);
CREATE STATISTICS [stat_cso_DimPromotion_ETLLoadID] ON [cso].[DimPromotion]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimPromotion_LoadDate] ON [cso].[DimPromotion]([LoadDate]);
CREATE STATISTICS [stat_cso_DimPromotion_MaxQuantity] ON [cso].[DimPromotion]([MaxQuantity]);
CREATE STATISTICS [stat_cso_DimPromotion_MinQuantity] ON [cso].[DimPromotion]([MinQuantity]);
CREATE STATISTICS [stat_cso_DimPromotion_PromotionCategory] ON [cso].[DimPromotion]([PromotionCategory]);
CREATE STATISTICS [stat_cso_DimPromotion_PromotionDescription] ON [cso].[DimPromotion]([PromotionDescription]);
CREATE STATISTICS [stat_cso_DimPromotion_PromotionKey] ON [cso].[DimPromotion]([PromotionKey]);
CREATE STATISTICS [stat_cso_DimPromotion_PromotionLabel] ON [cso].[DimPromotion]([PromotionLabel]);
CREATE STATISTICS [stat_cso_DimPromotion_PromotionName] ON [cso].[DimPromotion]([PromotionName]);
CREATE STATISTICS [stat_cso_DimPromotion_PromotionType] ON [cso].[DimPromotion]([PromotionType]);
CREATE STATISTICS [stat_cso_DimPromotion_StartDate] ON [cso].[DimPromotion]([StartDate]);
CREATE STATISTICS [stat_cso_DimPromotion_UpdateDate] ON [cso].[DimPromotion]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_EndDate] ON [cso].[DimSalesTerritory]([EndDate]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_ETLLoadID] ON [cso].[DimSalesTerritory]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_GeographyKey] ON [cso].[DimSalesTerritory]([GeographyKey]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_LoadDate] ON [cso].[DimSalesTerritory]([LoadDate]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_SalesTerritoryCountry] ON [cso].[DimSalesTerritory]([SalesTerritoryCountry]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_SalesTerritoryGroup] ON [cso].[DimSalesTerritory]([SalesTerritoryGroup]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_SalesTerritoryKey] ON [cso].[DimSalesTerritory]([SalesTerritoryKey]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_SalesTerritoryLabel] ON [cso].[DimSalesTerritory]([SalesTerritoryLabel]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_SalesTerritoryLevel] ON [cso].[DimSalesTerritory]([SalesTerritoryLevel]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_SalesTerritoryManager] ON [cso].[DimSalesTerritory]([SalesTerritoryManager]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_SalesTerritoryName] ON [cso].[DimSalesTerritory]([SalesTerritoryName]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_SalesTerritoryRegion] ON [cso].[DimSalesTerritory]([SalesTerritoryRegion]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_StartDate] ON [cso].[DimSalesTerritory]([StartDate]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_Status] ON [cso].[DimSalesTerritory]([Status]);
CREATE STATISTICS [stat_cso_DimSalesTerritory_UpdateDate] ON [cso].[DimSalesTerritory]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimProductSubcategory_ETLLoadID] ON [cso].[DimProductSubcategory]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimProductSubcategory_LoadDate] ON [cso].[DimProductSubcategory]([LoadDate]);
CREATE STATISTICS [stat_cso_DimProductSubcategory_ProductCategoryKey] ON [cso].[DimProductSubcategory]([ProductCategoryKey]);
CREATE STATISTICS [stat_cso_DimProductSubcategory_ProductSubcategoryDescription] ON [cso].[DimProductSubcategory]([ProductSubcategoryDescription]);
CREATE STATISTICS [stat_cso_DimProductSubcategory_ProductSubcategoryKey] ON [cso].[DimProductSubcategory]([ProductSubcategoryKey]);
CREATE STATISTICS [stat_cso_DimProductSubcategory_ProductSubcategoryLabel] ON [cso].[DimProductSubcategory]([ProductSubcategoryLabel]);
CREATE STATISTICS [stat_cso_DimProductSubcategory_ProductSubcategoryName] ON [cso].[DimProductSubcategory]([ProductSubcategoryName]);
CREATE STATISTICS [stat_cso_DimProductSubcategory_UpdateDate] ON [cso].[DimProductSubcategory]([UpdateDate]);
CREATE STATISTICS [stat_cso_FactITMachine_Datekey] ON [cso].[FactITMachine]([Datekey]);
CREATE STATISTICS [stat_cso_FactITMachine_ITMachinekey] ON [cso].[FactITMachine]([ITMachinekey]);
CREATE STATISTICS [stat_cso_FactITMachine_MachineKey] ON [cso].[FactITMachine]([MachineKey]);
CREATE STATISTICS [stat_cso_FactInventory_CurrencyKey] ON [cso].[FactInventory]([CurrencyKey]);
CREATE STATISTICS [stat_cso_FactInventory_DateKey] ON [cso].[FactInventory]([DateKey]);
CREATE STATISTICS [stat_cso_FactInventory_InventoryKey] ON [cso].[FactInventory]([InventoryKey]);
CREATE STATISTICS [stat_cso_FactInventory_ProductKey] ON [cso].[FactInventory]([ProductKey]);
CREATE STATISTICS [stat_cso_FactInventory_StoreKey] ON [cso].[FactInventory]([StoreKey]);
CREATE STATISTICS [stat_cso_FactStrategyPlan_AccountKey] ON [cso].[FactStrategyPlan]([AccountKey]);
CREATE STATISTICS [stat_cso_FactStrategyPlan_CurrencyKey] ON [cso].[FactStrategyPlan]([CurrencyKey]);
CREATE STATISTICS [stat_cso_FactStrategyPlan_Datekey] ON [cso].[FactStrategyPlan]([Datekey]);
CREATE STATISTICS [stat_cso_FactStrategyPlan_EntityKey] ON [cso].[FactStrategyPlan]([EntityKey]);
CREATE STATISTICS [stat_cso_FactStrategyPlan_ProductCategoryKey] ON [cso].[FactStrategyPlan]([ProductCategoryKey]);
CREATE STATISTICS [stat_cso_FactStrategyPlan_ScenarioKey] ON [cso].[FactStrategyPlan]([ScenarioKey]);
CREATE STATISTICS [stat_cso_FactStrategyPlan_StrategyPlanKey] ON [cso].[FactStrategyPlan]([StrategyPlanKey]);
CREATE STATISTICS [stat_cso_FactSalesQuota_ChannelKey] ON [cso].[FactSalesQuota]([ChannelKey]);
CREATE STATISTICS [stat_cso_FactSalesQuota_CurrencyKey] ON [cso].[FactSalesQuota]([CurrencyKey]);
CREATE STATISTICS [stat_cso_FactSalesQuota_DateKey] ON [cso].[FactSalesQuota]([DateKey]);
CREATE STATISTICS [stat_cso_FactSalesQuota_ProductKey] ON [cso].[FactSalesQuota]([ProductKey]);
CREATE STATISTICS [stat_cso_FactSalesQuota_SalesQuotaKey] ON [cso].[FactSalesQuota]([SalesQuotaKey]);
CREATE STATISTICS [stat_cso_FactSalesQuota_ScenarioKey] ON [cso].[FactSalesQuota]([ScenarioKey]);
CREATE STATISTICS [stat_cso_FactSalesQuota_StoreKey] ON [cso].[FactSalesQuota]([StoreKey]);
CREATE STATISTICS [stat_cso_FactSales_channelKey] ON [cso].[FactSales]([channelKey]);
CREATE STATISTICS [stat_cso_FactSales_CurrencyKey] ON [cso].[FactSales]([CurrencyKey]);
CREATE STATISTICS [stat_cso_FactSales_DateKey] ON [cso].[FactSales]([DateKey]);
CREATE STATISTICS [stat_cso_FactSales_ProductKey] ON [cso].[FactSales]([ProductKey]);
CREATE STATISTICS [stat_cso_FactSales_PromotionKey] ON [cso].[FactSales]([PromotionKey]);
CREATE STATISTICS [stat_cso_FactSales_SalesKey] ON [cso].[FactSales]([SalesKey]);
CREATE STATISTICS [stat_cso_FactSales_StoreKey] ON [cso].[FactSales]([StoreKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_CurrencyKey] ON [cso].[FactOnlineSales]([CurrencyKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_CustomerKey] ON [cso].[FactOnlineSales]([CustomerKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_DateKey] ON [cso].[FactOnlineSales]([DateKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_OnlineSalesKey] ON [cso].[FactOnlineSales]([OnlineSalesKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_ProductKey] ON [cso].[FactOnlineSales]([ProductKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_PromotionKey] ON [cso].[FactOnlineSales]([PromotionKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_StoreKey] ON [cso].[FactOnlineSales]([StoreKey]);
CREATE STATISTICS [stat_cso_DimCustomer_AddressLine1] ON [cso].[DimCustomer]([AddressLine1]);
CREATE STATISTICS [stat_cso_DimCustomer_AddressLine2] ON [cso].[DimCustomer]([AddressLine2]);
CREATE STATISTICS [stat_cso_DimCustomer_BirthDate] ON [cso].[DimCustomer]([BirthDate]);
CREATE STATISTICS [stat_cso_DimCustomer_CompanyName] ON [cso].[DimCustomer]([CompanyName]);
CREATE STATISTICS [stat_cso_DimCustomer_CustomerKey] ON [cso].[DimCustomer]([CustomerKey]);
CREATE STATISTICS [stat_cso_DimCustomer_CustomerLabel] ON [cso].[DimCustomer]([CustomerLabel]);
CREATE STATISTICS [stat_cso_DimCustomer_CustomerType] ON [cso].[DimCustomer]([CustomerType]);
CREATE STATISTICS [stat_cso_DimCustomer_DateFirstPurchase] ON [cso].[DimCustomer]([DateFirstPurchase]);
CREATE STATISTICS [stat_cso_DimCustomer_Education] ON [cso].[DimCustomer]([Education]);
CREATE STATISTICS [stat_cso_DimCustomer_EmailAddress] ON [cso].[DimCustomer]([EmailAddress]);
CREATE STATISTICS [stat_cso_DimCustomer_ETLLoadID] ON [cso].[DimCustomer]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimCustomer_FirstName] ON [cso].[DimCustomer]([FirstName]);
CREATE STATISTICS [stat_cso_DimCustomer_Gender] ON [cso].[DimCustomer]([Gender]);
CREATE STATISTICS [stat_cso_DimCustomer_GeographyKey] ON [cso].[DimCustomer]([GeographyKey]);
CREATE STATISTICS [stat_cso_DimCustomer_HouseOwnerFlag] ON [cso].[DimCustomer]([HouseOwnerFlag]);
CREATE STATISTICS [stat_cso_DimCustomer_LastName] ON [cso].[DimCustomer]([LastName]);
CREATE STATISTICS [stat_cso_DimCustomer_LoadDate] ON [cso].[DimCustomer]([LoadDate]);
CREATE STATISTICS [stat_cso_DimCustomer_MaritalStatus] ON [cso].[DimCustomer]([MaritalStatus]);
CREATE STATISTICS [stat_cso_DimCustomer_MiddleName] ON [cso].[DimCustomer]([MiddleName]);
CREATE STATISTICS [stat_cso_DimCustomer_NameStyle] ON [cso].[DimCustomer]([NameStyle]);
CREATE STATISTICS [stat_cso_DimCustomer_NumberCarsOwned] ON [cso].[DimCustomer]([NumberCarsOwned]);
CREATE STATISTICS [stat_cso_DimCustomer_NumberChildrenAtHome] ON [cso].[DimCustomer]([NumberChildrenAtHome]);
CREATE STATISTICS [stat_cso_DimCustomer_Occupation] ON [cso].[DimCustomer]([Occupation]);
CREATE STATISTICS [stat_cso_DimCustomer_Phone] ON [cso].[DimCustomer]([Phone]);
CREATE STATISTICS [stat_cso_DimCustomer_Suffix] ON [cso].[DimCustomer]([Suffix]);
CREATE STATISTICS [stat_cso_DimCustomer_Title] ON [cso].[DimCustomer]([Title]);
CREATE STATISTICS [stat_cso_DimCustomer_TotalChildren] ON [cso].[DimCustomer]([TotalChildren]);
CREATE STATISTICS [stat_cso_DimCustomer_UpdateDate] ON [cso].[DimCustomer]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimCustomer_YearlyIncome] ON [cso].[DimCustomer]([YearlyIncome]);
CREATE STATISTICS [stat_cso_DimEmployee_BaseRate] ON [cso].[DimEmployee]([BaseRate]);
CREATE STATISTICS [stat_cso_DimEmployee_BirthDate] ON [cso].[DimEmployee]([BirthDate]);
CREATE STATISTICS [stat_cso_DimEmployee_CurrentFlag] ON [cso].[DimEmployee]([CurrentFlag]);
CREATE STATISTICS [stat_cso_DimEmployee_DepartmentName] ON [cso].[DimEmployee]([DepartmentName]);
CREATE STATISTICS [stat_cso_DimEmployee_EmailAddress] ON [cso].[DimEmployee]([EmailAddress]);
CREATE STATISTICS [stat_cso_DimEmployee_EmergencyContactName] ON [cso].[DimEmployee]([EmergencyContactName]);
CREATE STATISTICS [stat_cso_DimEmployee_EmergencyContactPhone] ON [cso].[DimEmployee]([EmergencyContactPhone]);
CREATE STATISTICS [stat_cso_DimEmployee_EmployeeKey] ON [cso].[DimEmployee]([EmployeeKey]);
CREATE STATISTICS [stat_cso_DimEmployee_EndDate] ON [cso].[DimEmployee]([EndDate]);
CREATE STATISTICS [stat_cso_DimEmployee_ETLLoadID] ON [cso].[DimEmployee]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimEmployee_FirstName] ON [cso].[DimEmployee]([FirstName]);
CREATE STATISTICS [stat_cso_DimEmployee_Gender] ON [cso].[DimEmployee]([Gender]);
CREATE STATISTICS [stat_cso_DimEmployee_HireDate] ON [cso].[DimEmployee]([HireDate]);
CREATE STATISTICS [stat_cso_DimEmployee_LastName] ON [cso].[DimEmployee]([LastName]);
CREATE STATISTICS [stat_cso_DimEmployee_LoadDate] ON [cso].[DimEmployee]([LoadDate]);
CREATE STATISTICS [stat_cso_DimEmployee_MaritalStatus] ON [cso].[DimEmployee]([MaritalStatus]);
CREATE STATISTICS [stat_cso_DimEmployee_MiddleName] ON [cso].[DimEmployee]([MiddleName]);
CREATE STATISTICS [stat_cso_DimEmployee_ParentEmployeeKey] ON [cso].[DimEmployee]([ParentEmployeeKey]);
CREATE STATISTICS [stat_cso_DimEmployee_PayFrequency] ON [cso].[DimEmployee]([PayFrequency]);
CREATE STATISTICS [stat_cso_DimEmployee_Phone] ON [cso].[DimEmployee]([Phone]);
CREATE STATISTICS [stat_cso_DimEmployee_SalariedFlag] ON [cso].[DimEmployee]([SalariedFlag]);
CREATE STATISTICS [stat_cso_DimEmployee_SalesPersonFlag] ON [cso].[DimEmployee]([SalesPersonFlag]);
CREATE STATISTICS [stat_cso_DimEmployee_StartDate] ON [cso].[DimEmployee]([StartDate]);
CREATE STATISTICS [stat_cso_DimEmployee_Status] ON [cso].[DimEmployee]([Status]);
CREATE STATISTICS [stat_cso_DimEmployee_Title] ON [cso].[DimEmployee]([Title]);
CREATE STATISTICS [stat_cso_DimEmployee_UpdateDate] ON [cso].[DimEmployee]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimEmployee_VacationHours] ON [cso].[DimEmployee]([VacationHours]);
CREATE STATISTICS [stat_cso_DimEntity_EndDate] ON [cso].[DimEntity]([EndDate]);
CREATE STATISTICS [stat_cso_DimEntity_EntityDescription] ON [cso].[DimEntity]([EntityDescription]);
CREATE STATISTICS [stat_cso_DimEntity_EntityKey] ON [cso].[DimEntity]([EntityKey]);
CREATE STATISTICS [stat_cso_DimEntity_EntityLabel] ON [cso].[DimEntity]([EntityLabel]);
CREATE STATISTICS [stat_cso_DimEntity_EntityName] ON [cso].[DimEntity]([EntityName]);
CREATE STATISTICS [stat_cso_DimEntity_EntityType] ON [cso].[DimEntity]([EntityType]);
CREATE STATISTICS [stat_cso_DimEntity_ETLLoadID] ON [cso].[DimEntity]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimEntity_LoadDate] ON [cso].[DimEntity]([LoadDate]);
CREATE STATISTICS [stat_cso_DimEntity_ParentEntityKey] ON [cso].[DimEntity]([ParentEntityKey]);
CREATE STATISTICS [stat_cso_DimEntity_ParentEntityLabel] ON [cso].[DimEntity]([ParentEntityLabel]);
CREATE STATISTICS [stat_cso_DimEntity_StartDate] ON [cso].[DimEntity]([StartDate]);
CREATE STATISTICS [stat_cso_DimEntity_Status] ON [cso].[DimEntity]([Status]);
CREATE STATISTICS [stat_cso_DimEntity_UpdateDate] ON [cso].[DimEntity]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimProduct_AvailableForSaleDate] ON [cso].[DimProduct]([AvailableForSaleDate]);
CREATE STATISTICS [stat_cso_DimProduct_BrandName] ON [cso].[DimProduct]([BrandName]);
CREATE STATISTICS [stat_cso_DimProduct_ClassID] ON [cso].[DimProduct]([ClassID]);
CREATE STATISTICS [stat_cso_DimProduct_ClassName] ON [cso].[DimProduct]([ClassName]);
CREATE STATISTICS [stat_cso_DimProduct_ColorID] ON [cso].[DimProduct]([ColorID]);
CREATE STATISTICS [stat_cso_DimProduct_ColorName] ON [cso].[DimProduct]([ColorName]);
CREATE STATISTICS [stat_cso_DimProduct_ETLLoadID] ON [cso].[DimProduct]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimProduct_ImageURL] ON [cso].[DimProduct]([ImageURL]);
CREATE STATISTICS [stat_cso_DimProduct_LoadDate] ON [cso].[DimProduct]([LoadDate]);
CREATE STATISTICS [stat_cso_DimProduct_Manufacturer] ON [cso].[DimProduct]([Manufacturer]);
CREATE STATISTICS [stat_cso_DimProduct_ProductDescription] ON [cso].[DimProduct]([ProductDescription]);
CREATE STATISTICS [stat_cso_DimProduct_ProductKey] ON [cso].[DimProduct]([ProductKey]);
CREATE STATISTICS [stat_cso_DimProduct_ProductLabel] ON [cso].[DimProduct]([ProductLabel]);
CREATE STATISTICS [stat_cso_DimProduct_ProductName] ON [cso].[DimProduct]([ProductName]);
CREATE STATISTICS [stat_cso_DimProduct_ProductSubcategoryKey] ON [cso].[DimProduct]([ProductSubcategoryKey]);
CREATE STATISTICS [stat_cso_DimProduct_ProductURL] ON [cso].[DimProduct]([ProductURL]);
CREATE STATISTICS [stat_cso_DimProduct_Size] ON [cso].[DimProduct]([Size]);
CREATE STATISTICS [stat_cso_DimProduct_SizeRange] ON [cso].[DimProduct]([SizeRange]);
CREATE STATISTICS [stat_cso_DimProduct_SizeUnitMeasureID] ON [cso].[DimProduct]([SizeUnitMeasureID]);
CREATE STATISTICS [stat_cso_DimProduct_Status] ON [cso].[DimProduct]([Status]);
CREATE STATISTICS [stat_cso_DimProduct_StockTypeID] ON [cso].[DimProduct]([StockTypeID]);
CREATE STATISTICS [stat_cso_DimProduct_StockTypeName] ON [cso].[DimProduct]([StockTypeName]);
CREATE STATISTICS [stat_cso_DimProduct_StopSaleDate] ON [cso].[DimProduct]([StopSaleDate]);
CREATE STATISTICS [stat_cso_DimProduct_StyleID] ON [cso].[DimProduct]([StyleID]);
CREATE STATISTICS [stat_cso_DimProduct_StyleName] ON [cso].[DimProduct]([StyleName]);
CREATE STATISTICS [stat_cso_DimProduct_UnitCost] ON [cso].[DimProduct]([UnitCost]);
CREATE STATISTICS [stat_cso_DimProduct_UnitOfMeasureID] ON [cso].[DimProduct]([UnitOfMeasureID]);
CREATE STATISTICS [stat_cso_DimProduct_UnitOfMeasureName] ON [cso].[DimProduct]([UnitOfMeasureName]);
CREATE STATISTICS [stat_cso_DimProduct_UnitPrice] ON [cso].[DimProduct]([UnitPrice]);
CREATE STATISTICS [stat_cso_DimProduct_UpdateDate] ON [cso].[DimProduct]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimProduct_Weight] ON [cso].[DimProduct]([Weight]);
CREATE STATISTICS [stat_cso_DimProduct_WeightUnitMeasureID] ON [cso].[DimProduct]([WeightUnitMeasureID]);
CREATE STATISTICS [stat_cso_DimAccount_AccountDescription] ON [cso].[DimAccount]([AccountDescription]);
CREATE STATISTICS [stat_cso_DimAccount_AccountKey] ON [cso].[DimAccount]([AccountKey]);
CREATE STATISTICS [stat_cso_DimAccount_AccountLabel] ON [cso].[DimAccount]([AccountLabel]);
CREATE STATISTICS [stat_cso_DimAccount_AccountName] ON [cso].[DimAccount]([AccountName]);
CREATE STATISTICS [stat_cso_DimAccount_AccountType] ON [cso].[DimAccount]([AccountType]);
CREATE STATISTICS [stat_cso_DimAccount_CustomMemberOptions] ON [cso].[DimAccount]([CustomMemberOptions]);
CREATE STATISTICS [stat_cso_DimAccount_CustomMembers] ON [cso].[DimAccount]([CustomMembers]);
CREATE STATISTICS [stat_cso_DimAccount_ETLLoadID] ON [cso].[DimAccount]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimAccount_LoadDate] ON [cso].[DimAccount]([LoadDate]);
CREATE STATISTICS [stat_cso_DimAccount_Operator] ON [cso].[DimAccount]([Operator]);
CREATE STATISTICS [stat_cso_DimAccount_ParentAccountKey] ON [cso].[DimAccount]([ParentAccountKey]);
CREATE STATISTICS [stat_cso_DimAccount_UpdateDate] ON [cso].[DimAccount]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimAccount_ValueType] ON [cso].[DimAccount]([ValueType]);
CREATE STATISTICS [stat_cso_DimChannel_ChannelDescription] ON [cso].[DimChannel]([ChannelDescription]);
CREATE STATISTICS [stat_cso_DimChannel_ChannelKey] ON [cso].[DimChannel]([ChannelKey]);
CREATE STATISTICS [stat_cso_DimChannel_ChannelLabel] ON [cso].[DimChannel]([ChannelLabel]);
CREATE STATISTICS [stat_cso_DimChannel_ChannelName] ON [cso].[DimChannel]([ChannelName]);
CREATE STATISTICS [stat_cso_DimChannel_ETLLoadID] ON [cso].[DimChannel]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimChannel_LoadDate] ON [cso].[DimChannel]([LoadDate]);
CREATE STATISTICS [stat_cso_DimChannel_UpdateDate] ON [cso].[DimChannel]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimCurrency_CurrencyDescription] ON [cso].[DimCurrency]([CurrencyDescription]);
CREATE STATISTICS [stat_cso_DimCurrency_CurrencyKey] ON [cso].[DimCurrency]([CurrencyKey]);
CREATE STATISTICS [stat_cso_DimCurrency_CurrencyLabel] ON [cso].[DimCurrency]([CurrencyLabel]);
CREATE STATISTICS [stat_cso_DimCurrency_CurrencyName] ON [cso].[DimCurrency]([CurrencyName]);
CREATE STATISTICS [stat_cso_DimCurrency_ETLLoadID] ON [cso].[DimCurrency]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimCurrency_LoadDate] ON [cso].[DimCurrency]([LoadDate]);
CREATE STATISTICS [stat_cso_DimCurrency_UpdateDate] ON [cso].[DimCurrency]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimDate_AsiaSeason] ON [cso].[DimDate]([AsiaSeason]);
CREATE STATISTICS [stat_cso_DimDate_CalendarDayOfWeek] ON [cso].[DimDate]([CalendarDayOfWeek]);
CREATE STATISTICS [stat_cso_DimDate_CalendarDayOfWeekLabel] ON [cso].[DimDate]([CalendarDayOfWeekLabel]);
CREATE STATISTICS [stat_cso_DimDate_CalendarHalfYear] ON [cso].[DimDate]([CalendarHalfYear]);
CREATE STATISTICS [stat_cso_DimDate_CalendarHalfYearLabel] ON [cso].[DimDate]([CalendarHalfYearLabel]);
CREATE STATISTICS [stat_cso_DimDate_CalendarMonth] ON [cso].[DimDate]([CalendarMonth]);
CREATE STATISTICS [stat_cso_DimDate_CalendarMonthLabel] ON [cso].[DimDate]([CalendarMonthLabel]);
CREATE STATISTICS [stat_cso_DimDate_CalendarQuarter] ON [cso].[DimDate]([CalendarQuarter]);
CREATE STATISTICS [stat_cso_DimDate_CalendarQuarterLabel] ON [cso].[DimDate]([CalendarQuarterLabel]);
CREATE STATISTICS [stat_cso_DimDate_CalendarWeek] ON [cso].[DimDate]([CalendarWeek]);
CREATE STATISTICS [stat_cso_DimDate_CalendarWeekLabel] ON [cso].[DimDate]([CalendarWeekLabel]);
CREATE STATISTICS [stat_cso_DimDate_CalendarYear] ON [cso].[DimDate]([CalendarYear]);
CREATE STATISTICS [stat_cso_DimDate_CalendarYearLabel] ON [cso].[DimDate]([CalendarYearLabel]);
CREATE STATISTICS [stat_cso_DimDate_DateDescription] ON [cso].[DimDate]([DateDescription]);
CREATE STATISTICS [stat_cso_DimDate_Datekey] ON [cso].[DimDate]([Datekey]);
CREATE STATISTICS [stat_cso_DimDate_EuropeSeason] ON [cso].[DimDate]([EuropeSeason]);
CREATE STATISTICS [stat_cso_DimDate_FiscalHalfYear] ON [cso].[DimDate]([FiscalHalfYear]);
CREATE STATISTICS [stat_cso_DimDate_FiscalHalfYearLabel] ON [cso].[DimDate]([FiscalHalfYearLabel]);
CREATE STATISTICS [stat_cso_DimDate_FiscalMonth] ON [cso].[DimDate]([FiscalMonth]);
CREATE STATISTICS [stat_cso_DimDate_FiscalMonthLabel] ON [cso].[DimDate]([FiscalMonthLabel]);
CREATE STATISTICS [stat_cso_DimDate_FiscalQuarter] ON [cso].[DimDate]([FiscalQuarter]);
CREATE STATISTICS [stat_cso_DimDate_FiscalQuarterLabel] ON [cso].[DimDate]([FiscalQuarterLabel]);
CREATE STATISTICS [stat_cso_DimDate_FiscalYear] ON [cso].[DimDate]([FiscalYear]);
CREATE STATISTICS [stat_cso_DimDate_FiscalYearLabel] ON [cso].[DimDate]([FiscalYearLabel]);
CREATE STATISTICS [stat_cso_DimDate_FullDateLabel] ON [cso].[DimDate]([FullDateLabel]);
CREATE STATISTICS [stat_cso_DimDate_HolidayName] ON [cso].[DimDate]([HolidayName]);
CREATE STATISTICS [stat_cso_DimDate_IsHoliday] ON [cso].[DimDate]([IsHoliday]);
CREATE STATISTICS [stat_cso_DimDate_IsWorkDay] ON [cso].[DimDate]([IsWorkDay]);
CREATE STATISTICS [stat_cso_DimDate_NorthAmericaSeason] ON [cso].[DimDate]([NorthAmericaSeason]);
CREATE STATISTICS [stat_cso_DimStore_AddressLine1] ON [cso].[DimStore]([AddressLine1]);
CREATE STATISTICS [stat_cso_DimStore_AddressLine2] ON [cso].[DimStore]([AddressLine2]);
CREATE STATISTICS [stat_cso_DimStore_CloseDate] ON [cso].[DimStore]([CloseDate]);
CREATE STATISTICS [stat_cso_DimStore_CloseReason] ON [cso].[DimStore]([CloseReason]);
CREATE STATISTICS [stat_cso_DimStore_EmployeeCount] ON [cso].[DimStore]([EmployeeCount]);
CREATE STATISTICS [stat_cso_DimStore_EntityKey] ON [cso].[DimStore]([EntityKey]);
CREATE STATISTICS [stat_cso_DimStore_ETLLoadID] ON [cso].[DimStore]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimStore_GeographyKey] ON [cso].[DimStore]([GeographyKey]);
CREATE STATISTICS [stat_cso_DimStore_GeoLocation] ON [cso].[DimStore]([GeoLocation]);
CREATE STATISTICS [stat_cso_DimStore_Geometry] ON [cso].[DimStore]([Geometry]);
CREATE STATISTICS [stat_cso_DimStore_LastRemodelDate] ON [cso].[DimStore]([LastRemodelDate]);
CREATE STATISTICS [stat_cso_DimStore_LoadDate] ON [cso].[DimStore]([LoadDate]);
CREATE STATISTICS [stat_cso_DimStore_OpenDate] ON [cso].[DimStore]([OpenDate]);
CREATE STATISTICS [stat_cso_DimStore_SellingAreaSize] ON [cso].[DimStore]([SellingAreaSize]);
CREATE STATISTICS [stat_cso_DimStore_Status] ON [cso].[DimStore]([Status]);
CREATE STATISTICS [stat_cso_DimStore_StoreDescription] ON [cso].[DimStore]([StoreDescription]);
CREATE STATISTICS [stat_cso_DimStore_StoreFax] ON [cso].[DimStore]([StoreFax]);
CREATE STATISTICS [stat_cso_DimStore_StoreKey] ON [cso].[DimStore]([StoreKey]);
CREATE STATISTICS [stat_cso_DimStore_StoreManager] ON [cso].[DimStore]([StoreManager]);
CREATE STATISTICS [stat_cso_DimStore_StoreName] ON [cso].[DimStore]([StoreName]);
CREATE STATISTICS [stat_cso_DimStore_StorePhone] ON [cso].[DimStore]([StorePhone]);
CREATE STATISTICS [stat_cso_DimStore_StoreType] ON [cso].[DimStore]([StoreType]);
CREATE STATISTICS [stat_cso_DimStore_UpdateDate] ON [cso].[DimStore]([UpdateDate]);
CREATE STATISTICS [stat_cso_DimStore_ZipCode] ON [cso].[DimStore]([ZipCode]);
CREATE STATISTICS [stat_cso_DimStore_ZipCodeExtension] ON [cso].[DimStore]([ZipCodeExtension]);
CREATE STATISTICS [stat_cso_DimGeography_CityName] ON [cso].[DimGeography]([CityName]);
CREATE STATISTICS [stat_cso_DimGeography_ContinentName] ON [cso].[DimGeography]([ContinentName]);
CREATE STATISTICS [stat_cso_DimGeography_ETLLoadID] ON [cso].[DimGeography]([ETLLoadID]);
CREATE STATISTICS [stat_cso_DimGeography_GeographyKey] ON [cso].[DimGeography]([GeographyKey]);
CREATE STATISTICS [stat_cso_DimGeography_GeographyType] ON [cso].[DimGeography]([GeographyType]);
CREATE STATISTICS [stat_cso_DimGeography_LoadDate] ON [cso].[DimGeography]([LoadDate]);
CREATE STATISTICS [stat_cso_DimGeography_RegionCountryName] ON [cso].[DimGeography]([RegionCountryName]);
CREATE STATISTICS [stat_cso_DimGeography_StateProvinceName] ON [cso].[DimGeography]([StateProvinceName]);
CREATE STATISTICS [stat_cso_DimGeography_UpdateDate] ON [cso].[DimGeography]([UpdateDate]);
CREATE STATISTICS [stat_cso_FactOnlineSales_CurrencyKey] ON [cso].[FactOnlineSales]([CurrencyKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_CustomerKey] ON [cso].[FactOnlineSales]([CustomerKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_DateKey] ON [cso].[FactOnlineSales]([DateKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_OnlineSalesKey] ON [cso].[FactOnlineSales]([OnlineSalesKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_ProductKey] ON [cso].[FactOnlineSales]([ProductKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_PromotionKey] ON [cso].[FactOnlineSales]([PromotionKey]);
CREATE STATISTICS [stat_cso_FactOnlineSales_StoreKey] ON [cso].[FactOnlineSales]([StoreKey]);

-- END of the lab

