with
    SalesTerrytoryStaging as (
        select
            cast(territoryid as int) as TerritoryId
            , cast(name as string) as TerritoryName
            , cast(countryregioncode as string) as CountryRegionCode
            , cast(salesytd as float) as SalesTerritoryAccumulatedYear
            , cast(saleslastyear as float) as TotalSalesYear
            , cast(costytd as float) as CommercialCostsTerritoryYear
            , cast(costlastyear as float) as CommercialCostsTerritoryPreviousYear
        from {{ source('sales', 'salesterrytory') }}
    )

select *
from SalesTerrytoryStaging
