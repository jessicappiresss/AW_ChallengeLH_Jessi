with
    SalesTerritoryStaging as (
        select
            cast(territoryid as int) as TerritoryId
            , cast(name as string) as TerritoryName
            , cast(countryregioncode as string) as CountryRegionCode
            , cast(salesytd as float64) as SalesTerritoryAccumulatedYear
            , cast(saleslastyear as float64) as TotalSalesYear
            , cast(costytd as float64) as CommercialCostsTerritoryYear
            , cast(costlastyear as float64) as CommercialCostsTerritoryPreviousYear
        from {{ source('sales', 'salesterritory') }}
    )

select *
from SalesTerritoryStaging
