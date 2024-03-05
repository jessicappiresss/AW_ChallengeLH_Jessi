with
    SalesPersonStaging as (
        select
            cast(businessentityid as int) as BusinessPersonid
            , cast(territoryid as int) as TerritoryId
            , cast(salesquota as int) as ProjectionAnnualSales
            , cast(bonus as int) as BonusWhenGoalAchieved
            , cast(salesytd as float64) as TotalSalesYear
            , cast(saleslastyear as float64) as TotalSalesPreviousYear
        from {{ source('sales', 'salesperson') }}
    )

select *
from SalesPersonStaging
