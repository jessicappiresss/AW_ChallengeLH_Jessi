with
    SalesTerritoryTable as (
        select *
        from {{ ref('stg_erp_Sales_Territory') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(TerritoryID  as string)) as Pk_Territory
            , *
        from SalesTerritoryTable
    )

select *
from CreatingPk
