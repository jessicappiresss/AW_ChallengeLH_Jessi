with
    SalesPersonTable as (
        select *
        from {{ ref('stg_erp_Sales_Person') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(BusinessPersonid  as string)) as Pk_BusinessPerson
            , *
        from SalesPersonTable
    )

select *
from CreatingPk
