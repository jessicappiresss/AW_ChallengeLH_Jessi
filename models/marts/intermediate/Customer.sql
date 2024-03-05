with
    CustomerTable as (
        select *
        from {{ ref('stg_erp_Customer') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(CustomerId as string)) as Pk_Customer
            , *
        from CustomerTable
    )

select *
from CreatingPk
