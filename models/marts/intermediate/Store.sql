with
    StoreTable as (
        select *
        from {{ ref('stg_erp_Store') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(BusinessPersonId  as string)) as Pk_BusinessPerson
            , *
        from StoreTable
    )

select *
from CreatingPk
