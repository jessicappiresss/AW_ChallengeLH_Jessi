with
    StoreStaging as (
        select
            cast(businessentityid  as int) as BusinessPersonId
            , cast(name as string) as StoreName
            , cast(salespersonid as int) as SalesPersonId
        from {{ source('sales', 'store') }}
    )

select *
from StoreStaging
