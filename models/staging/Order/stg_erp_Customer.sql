with
    CustomerStaging as (
        select
            cast(customerid as int) as CustomerId
            , cast(personid  as int) as PersonId
            , cast(storeid  as int) as StoreId
            , cast(territoryid as int) as TerritoryId
        from {{ source('sales', 'customer') }}
    )

select *
from CustomerStaging
