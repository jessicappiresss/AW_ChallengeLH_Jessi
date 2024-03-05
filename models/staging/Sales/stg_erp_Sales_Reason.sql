with
    SalesReasonStaging as (
        select
            cast(salesreasonid as int) as SalesReasonId
            , cast(name as string) as ReasonGivenName
            , cast(reasontype  as string) as ReasonType
        from {{ source('sales', 'salesreason') }}
    )

select *
from SalesReasonStaging
