with
    SalesReason as (
        select *
        from {{ ref('stg_erp_Sales_Reason') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(SalesReasonId as string)) as Pk_SalesReason
            , SalesReasonId
            , ReasonGivenName
            , ReasonType
        from SalesReason
    )

select *
from CreatingPk
