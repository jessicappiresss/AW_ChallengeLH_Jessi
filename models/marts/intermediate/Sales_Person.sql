with
    SalesPersonTable as (
        select *
        from {{ ref('stg_erp_Sales_Person') }}
    )

    , PersonStg as (
        select *
        from {{ ref('stg_erp_Person') }}
    )

    , SalesPersonTable1 as (
        select
            PersonStg.CompleteName as SellerCompleteName
            , SalesPersonTable.*
        from SalesPersonTable
        left join PersonStg
            on SalesPersonTable.BusinessPersonId = PersonStg.BusinessPersonId
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(BusinessPersonid  as string)) as Pk_BusinessPerson
            , *
        from SalesPersonTable1
    )

select *
from CreatingPk
