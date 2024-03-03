with
    PersonCreditCard as (
        select *
        from {{ ref('stg_erp_Person_Credit_Card') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(BusinessPersonId as string)) as Pk_BusinessPerson
            , BusinessPersonId
            , CreditCardId
        from PersonCreditCard
    )

select *
from CreatingPk
