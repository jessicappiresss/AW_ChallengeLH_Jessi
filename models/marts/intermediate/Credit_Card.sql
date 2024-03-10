with
    CreditCard as (
        select *
        from {{ ref('stg_erp_Credit_Card') }}
    )

    , PersonCreditCard as (
        select *
        from {{ ref('Person_Credit_Card') }}
    )

    , CreditCardPrincipalTable as (
        select
            PersonCreditCard.BusinessPersonId
            , PersonCreditCard.Pk_BusinessPerson
            , CreditCard.*
        from CreditCard
        left join PersonCreditCard
            on CreditCard.CreditCardId = PersonCreditCard.CreditCardId
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(CreditCardId as string)) as Pk_CreditCard
            , CreditCardId
            , Pk_BusinessPerson as Fk_BusinessPerson
            , CardBrandType
        from CreditCardPrincipalTable
    )

select *
from CreatingPk
