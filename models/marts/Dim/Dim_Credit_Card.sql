with
    CreditCardTable as (
        select *
        from {{ ref('Credit_Card') }}
    )

    , SelectingColumnsDimCreditCard as (
        select
            Pk_CreditCard
            , Fk_BusinessPerson
            , CreditCardId
            , CardBrandType
        from CreditCardTable
    )

    , CreatingSkCreditCard as (
        select
            farm_fingerprint(cast(Pk_CreditCard as string)) as Sk_CreditCard
            , SelectingColumnsDimCreditCard.*
        from SelectingColumnsDimCreditCard
    )

select *
from CreatingSkCreditCard
