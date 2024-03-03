with
    CreditCardStaging as (
        select
            cast(creditcardid  as int) as CreditCardId
            , cast(cardtype  as string) as CardBrandType
        from {{ source('sales', 'creditcard') }}
    )

select *
from CreditCardStaging