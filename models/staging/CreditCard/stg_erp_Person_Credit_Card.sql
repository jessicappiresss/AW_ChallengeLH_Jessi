with
    PersonCardStaging as (
        select
            cast(businessentityid  as int) as BusinessPersonId
            , cast(creditcardid  as int) as CreditCardId
        from {{ source('sales', 'personcreditcard') }}
    )

select *
from PersonCardStaging
