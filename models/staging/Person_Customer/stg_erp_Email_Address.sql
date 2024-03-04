with
    EmailAddressStagingAndPk as (
        select
            businessentityid as BusinessPersonId
            , emailaddressid as EmailAddressId
            , emailaddress as EmailAddress
        from {{ source('person', 'emailaddress') }}
    )

    ,CreatingPk as (
        select
            farm_fingerprint(cast(EmailAddressId as string)) as Pk_EmailAddress
            , EmailAddressId
            , BusinessPersonId
            , EmailAddress
        from EmailAddressStagingAndPk
    )

select *
from CreatingPk
