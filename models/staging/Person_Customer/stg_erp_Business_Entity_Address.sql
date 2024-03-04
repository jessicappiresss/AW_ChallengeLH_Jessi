with
    BusinessAddressStagingAndPk as (
        select
            cast(businessentityid as int) as BusinessPersonId
            , cast(addressid as int) as AddressId
            , cast(addresstypeid as int) as AddressTypeId
        from {{ source('person', 'businessentityaddress') }}
    )

    ,CreatingPk as (
        select
            farm_fingerprint(cast(AddressId as string)) as Pk_Address
            , AddressId
            , BusinessPersonId
            , AddressTypeId
        from BusinessAddressStagingAndPk
    )

select *
from CreatingPk
