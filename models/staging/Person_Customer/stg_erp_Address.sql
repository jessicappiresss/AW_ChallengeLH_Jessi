with
    AddressStagingAndPk as (
        select
            cast(addressid as int) as AddressId
            , cast(addressline1 as string) as AddressRoadName
            , cast(city as string) as AddressCityName
            , cast(stateprovinceid as int) as StateProvinceId
            , cast(postalcode as string) as PostalCode
        from {{ source('person', 'address') }}
    )

    ,CreatingPk as (
        select
            farm_fingerprint(cast(AddressId as string)) as Pk_PrincipalAddress
            , AddressId
            , AddressRoadName
            , AddressCityName
            , StateProvinceId
            , PostalCode
        from AddressStagingAndPk
    )

select *
from CreatingPk
