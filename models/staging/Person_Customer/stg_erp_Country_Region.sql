with
    BusinessAddressStagingAndPk as (
        select
            cast(countryregioncode as string) as CountryRegionCode
            , cast(name as string) as CountryName
        from {{ source('person', 'countryregion') }}
    )

    ,CreatingPk as (
        select
            farm_fingerprint(cast(CountryRegionCode as string)) as Pk_CountryRegion
            , CountryRegionCode
            , CountryName
        from BusinessAddressStagingAndPk
    )

select *
from CreatingPk
