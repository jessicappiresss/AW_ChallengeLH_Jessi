with
    StateProvinceStagingAndPk as (
        select
            cast(stateprovinceid as int) as StateProvinceId
            , cast(stateprovincecode as string) as StateProvinceAbbreviation
            , cast(countryregioncode as string) as CountryRegionCode
            , cast(name as string) as RegionName
            , cast(territoryid as int) as TerritoryId
        from {{ source('person', 'stateprovince') }}
    )

    ,CreatingPk as (
        select
            farm_fingerprint(cast(StateProvinceId as string)) as Pk_StateProvince
            , StateProvinceId
            , StateProvinceAbbreviation
            , CountryRegionCode
            , RegionName
            , TerritoryId
        from StateProvinceStagingAndPk
    )

select *
from CreatingPk
