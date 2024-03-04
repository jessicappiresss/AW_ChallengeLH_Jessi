with
    ContactTypeStagingAndPk as (
        select
            cast(contacttypeid as int) as ContactTypeId
            , cast(name as string) as ContactTypeName
        from {{ source('person', 'contacttype') }}
    )

    ,CreatingPk as (
        select
            farm_fingerprint(cast(ContactTypeId as string)) as Pk_ContactType
            , ContactTypeId
            , ContactTypeName
        from ContactTypeStagingAndPk
    )

select *
from CreatingPk
