with
    BusinessEntityContactStagingAndPk as (
        select
            cast(businessentityid as int) as BusinessPersonId
            , cast(personid as int) as PersonContactId
            , cast(contacttypeid as int) as ContactTypeId
        from {{ source('person', 'businessentitycontact') }}
    )

    ,CreatingPk as (
        select
            farm_fingerprint(cast(PersonContactId as string)) as Pk_PersonContact
            , PersonContactId
            , BusinessPersonId
            , ContactTypeId
        from BusinessEntityContactStagingAndPk
    )

select *
from CreatingPk
