with
    PersonStagingAndPk as (
        select
            cast(businessentityid as int) as BusinessPersonId
            , cast(persontype as string) as PersonType
            , case
                when persontype = 'SC' then 'Store contact'
                when persontype = 'IN' then 'Individual customer (retail)'
                when persontype = 'SP' then 'Seller'
                when persontype = 'EM' then 'Employee (non-sales related)'
                when persontype = 'VC' then 'Supplier contact'
                when persontype = 'GC' then 'General contact'
                else 'unknown'
            end as PersonTypeDescription
            , cast(namestyle as string) as namestyle
            , case 
                when namestyle = true then 'Western order (name, surname)'
                when namestyle = false then 'Eastern order (surname, first name)'
                else 'unknown'
            end as ModelTypeName
            , cast(title as string) as TitleName
            , cast(firstname as string) as FirstName
            , cast(middlename as string) as MiddleName
            , cast(lastname as string) as LastName
            , concat(
                coalesce(firstname, ''), 
                case when firstname is not null and (middlename is not null or lastname is not null) then ' ' else '' end,
                coalesce(middlename, ''), 
                case when middlename is not null and lastname is not null then ' ' else '' end,
                coalesce(lastname, '')
        ) as CompleteName
        from {{ source('person', 'person') }}
    )

    ,CreatingPk as (
        select
            farm_fingerprint(cast(BusinessPersonId as string)) as Pk_BusinessPerson
            , BusinessPersonId
            , PersonType
            , PersonTypeDescription
            , ModelTypeName
            , TitleName
            , CompleteName
        from PersonStagingAndPk
    )

select *
from CreatingPk
