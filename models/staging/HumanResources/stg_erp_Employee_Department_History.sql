with
    EmployeeDepartmentHistoryStaging as (
        select
            cast(businessentityid  as int) as BusinessPersonId 
            , cast(departmentid as int) as DepartmentEmployeeId
        from {{ source('humanresources', 'employeedepartmenthistory') }}
    )

select *
from EmployeeDepartmentHistoryStaging
