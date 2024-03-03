with
    EmployeeDepartmentStaging as (
        select
            cast(departmentid as int) as DepartmentEmployeeId
            , cast(name as string) as DepartamentName
            , cast(groupname as string) as DepartmentGroupName
        from {{ source('humanresources', 'department') }}
    )

select *
from EmployeeDepartmentStaging
