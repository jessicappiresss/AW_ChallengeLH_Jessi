with
    EmployeeStaging as (
        select
            cast(loginid as string) as EmployeeLoginId
            , cast(businessentityid as int) as BusinessPersonId
            , cast(jobtitle as string) as JobName
            , cast(gender as string) as EmployeeGender
            , cast(hiredate as timestamp) as EmployeeHiredDate
            , cast(salariedflag as string) as SalariedFlag
            , case
                when salariedflag = true then 'Yes'
                when salariedflag = false then 'No'
                else 'Unknown'
            end as SalariedEmployee
            , cast(vacationhours as int) as EmployeeVacationHours
            , cast(sickleavehours as int) as EmployeeSickLeaveHours
            , cast(currentflag as string) CurrentFlag
            , case
                when currentflag = true then 'Yes'
                when currentflag = false then 'No'
                else 'Unknown'
            end as CurrentEmployee
        from {{ source('humanresources', 'employee') }}
    )

select *
from EmployeeStaging
