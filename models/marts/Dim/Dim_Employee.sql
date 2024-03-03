with
    EmployeePrincipalTable as (
        select *
        from {{ ref('Employee') }}
    )

    , SelectingColumnsDimEmployee as (
        select
            Pk_EmployeeLogin
            , Fk_BusinessPerson
            , Fk_DepartmentEmployee
            , BusinessPersonId
            , DepartmentEmployeeId
            , JobName
            , DepartamentName
            , DepartmentGroupName
            , EmployeeGender
            , EmployeeHiredDate
            , SalariedEmployee
            , EmployeeVacationHours
            , EmployeeSickLeaveHours
            , CurrentEmployee
        from EmployeePrincipalTable
    )

    , CreatingSkEmployee as (
        select
            farm_fingerprint(cast(Pk_EmployeeLogin as string)) as Sk_Employee
            , SelectingColumnsDimEmployee.*
        from SelectingColumnsDimEmployee
    )

select *
from CreatingSkEmployee
