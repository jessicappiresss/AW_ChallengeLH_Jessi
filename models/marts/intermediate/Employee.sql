with
    Employee as (
        select *
        from {{ ref('stg_erp_Employee') }}
    )

    , EmployeeDepartment as (
        select *
        from {{ ref('Employee_Department') }}
    )

    , Department as (
        select *
        from {{ ref('Department') }}
    )

    , FirstJoinEmployeeTable as (
        select
            EmployeeDepartment.Pk_BusinessPerson
            , EmployeeDepartment.DepartmentEmployeeId
            , Employee.*
        from Employee
        left join EmployeeDepartment
            on Employee.BusinessPersonId = EmployeeDepartment.BusinessPersonId
    )

    , NewTableEmployee as (
        select
            Department.Pk_DepartmentEmployee
            , Department.DepartamentName
            , Department.DepartmentGroupName
            , FirstJoinEmployeeTable.*
        from FirstJoinEmployeeTable
        left join Department
            on FirstJoinEmployeeTable.DepartmentEmployeeId = Department.DepartmentEmployeeId
    )

        , CreatingPkEmployee as (
        select
            farm_fingerprint(cast(EmployeeLoginId as string)) as Pk_EmployeeLogin
            , EmployeeLoginId
            , Pk_BusinessPerson as Fk_BusinessPerson
            , Pk_DepartmentEmployee as Fk_DepartmentEmployee
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
        from NewTableEmployee
    )

select *
from CreatingPkEmployee
