with
    EmployeeDepartment as (
        select *
        from {{ ref('stg_erp_Employee_Department_History') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(BusinessPersonId as string)) as Pk_BusinessPerson
            , BusinessPersonId
            , DepartmentEmployeeId
        from EmployeeDepartment
    )

select *
from CreatingPk
