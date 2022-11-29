/*A*/
select "EMP_FULLNAME"
from tested.employees
order by "EMP_SALARY_LEVEL" desc
limit 1;
/*B*/
select "EMP_FULLNAME"
from tested.employees
order by "EMP_FULLNAME";
/*C*/
with experience as (
    select
        EXTRACT(DAY FROM now()-"EMP_JOINED"::timestamp) as "days",
        "EMP_LEVEL"
    from tested.employees)
select "EMP_LEVEL", avg(days)
from experience
group by "EMP_LEVEL";
/*D*/
select e."EMP_FULLNAME", d."DEP_NAME"
from tested.employees e
left join tested.departments d on e."EMP_DEPARTMENT_ID"=d."DEP_ID";
/*E*/
with max_dep_salary as (
    select e."EMP_DEPARTMENT_ID",
           e."EMP_SALARY_LEVEL",
           e."EMP_FULLNAME",
           row_number() over (partition by "EMP_DEPARTMENT_ID" order by "EMP_SALARY_LEVEL" desc) as "row_num"
    from tested.employees e
)
select d."DEP_NAME", m."EMP_FULLNAME", m."EMP_SALARY_LEVEL"
from max_dep_salary m
         left join tested.departments d on
        d."DEP_ID"=m."EMP_DEPARTMENT_ID"
where m."row_num"=1