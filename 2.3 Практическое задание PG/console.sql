/*6.1*/
select "EMP_ID", "EMP_FULLNAME",
       now()::date - "EMP_JOINED"::date  as "experience_days"
from tested.employees;
/*6.2*/
select "EMP_ID", "EMP_FULLNAME",
       now()::date - "EMP_JOINED"::date  as "experience_days"
from tested.employees
limit 3;
/*6.3*/
select "EMP_ID"
from tested.employees
where "EMP_DRIVER" = true;
/*6.4*/
select e."EMP_ID"
from tested.employees e
left join tested."SCORE" s on e."EMP_ID"= s."EMP_ID"
where s."SCORE_VALUE"='D' or s."SCORE_VALUE"='E';
/*6.5*/
select e."EMP_SALARY_LEVEL"
from tested.employees e
order by e."EMP_SALARY_LEVEL" desc
limit 1;