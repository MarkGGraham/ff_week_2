use database ff;
use warehouse wh_ff;

create or replace schema week_2;
use schema week_2;

create or replace stage parquet_stage;
create or replace file format FF_PARQUET type = PARQUET;

/* run snowsql job to load data as follows:
put file://d:/Data/Snowflake/FF/week_2/employees.parquet @parquet_stage;
*/

-- have a look at the data
select 
    -- METADATA$FILENAME::STRING as FILE_NAME
  METADATA$FILE_ROW_NUMBER as ROW_NUMBER
  , $1::VARIANT as CONTENTS
from @PARQUET_STAGE
  (file_format => 'FF_PARQUET')
order by ROW_NUMBER
;

-- Create a table in which to land the data if required?
create or replace table RAW_EMPLOYEE (
    FILE_NAME STRING
  , ROW_NUMBER INT
  , RESULT STRING
);

-- Ingest the data if needed
COPY INTO raw_employee
FROM (
  select 
      METADATA$FILENAME::STRING as FILE_NAME
    , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
    , $1::string as CONTENTS
  from @PARQUET_STAGE
    (file_format => 'FF_PARQUET')
)
;

-- Query the results
SELECT "RESULT" 
FROM RAW_EMPLOYEE
ORDER BY 
    FILE_NAME
  , ROW_NUMBER
;


--create target relational table
create table EMPLOYEE (
city string
,country string
,country_code string
,dept string
,education string
,email string
,employee_id string
,first_name string
,job_title string
,last_name string
,payroll_iban string
,postcode string
,street_name string
,street_num string
,time_zone string
,title string
);

--for testing
delete from employee;

--load directly from stage
insert into employee (
 city
,country
,country_code
,dept
,education
,email
,employee_id
,first_name
,job_title
,last_name
,payroll_iban
,postcode
,street_name
,street_num
,time_zone
,title
)
select 
 parse_json($1):city as city
,parse_json($1):country as country
,parse_json($1):country_code as country_code
,parse_json($1):dept as dept
,parse_json($1):education as education
,parse_json($1):email as email
,parse_json($1):employee_id as employee_id
,parse_json($1):first_name as first_name
,parse_json($1):job_title as job_title
,parse_json($1):last_name as last_name
,parse_json($1):payroll_iban as payroll_iban 
,parse_json($1):postcode as postcode
,parse_json($1):street_name as street_name
,parse_json($1):street_num as street_num
,parse_json($1):time_zone as time_zone
,parse_json($1):title as title  
from @PARQUET_STAGE (file_format => 'FF_PARQUET');
    
-- select * from @PARQUET_STAGE (file_format => 'FF_PARQUET');   

-- Create stream for employee to track changes
create or replace stream employee_check on table employee;

--run updates on employee
UPDATE employee SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
UPDATE employee SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
UPDATE employee SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
UPDATE employee SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
UPDATE employee SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;

-- get the updates
select employee_id, dept, job_title, metadata$row_id, metadata$action, metadata$isupdate from employee_check;