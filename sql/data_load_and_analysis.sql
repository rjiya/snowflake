CREATE WAREHOUSE WH_XL WAREHOUSE_SIZE = XLARGE;


CREATE DATABASE Motor_Vehicle_collisions;
CREATE SCHEMA MVC;
CREATE TABLE Motor_Vehicle_Collisions.MVC.Crashes
(
    CRASH_DATE VARCHAR,
    CRASH_TIME VARCHAR,
    BOROUGH VARCHAR,
    ZIP_CODE VARCHAR,
    LATITUDE VARCHAR,
    LONGITUDE VARCHAR,
    LOCATION VARCHAR,
    ON_STREET_NAME VARCHAR,
    CROSS_STREET_NAME VARCHAR,
    OFF_STREET_NAME VARCHAR,
    NUMBER_OF_PERSONS_INJURED VARCHAR,
    NUMBER_OF_PERSONS_KILLED VARCHAR,
    NUMBER_OF_PEDESTRIANS_INJURED VARCHAR,
    NUMBER_OF_PEDESTRIANS_KILLED VARCHAR,
    NUMBER_OF_CYCLIST_INJURED VARCHAR,
    NUMBER_OF_CYCLIST_KILLED VARCHAR,
    NUMBER_OF_MOTORIST_INJURED VARCHAR,
    NUMBER_OF_MOTORIST_KILLED VARCHAR,
    CONTRIBUTING_FACTOR_VEHICLE_1 VARCHAR,
    CONTRIBUTING_FACTOR_VEHICLE_2 VARCHAR,
    CONTRIBUTING_FACTOR_VEHICLE_3 VARCHAR,
    CONTRIBUTING_FACTOR_VEHICLE_4 VARCHAR,
    CONTRIBUTING_FACTOR_VEHICLE_5 VARCHAR,
    COLLISION_ID VARCHAR,
    VEHICLE_TYPE_CODE_1 VARCHAR,
    VEHICLE_TYPE_CODE_2 VARCHAR,
    VEHICLE_TYPE_CODE_3 VARCHAR,
    VEHICLE_TYPE_CODE_4 VARCHAR,
    VEHICLE_TYPE_CODE_5 VARCHAR
 );


-- drop database MOTOR_VEHICLE_COLLISION;

CREATE OR REPLACE FILE FORMAT MOTOR_VEHICLE_COLLISIONS.MVC.my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 0
  NULL_IF = ('NULL', 'null')
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = true
  COMPRESSION = NONE;

list @internal_stage;


select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29
from @internal_stage/Motor_Vehicle_Collisions_-_Crashes_20241218.csv (file_format=> 'MOTOR_VEHICLE_COLLISIONS.MVC.my_csv_format') limit 100;


insert into MOTOR_VEHICLE_COLLISIONS.MVC.CRASHES
select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29
from @internal_stage/Motor_Vehicle_Collisions_-_Crashes_20241218.csv (file_format=> 'MOTOR_VEHICLE_COLLISIONS.MVC.my_csv_format')
where $1 != 'CRASH DATE';


select * from motor_vehicle_collisions.mvc.crashes limit 100;
select count(*) from motor_vehicle_collisions.mvc.crashes;

select crash_date, to_date(crash_date) from motor_vehicle_collisions.mvc.crashes;

select distinct borough from motor_vehicle_collisions.mvc.crashes;

select borough, count(*) as record_count from motor_vehicle_collisions.mvc.crashes 
where borough is not null
group by all
order by record_count desc;

select year(to_date(crash_date)) as crash_year, count(*) as record_count
from motor_vehicle_collisions.mvc.crashes
group by all
--order by record_count desc;
order by crash_year asc;

select 
    case
        when to_time(crash_time) between to_time('6:00') and to_time('18:00') then 'Day_Time'
        else 'Night_Time'
    end as crash_time_1,
    count(*)
from motor_vehicle_collisions.mvc.crashes
group by all;

select
    borough,
    case
        when to_time(crash_time) between to_time('9:00') and to_time('21:00') then  'Busy_Time'
        else 'Quiet_Time'
    end as crash_time_1,
    count(*)
from motor_vehicle_collisions.mvc.crashes 
group by all
order by 1, 2;
        
select 
    borough, 
    count(*) as record_count,
    (count(*) * 100.0 / (select count(*) from MOTOR_VEHICLE_COLLISIONS.MVC.crashes)) as borough_percentage
from motor_vehicle_collisions.mvc.crashes
----where borough is not null
group by borough order by record_count desc;


select * from motor_vehicle_collisions.mvc.crashes limit 100;

select borough, count(*) as record_count, sum(number_of_persons_killed) as total_person_killed
from motor_vehicle_collisions.mvc.crashes
where borough is not null
group by borough
order by record_count desc;


select borough, count(*) as record_count, number_of_persons_killed
from motor_vehicle_collisions.mvc.crashes
where borough is not null
group by borough, number_of_persons_killed
order by record_count desc;

select Date(to_date(crash_date)) as crash_day,
       count(*) as record_day
from MOTOR_VEHICLE_COLLISIONS.MVC.CRASHES
GROUP BY crash_day
order by crash_day;

SELECT DATE_TRUNC('week', to_date(crash_date)) AS start_of_week, 
       COUNT(*) AS total_crashes
FROM motor_vehicle_collisions.mvc.crashes
GROUP BY start_of_week
ORDER BY start_of_week;

SELECT YEAR(to_date(crash_date)) AS crash_year,
       WEEK(to_date(crash_date)) AS crash_week, 
       COUNT(*) AS total_crashes
FROM motor_vehicle_collisions.mvc.crashes
where YEAR(to_date(crash_date)) like 2013
GROUP BY crash_year, crash_week
ORDER BY crash_year, crash_week;

select 
    borough, 
    count(*) as record_count,
    (count(*) * 100.0 / (select count(*) from MOTOR_VEHICLE_COLLISIONS.MVC.crashes)) as borough_percentage
from motor_vehicle_collisions.mvc.crashes
----where borough is not null
group by borough 
HAVING record_count < 700000
order by record_count desc;
 
// Analysis by zip code
select * from motor_vehicle_collisions.mvc.crashes limit 100;


