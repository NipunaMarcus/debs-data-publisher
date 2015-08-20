CREATE TEMPORARY TABLE debsData USING CarbonAnalytics OPTIONS (tableName "DEBS_DATA", schema "id STRING, value FLOAT, property BOOLEAN, plug_id INT, household_id INT, house_id INT");

create temporary table plugUsage using CarbonAnalytics options (tableName "plug_usage", schema "house_id INT, household_id INT, plug_id INT, usage FLOAT -sp");

insert overwrite table plugUsage select house_id, household_id, plug_id, max(value) - min (value) as usage from debsData where property = false group by house_id, household_id, plug_id ;

create temporary table householdUsage using CarbonAnalytics options (tableName "household_usage", schema "house_id INT, household_id INT, usage FLOAT");

insert overwrite table householdUsage select house_id, household_id,  sum(usage) as usage from plugUsage group by house_id, household_id ;

create temporary table houseUsage using CarbonAnalytics options (tableName "house_usage", schema "house_id INT, usage FLOAT");

insert overwrite table houseUsage select house_id, sum(usage) as usage from householdUsage group by house_id ;

create temporary table avgUsage using CarbonAnalytics options (tableName "avg_usage", schema "house_avg float, household_avg FLOAT, plug_avg float");

insert overwrite table avgUsage select * from (select avg(usage) from houseUsage) as a1 join (select avg(usage) from householdUsage) as a2 join (select avg(usage) from plugUsage) as a3 on 1=1 ;