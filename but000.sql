-- Databricks notebook source
------------------------------------------------------------------------------
--
-- ods_crm_BUT000.sql
--
--  Create ods cache view of crm_BUT000 bringing back the latest deleted rows
--
--
-- Ver  Date        Author      Comment
-- 1.00 2020-12-27  thwaitef    Initial Version
-- 1.01 2021-02-15  lovelad1    Add xubname
------------------------------------------------------------------------------

-- compact tables
set hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;

DROP TABLE IF EXISTS ${environment}.${schema}ODS_CRM_BUT000 PURGE;

 CREATE TABLE IF NOT EXISTS ${environment}.${schema}ODS_CRM_BUT000 (
    partner   VARCHAR(10)
    ,partner_guid  VARCHAR(32)
    ,bpext   STRING
    ,birthdt    STRING
    ,deathdt    STRING
    ,xubname    STRING
    ,row_type               CHAR(1)
    ,tech_end_date        STRING
 )
 PARTITIONED BY (
     tech_datestamp          DATE
    ,bu_group    STRING
 )
 STORED AS ORC tblproperties ('orc.compress'='SNAPPY','orc.compress.size'='16384')
 ;


ALTER TABLE ${environment}.${schema}ODS_CRM_BUT000 DROP if EXISTS PARTITION(tech_datestamp='${tech_datestamp}') PURGE;

INSERT INTO ${environment}.${schema}ODS_CRM_BUT000 PARTITION (tech_datestamp='${tech_datestamp}',bu_group)
SELECT
    p.partner
    ,p.partner_guid
    ,p.bpext
    ,p.birthdt
    ,p.deathdt
    ,xubname
    ,row_type
    ,tech_end_date
    ,p.bu_group
FROM (
    select
    partner
    ,partner_guid
    ,bpext
    ,birthdt
    ,deathdt
    ,xubname
    ,row_type
    ,tech_end_date
    ,row_number() over (partition by partner order by row_type desc, tech_end_date desc) latest_row
    ,bu_group

    FROm (
        SELECT
            partner
            ,partner_guid
            ,bpext
            ,birthdt
            ,deathdt
            ,xubname
            ,'O' as row_type
            ,tech_end_date
            ,bu_group
        FROM
            ${prod_open_area}.CRM_BUT000

        WHERE
            tech_datestamp = '${tech_datestamp}' and client = '100'
        UNION ALL
        SELECT
            partner
            ,partner_guid
            ,bpext
            ,birthdt
            ,deathdt
            ,xubname
            ,'C' as row_type
            ,tech_end_date
            ,bu_group
        FROM
            ${prod_closed_area}.CRM_BUT000
        WHERE
            tech_datestamp <  '${tech_datestamp}'
        AND tech_closure_flag= 'DELETE'  and client = '100'
    ) a
) p where latest_row = 1

;
