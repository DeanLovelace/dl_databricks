-- Databricks notebook source
CREATE WIDGET TEXT environment DEFAULT "environment";
CREATE WIDGET TEXT tech_datestamp DEFAULT "9999-12-12";
CREATE WIDGET TEXT prod_open_area DEFAULT "prod_open_area";
CREATE WIDGET TEXT prod_closed_area DEFAULT "prod_closed_area";

-- COMMAND ----------

select '${environment}' as environment, '${tech_datestamp}' as tech_datestamp, '${prod_open_area}' as prod_open_area, '${prod_closed_area}' as prod_closed_area
