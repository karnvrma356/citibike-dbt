{{ config( materialized='table') }}

SELECT * FROM 
{{ ref('STG_1_DIM_DATE') }}
