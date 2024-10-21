{{config(materialized='incremental')}}

WITH source AS (
SELECT 
	"CO_VIA" as co_via, 
	"NO_VIA" as nome_via,
	(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "data_carga"
FROM {{source('sources', 'via')}}
)

SELECT
    co_via,
    nome_via,
    data_carga
FROM source

{% if is_incremental() %}
WHERE co_via NOT IN (SELECT co_via FROM {{this}})
{% endif %}