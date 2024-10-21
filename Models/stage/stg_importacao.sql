{{config(materialized='incremental')}}

WITH source AS (
SELECT 
    "CO_ANO" as co_ano, 
    "CO_MES" as co_mes, 
    "CO_NCM" as co_ncm,  
    "CO_PAIS" as co_pais, 
    "SG_UF_NCM" as sg_uf, 
    "CO_VIA" as co_via, 
    "CO_URF" as co_urf, 
    "QT_ESTAT" as qtd_est, 
    "KG_LIQUIDO" as kg_liq, 
    "VL_FOB" as vl_fob, 
    "VL_FRETE" as vl_frete, 
    "VL_SEGURO" as vl_seguro,
    (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "data_carga"
FROM {{source('sources', 'imp')}}
)

SELECT
    co_ano,
    co_mes,
    co_ncm,
    co_pais,
    sg_uf,
    co_via,
    co_urf,
    qtd_est,
    kg_liq,
    vl_fob,
    vl_frete,
    vl_seguro,
    data_carga
FROM source

{% if is_incremental() %}
WHERE (co_ano, co_mes) NOT IN (SELECT co_ano,co_mes FROM {{this}})
{% endif %}