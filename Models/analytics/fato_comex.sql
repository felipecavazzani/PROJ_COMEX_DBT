{{ config(materialized='incremental') }}

WITH source AS (
    SELECT
        ROW_NUMBER() OVER () AS fato_sk,
        i.co_ano AS co_ano,
        i.co_mes AS co_mes,
        v.via_sk AS via_sk,
        n.ncm_sk AS ncm_sk,
        p.pais_sk AS pais_sk,
        u.uf_sk AS uf_sk,
        i.qtd_est as qtd_est,
        i.kg_liq as kg_liq,
        i.vl_fob as vl_fob,
        i.vl_frete as vl_frete,
        i.vl_seguro as vl_seguro,
        (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_carga  
    FROM 
        {{ ref('fct_comex_desnormalized') }} i  
    LEFT JOIN 
        {{ ref('dim_via') }} v ON i.co_via = v.co_via 
    LEFT JOIN 
        {{ ref('dim_ncm') }} n ON i.co_ncm = n.co_ncm  
    LEFT JOIN 
        {{ ref('dim_pais') }} p ON i.co_pais = p.co_pais  
    LEFT JOIN 
        {{ ref('dim_uf') }} u ON i.sg_uf = u.sg_uf
)

SELECT
    fato_sk,
    co_ano,
    co_mes,
    via_sk,
    ncm_sk,
    pais_sk,
    uf_sk,
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
