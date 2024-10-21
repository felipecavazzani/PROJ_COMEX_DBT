{{config(materialized='incremental')}}

SELECT 
    CAST(ib.co_ano AS VARCHAR) as co_ano,
    CAST(ib.co_mes AS VARCHAR) as co_mes,
    CAST(nb.co_ncm AS VARCHAR),
    UPPER(nb.nome_ncm) as nome_ncm,
    CAST(pb.co_pais AS VARCHAR),
    UPPER(pb.co_isoa3) as co_isoa3,
    CAST(ub.co_uf AS VARCHAR),
    ub.sg_uf,
    UPPER(ub.no_uf) as no_uf,
    ub.no_regiao,
    CAST(vb.co_via AS VARCHAR),
    vb.nome_via,
    ib.qtd_est,
    ib.kg_liq,
    ib.vl_fob,
    ib.vl_frete,
    ib.vl_seguro
FROM 
    {{ ref('stg_importacao') }} ib
LEFT JOIN 
    {{ ref('stg_ncm') }} nb ON ib.co_ncm = nb.co_ncm 
LEFT JOIN 
    {{ ref('stg_pais') }} pb ON ib.co_pais = pb.co_pais 
LEFT JOIN 
    {{ ref('stg_uf') }} ub ON ib.sg_uf = ub.sg_uf 
LEFT JOIN 
    {{ ref('stg_via') }} vb ON ib.co_via = vb.co_via


{% if is_incremental() %}
WHERE (ib.co_ano, ib.co_mes) NOT IN (SELECT ib.co_ano,ib.co_mes FROM {{this}})
{% endif %}
