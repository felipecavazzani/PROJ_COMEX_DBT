WITH valida_uf AS (
    SELECT
        uf_sk,
        COUNT(*) AS conta_duplicados,
        SUM(CASE WHEN co_uf IS NULL OR sg_uf IS NULL OR no_uf IS NULL OR no_regiao IS NULL  OR data_carga IS NULL THEN 1 ELSE 0 END) AS conta_nulos
    FROM {{ ref('dim_uf') }}  
    GROUP BY uf_sk
)

SELECT *
FROM valida_uf
WHERE conta_duplicados > 1
   OR conta_nulos > 0