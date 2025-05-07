DROP TABLE IF EXISTS sandbox.asn.t4_abt_points_churn;
CREATE TABLE IF NOT EXISTS sandbox.asn.t4_abt_points_churn AS

WITH tb_atividade AS (
    SELECT DISTINCT idCliente,
          date(dtCriacao) AS dtAtivo

    FROM silver.points.transacoes
),

tb_final AS (

    SELECT t1.*,
        COUNT(t2.dtAtivo) AS qtdAtividade,
        MAX(CASE WHEN t2.dtAtivo IS NOT NULL THEN 1 ELSE 0 END) AS flagAtividade

    FROM sandbox.asn.t4_points_churn_feature_store AS t1

    LEFT JOIN tb_atividade AS t2
    ON t1.idCliente = t2.idCliente
    AND t1.dtRef <= t2.dtAtivo
    AND t1.dtRef + INTERVAL 28 DAYS < t2.dtAtivo

    WHERE  QtdDiasUltTransacao <= 84

    GROUP BY ALL
)

SELECT *
FROM tb_final;