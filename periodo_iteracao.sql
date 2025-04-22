WITH tb_transacoes (
    SELECT *
    FROM silver.points.transacoes
    WHERE dtCriacao < '2024-06-01'
),

tb_dia_semana AS (

    SELECT 
      idCliente,
      date_format(to_timestamp(dtCriacao, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'), 'EEEE') AS dia_da_semana,
      COUNT(*) AS total,
      ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY idCliente), 2) AS percentual
    FROM tb_transacoes
    GROUP BY idCliente, dia_da_semana
    ORDER BY idCliente, percentual DESC

),

tb_periodo_dia AS (

  SELECT 
    idCliente,
    CASE 
      WHEN hour(to_timestamp(dtCriacao, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX')) BETWEEN 6 AND 11 THEN 'Manhã'
      WHEN hour(to_timestamp(dtCriacao, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX')) BETWEEN 12 AND 17 THEN 'Tarde'
      WHEN hour(to_timestamp(dtCriacao, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX')) BETWEEN 18 AND 23 THEN 'Noite'
      ELSE 'Madrugada'
    END AS periodo_dia,
    COUNT(*) AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY idCliente), 2) AS percentual
  FROM tb_transacoes
  GROUP BY idCliente, periodo_dia
  ORDER BY idCliente, percentual DESC

),

tb_semana_pivot AS (

  SELECT idCliente,
        sum(case when dia_da_semana = 'Sunday' then percentual else 0 end) as pctSunday,
        sum(case when dia_da_semana = 'Monday' then percentual else 0 end) as pctMonday,
        sum(case when dia_da_semana = 'Tuesday' then percentual else 0 end) as pctTuesday,
        sum(case when dia_da_semana = 'Wednesday' then percentual else 0 end) as pctWednesday,
        sum(case when dia_da_semana = 'Thursday' then percentual else 0 end) as pctThursday,
        sum(case when dia_da_semana = 'Friday' then percentual else 0 end) as pctFriday,
        sum(case when dia_da_semana = 'Saturday' then percentual else 0 end) as pctSaturday

  FROM tb_dia_semana
  GROUP BY ALL

),

tb_periodo_pivot AS (

  SELECT 
    idCliente,
    sum(CASE WHEN periodo_dia = 'Manhã' THEN percentual ELSE 0 END) AS pctManha,
    sum(CASE WHEN periodo_dia = 'Tarde' THEN percentual ELSE 0 END) AS pctTarde,
    sum(CASE WHEN periodo_dia = 'Noite' THEN percentual ELSE 0 END) AS pctNoite,
    sum(CASE WHEN periodo_dia = 'Madrugada' THEN percentual ELSE 0 END) AS pctMadrugada

  FROM tb_periodo_dia
  GROUP BY ALL 
)

SELECT t1.*,
       t2.pctManha,
       t2.pctTarde,
       t2.pctNoite,
       t2.pctMadrugada

FROM tb_semana_pivot AS t1
LEFT JOIN tb_periodo_pivot AS t2
ON t1.idCliente = t2.idCliente