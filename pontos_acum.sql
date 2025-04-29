WITH tb_transacoes AS (

  SELECT *
  FROM silver.points.transacoes
  WHERE dtCriacao < '2024-06-01'

),

tb_cliente_agrupado AS (

  SELECT idCliente,

  count(distinct idTransacao) / count(distinct date(dtCriacao)) AS qtdTransacoesDia,

  COALESCE(count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 7 DAYS THEN idTransacao ELSE 0 END) /
    count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 7 DAYS THEN date(dtCriacao) END),0) AS qtdTransacaoDiaD7,

  COALESCE(count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 14 DAYS THEN idTransacao ELSE 0 END) /
    count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 14 DAYS THEN date(dtCriacao) END),0) AS qtdTransacaoDiaD14,

  COALESCE(count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 28 DAYS THEN idTransacao ELSE 0 END) /
    count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 28 DAYS THEN date(dtCriacao) END),0) AS qtdTransacaoDiaD28,

  COALESCE(count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 56 DAYS THEN idTransacao ELSE 0 END) /
    count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 56 DAYS THEN date(dtCriacao) END),0) AS qtdTransacaoDiaD56,

    sum(vlPontosTransacao) AS qtPontos,

    sum(CASE WHEN vlPontosTransacao > 0 THEN vlPontosTransacao ELSE 0 END ) AS vlPontosPos,
    sum(CASE WHEN vlPontosTransacao < 0 THEN vlPontosTransacao ELSE 0 END ) AS vlPontosNeg,

    sum(CASE WHEN vlPontosTransacao > 0 AND dtCriacao >= date('2024-06-01') - INTERVAL 7 DAYS THEN vlPontosTransacao ELSE 0 END ) AS vlPontosPosD7,
    sum(CASE WHEN vlPontosTransacao < 0 AND dtCriacao >= date('2024-06-01') - INTERVAL 7 DAYS THEN vlPontosTransacao ELSE 0 END ) AS vlPontosNegD7,

    sum(CASE WHEN vlPontosTransacao > 0 AND dtCriacao >= date('2024-06-01') - INTERVAL 14 DAYS THEN vlPontosTransacao ELSE 0 END ) AS vlPontosPosD14,
    sum(CASE WHEN vlPontosTransacao < 0 AND dtCriacao >= date('2024-06-01') - INTERVAL 14 DAYS THEN vlPontosTransacao ELSE 0 END ) AS vlPontosNegD14,

    sum(CASE WHEN vlPontosTransacao > 0 AND dtCriacao >= date('2024-06-01') - INTERVAL 28 DAYS THEN vlPontosTransacao ELSE 0 END ) AS vlPontosPosD28,
    sum(CASE WHEN vlPontosTransacao < 0 AND dtCriacao >= date('2024-06-01') - INTERVAL 28 DAYS THEN vlPontosTransacao ELSE 0 END ) AS vlPontosNegD28,

    sum(CASE WHEN vlPontosTransacao > 0 AND dtCriacao >= date('2024-06-01') - INTERVAL 56 DAYS THEN vlPontosTransacao ELSE 0 END ) AS vlPontosPosD56,
    sum(CASE WHEN vlPontosTransacao < 0 AND dtCriacao >= date('2024-06-01') - INTERVAL 56 DAYS THEN vlPontosTransacao ELSE 0 END ) AS vlPontosNegD56,

    sum(abs(vlPontosTransacao)) / count(distinct date(dtCriacao)) AS qtdPontosDia,

    COALESCE(sum(CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 7 DAYS THEN abs(vlPontosTransacao) ELSE 0 END) /
      count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 7 DAYS THEN date(dtCriacao) END),0) AS qtdPontosDiaD7,

    COALESCE(sum(CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 14 DAYS THEN abs(vlPontosTransacao) ELSE 0 END) /
      count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 14 DAYS THEN date(dtCriacao) END),0) AS qtdPontosDiaD14,

    COALESCE(sum(CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 28 DAYS THEN abs(vlPontosTransacao) ELSE 0 END) /
      count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 28 DAYS THEN date(dtCriacao) END),0) AS qtdPontosDiaD28,

    COALESCE(sum(CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 56 DAYS THEN abs(vlPontosTransacao) ELSE 0 END) /
      count(DISTINCT CASE WHEN dtCriacao >= date('2024-06-01') - INTERVAL 56 DAYS THEN date(dtCriacao) END),0) AS qtdPontosDiaD56

  FROM tb_transacoes
  GROUP BY ALL
),

tb_daily AS (

  SELECT idCliente,
        date(dtCriacao) AS dtDia,
        (max(unix_timestamp(dtCriacao)) - min(unix_timestamp(dtCriacao))) / 60 + 1 AS qtMinutosAssistidos,
        count(distinct idTransacao) AS qtdeTransacaoDia
  FROM tb_transacoes
  GROUP BY ALL
),

tb_horas_assistdas AS (

    SELECT idCliente,
          
          sum(qtMinutosAssistidos) AS qtMinutosAssistidos,
          sum(CASE WHEN dtDia >= date('2024-06-01') - INTERVAL 7 DAYS THEN qtMinutosAssistidos ELSE 0 END) AS qtMinutosAssistidoD7,
          sum(CASE WHEN dtDia >= date('2024-06-01') - INTERVAL 14 DAYS THEN qtMinutosAssistidos ELSE 0 END) AS qtMinutosAssistidosD14,
          sum(CASE WHEN dtDia >= date('2024-06-01') - INTERVAL 28 DAYS THEN qtMinutosAssistidos ELSE 0 END) AS qtMinutosAssistidosD28,
          sum(CASE WHEN dtDia >= date('2024-06-01') - INTERVAL 56 DAYS THEN qtMinutosAssistidos ELSE 0 END) AS qtMinutosAssistidosD56,
          
          avg(qtMinutosAssistidos) AS avgMinutosAssistidos,
          avg(CASE WHEN dtDia >= date('2024-06-01') - INTERVAL 7 DAYS THEN qtMinutosAssistidos END) AS avgMinutosAssistidoD7,
          avg(CASE WHEN dtDia >= date('2024-06-01') - INTERVAL 14 DAYS THEN qtMinutosAssistidos END) AS avgMinutosAssistidosD14,
          avg(CASE WHEN dtDia >= date('2024-06-01') - INTERVAL 28 DAYS THEN qtMinutosAssistidos END) AS avgMinutosAssistidosD28,
          avg(CASE WHEN dtDia >= date('2024-06-01') - INTERVAL 56 DAYS THEN qtMinutosAssistidos END) AS avgMinutosAssistidosD56

    FROM tb_daily
    GROUP BY ALL
),

tb_user AS (

SELECT distinct idCliente
FROM tb_transacoes
),

tb_calendar AS (

SELECT DISTINCT date(dtCriacao)
FROM tb_transacoes

),

tb_cross AS (

SELECT * FROM tb_user, tb_calendar

),

tb_dia_transacao_completa_d7 AS (

    SELECT t1.*,
          coalesce(t2.qtdeTransacaoDia,0) AS qtdeTransacao
    FROM tb_cross AS t1

    LEFT JOIN tb_daily AS t2
    ON t1.idCliente = t2.idCliente
    AND t1.dtCriacao = t2.dtDia

    WHERE t1.dtCriacao >= date('2024-06-01') - INTERVAL 8 DAYS

    ORDER BY t1.idCliente, t1.dtCriacao
),

tb_lag_d7 AS (

SELECT *,
       lag(qtdeTransacao) OVER (PARTITION BY idCliente ORDER BY dtCriacao) AS lagQtdeTransacao

FROM tb_dia_transacao_completa_d7

),

tb_ifr AS (

  SELECT idCliente,
          count(distinct dtCriacao) AS qtdeDias,
          sum(case when qtdeTransacao - lagQtdeTransacao > 0 then qtdeTransacao - lagQtdeTransacao end) as ganhos,
          sum(case when qtdeTransacao - lagQtdeTransacao < 0 then abs(qtdeTransacao - lagQtdeTransacao) end) as perdas,

          100 - 100 / (1 + sum(case when qtdeTransacao - lagQtdeTransacao > 0 then qtdeTransacao - lagQtdeTransacao end) / sum(case when qtdeTransacao - lagQtdeTransacao < 0 then abs(qtdeTransacao - lagQtdeTransacao) end)) As ifr_bruto,

          100 - 100 / (2 + sum(case when qtdeTransacao - lagQtdeTransacao > 0 then qtdeTransacao - lagQtdeTransacao end) / (1+sum(case when qtdeTransacao - lagQtdeTransacao < 0 then abs(qtdeTransacao - lagQtdeTransacao) end))) As ifr_plus1,

          case when sum(case when qtdeTransacao - lagQtdeTransacao < 0 then abs(qtdeTransacao - lagQtdeTransacao) end) = 0
                  then 100 - 100 / (2 + sum(case when qtdeTransacao - lagQtdeTransacao > 0 then qtdeTransacao - lagQtdeTransacao end) / (1+sum(case when qtdeTransacao - lagQtdeTransacao < 0 then abs(qtdeTransacao - lagQtdeTransacao) end)))
              else 100 - 100 / (1 + sum(case when qtdeTransacao - lagQtdeTransacao > 0 then qtdeTransacao - lagQtdeTransacao end) / sum(case when qtdeTransacao - lagQtdeTransacao < 0 then abs(qtdeTransacao - lagQtdeTransacao) end))
              end as ifr_plus1_case

  FROM tb_lag_d7
  WHERE lagQtdeTransacao IS NOT NULL

  GROUP BY ALL

),

tb_final  AS (

  SELECT t1.*,
        t2.qtMinutosAssistidos,
        t3.ifr_bruto as vlIFRBruto,
        t3.ifr_plus1 AS vlIFRPlus1,
        t3.ifr_plus1_case AS vlIFRPlus1Case

  FROM tb_cliente_agrupado AS t1
  LEFT JOIN tb_horas_assistdas AS t2
  ON t1.idCliente = t2.idCliente

  LEFT JOIN tb_ifr AS t3
  ON t1.idCliente = t3.idCliente

)

SELECT *
FROM tb_final