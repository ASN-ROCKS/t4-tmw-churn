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

)

SELECT * FROM tb_cliente_agrupado