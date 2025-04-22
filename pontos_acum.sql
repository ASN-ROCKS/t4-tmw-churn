WITH tb_transacoes AS (

  SELECT *
  FROM silver.points.transacoes
  WHERE dtCriacao < '2024-06-01'

),

tb_saldo_pontos AS (

  SELECT idCliente,
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
    sum(CASE WHEN vlPontosTransacao < 0 AND dtCriacao >= date('2024-06-01') - INTERVAL 56 DAYS THEN vlPontosTransacao ELSE 0 END ) AS vlPontosNegD56

  FROM tb_transacoes
  GROUP BY ALL

)

SELECT * FROM tb_saldo_pontos