SELECT idCliente,
       SUM(vlPontosTransacao) AS qtPontos
FROM silver.points.transacoes
WHERE dtCriacao < '{date}'
GROUP BY ALL