with tb_transacoes_produto AS (

    SELECT t1.idCliente,
          t1.dtCriacao,
          t1.idTransacao,
          t2.idTransacaoProduto,
          t2.idProduto,
          t3.descNomeProduto

    FROM silver.points.transacoes AS t1

    LEFT JOIN silver.points.transacao_produto AS t2
    ON t1.idTransacao = t2.idTransacao

    LEFT JOIN silver.points.produtos AS t3
    ON t2.idProduto = t3.idProduto

    WHERE t1.dtCriacao < '{date}'
)

SELECT '{date}' AS dtSafra,
    idCliente, 
    count(*) AS qtd_transacoes,
    COUNT(DISTINCT CASE WHEN descNomeProduto = 'ChatMessage' THEN idTransacaoProduto END) / count(distinct idTransacaoProduto) AS pctidTransacaoProdutoChatMessage,
    COUNT(DISTINCT CASE WHEN descNomeProduto = 'Lista de presença' THEN idTransacaoProduto END) / count(distinct idTransacaoProduto) AS pctidTransacaoProdutoListaPresenca,
    COUNT(DISTINCT CASE WHEN descNomeProduto = 'Resgatar Ponei' THEN idTransacaoProduto END) / count(distinct idTransacaoProduto) AS pctidTransacaoProdutoResgatarPonei,
    COUNT(DISTINCT CASE WHEN descNomeProduto = 'Presença Streak' THEN idTransacaoProduto END) / count(distinct idTransacaoProduto) AS pctidTransacaoProdutoPresencaStreak,
    COUNT(DISTINCT CASE WHEN descNomeProduto = 'Troca de Pontos StreamElements' THEN idTransacaoProduto END) / count(distinct idTransacaoProduto) AS pctidTransacaoProdutoTrocaPontosStreamElements
FROM tb_transacoes_produto
GROUP BY idCliente

