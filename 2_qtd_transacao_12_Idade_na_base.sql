WITH tb_clientes_base AS (
    SELECT 
        idCliente,
        min(dtCriacao) AS dtCriacao_min,
        count(*) AS qtd_transacoes
    FROM silver.points.transacoes
    WHERE dtCriacao < '{date}'
    GROUP BY idCliente
)

SELECT 
    '{date}' AS dtSafra,
    idCliente,
    datediff(date('{date}'), date(dtCriacao_min)) AS qtd_idade_em_dias,
    qtd_transacoes
FROM tb_clientes_base