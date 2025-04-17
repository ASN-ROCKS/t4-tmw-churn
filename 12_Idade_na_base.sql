WITH tb_clientes_base AS (
    SELECT 
        idCliente,
        min(dtCriacao) AS dtCriacao_min
    FROM silver.points.transacoes
    WHERE dtCriacao < '{date}'
    GROUP BY idCliente
)

SELECT 
    '{date}' AS dtSafra,
    idCliente,
    datediff(date('{date}'), date(dtCriacao_min)) AS qtd_idade_em_dias
FROM tb_clientes_base