  -- DATA REFERENCIA 
 DECLARE VARIABLE dtRfc DATE;
     SET VARIABLE dtRfc = CAST('2024-06-01' AS DATE);

-- CLIENTE ATIVOS (QUE APRESENTARAM AO MENOS 1 TRANSAÇÃO NOS ÚLTIMOS 28 DIAS)
WITH TB_CLI_ATV AS
(
  SELECT T.idCliente
       , dtRfc as dtRfc
       , MAX(CAST(T.dtCriacao AS DATE)) dtUltima 
    FROM silver.points.transacoes AS T
   WHERE CAST(dtCriacao AS DATE) BETWEEN DATEADD(DAY,-29,dtRfc) AND DATEADD(DAY,-1,dtRfc)
   GROUP BY  T.idCliente
   )

-- QUANTIDADE DE DIAS DISTINTOS COM ITERAÇÃO 
SELECT 
       B.dtRfc
     , A.idCliente
     , MAX(DATEDIFF(B.dtRfc,B.dtUltima)) QtdDiasUltTrans
     , COUNT(DISTINCT CASE WHEN CAST(A.dtCriacao AS DATE) BETWEEN DATEADD(DAY,-29,dtRfc) AND DATEADD(DAY,-1,dtRfc) THEN CAST(A.dtCriacao AS DATE) 
                       END) QtdTransUlt28d
     , SUM(CASE WHEN (CAST(A.dtCriacao AS DATE) BETWEEN DATEADD(DAY,-29,dtRfc) AND DATEADD(DAY,-1,dtRfc)) AND A.vlPontosTransacao >= 0 THEN A.vlPontosTransacao
           ELSE 0
            END) QtPts
  FROM silver.points.transacoes A
 INNER JOIN TB_CLI_ATV B
    ON A.idCliente = B.idCliente
 WHERE A.dtCriacao < dtRfc
 GROUP BY 
       B.dtRfc
     , A.idCliente



