  -- DATA REFERENCIA 
 DECLARE VARIABLE dtRfc DATE;
     SET VARIABLE dtRfc = CAST('2024-06-01' AS DATE);

-- CLIENTE ATIVOS (QUE APRESENTARAM AO MENOS 1 TRANSAÇÃO NOS ÚLTIMOS 28 DIAS)
WITH TB_CLI_ATV AS
(
  SELECT T.idCliente, dtRfc as dtRfc
    FROM silver.points.transacoes AS T
   WHERE CAST(dtCriacao AS DATE) BETWEEN DATEADD(DAY,-29,dtRfc) AND dtRfc
   GROUP BY  T.idCliente
   )

-- QUANTIDADE DE DIAS DISTINTOS COM ITERAÇÃO 
SELECT 
       B.dtRfc
     , A.idCliente
     , COUNT(DISTINCT CASE WHEN CAST(A.dtCriacao AS DATE) BETWEEN DATEADD(DAY,-8,dtRfc) AND dtRfc THEN CAST(A.dtCriacao AS DATE)
        END) QtdInter7d
     , COUNT(DISTINCT CASE WHEN CAST(A.dtCriacao AS DATE) BETWEEN DATEADD(DAY,-29,dtRfc) AND dtRfc THEN CAST(A.dtCriacao AS DATE) 
        END) QtdInter28d
     , COUNT(DISTINCT CASE WHEN CAST(A.dtCriacao AS DATE) BETWEEN DATEADD(DAY,-57,dtRfc) AND dtRfc THEN CAST(A.dtCriacao AS DATE)
        END) QtdInter56d
  FROM silver.points.transacoes A
 INNER JOIN TB_CLI_ATV B
    ON A.idCliente = B.idCliente
 WHERE A.dtCriacao < dtRfc
 GROUP BY 
       B.dtRfc
     , A.idCliente

