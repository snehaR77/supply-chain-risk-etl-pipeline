SELECT supplier_id, AVG(risk_score) as avg_risk
FROM risk_metrics
GROUP BY supplier_id
ORDER BY avg_risk DESC;
