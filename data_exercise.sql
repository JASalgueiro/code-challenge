--DailyPositions in USD
SELECT 
     m.identifier,
     m.date,
     m.close_usd*p.shares AS DailyPosition
    
FROM 
    price m
    
JOIN 
    position p
    
ON 
    m.company_id = p.company_id
    AND
    m.date = p.date

ORDER BY 
    date DESC, DailyPosition DESC;

--TOP25 Companies
WITH Results AS (
    SELECT 
        m.identifier,
        AVG(m.close_usd * p.shares) AS Average,
        NTILE(4) OVER (ORDER BY AVG(m.close_usd * p.shares) DESC) AS Quartile
    FROM 
        price m
    JOIN 
        position p
    ON 
        m.company_id = p.company_id
        AND
        m.date = p.date
    WHERE 
        m.date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY
        m.identifier
)

SELECT 
    identifier,
    Average
FROM 
    Results
WHERE 
    Quartile = 1
ORDER BY 
    Average DESC;

--Daily Sector Position
SELECT 
c.sector_name,
p.date,
SUM(p.shares*m.close_usd) AS SECTORVALUE,
    
FROM 
    company c
    
JOIN 
    position p
ON 
    c.id = p.company_id

JOIN 
    price m
ON 
    (p.company_id=m.company_id AND p.date=m.date)
  
GROUP BY 
    c.sector_name,p.date

ORDER BY
    p.date DESC, SECTORVALUE DESC;