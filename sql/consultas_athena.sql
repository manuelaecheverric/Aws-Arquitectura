-- Q1a. Noticias por fecha, medio y seccion (Gold)
SELECT pub_date, medio, seccion_principal, news_count
FROM gold_news_daily
ORDER BY pub_date, seccion_principal
;

-- Q1b. Detalle de noticias de una fecha y seccion (Silver, con filtro de particion)
SELECT pub_date, seccion_principal, title
FROM silver_news
WHERE pub_date = '2026-10-22'
ORDER BY seccion_principal, title
;

-- Q2. Diez organizaciones presentes en mas noticias (Gold)
SELECT entity_text AS organizacion, SUM(news_count) AS noticias
FROM gold_entity_daily
WHERE entity_type = 'ORGANIZATION'
GROUP BY entity_text
ORDER BY noticias DESC, organizacion
LIMIT 10
;

-- Q3. Presencia por dia de la entidad mas frecuente (Gold)
SELECT pub_date, entity_type, entity_text, news_count
FROM gold_entity_daily
WHERE entity_text = (
    SELECT entity_text
    FROM gold_entity_daily
    GROUP BY entity_text
    ORDER BY SUM(news_count) DESC, entity_text
    LIMIT 1
)
ORDER BY pub_date
;

-- Q4. Cinco frases clave mas frecuentes por seccion (Gold)
SELECT seccion_principal, key_phrase, noticias
FROM (
    SELECT seccion_principal, key_phrase, SUM(news_count) AS noticias,
           ROW_NUMBER() OVER (
               PARTITION BY seccion_principal
               ORDER BY SUM(news_count) DESC, key_phrase
           ) AS rk
    FROM gold_key_phrase_section_daily
    GROUP BY seccion_principal, key_phrase
)
WHERE rk <= 5
ORDER BY seccion_principal, noticias DESC
;

-- Q5a. Conciliacion por fecha: noticias en Silver vs Gold
SELECT COALESCE(s.pub_date, g.pub_date) AS pub_date,
       s.noticias_silver, g.noticias_gold,
       s.noticias_silver - g.noticias_gold AS diferencia
FROM (SELECT pub_date, COUNT(DISTINCT news_id) AS noticias_silver
      FROM silver_news GROUP BY pub_date) s
FULL OUTER JOIN
     (SELECT pub_date, SUM(news_count) AS noticias_gold
      FROM gold_news_daily GROUP BY pub_date) g
ON s.pub_date = g.pub_date
ORDER BY 1
;

-- Q5b. Conciliacion de totales: noticias, entidades y frases clave
SELECT 'noticias' AS indicador,
       (SELECT COUNT(DISTINCT news_id) FROM silver_news) AS silver,
       (SELECT SUM(news_count) FROM gold_news_daily) AS gold
UNION ALL
SELECT 'entidades',
       (SELECT COUNT(*) FROM silver_news_entities),
       (SELECT SUM(news_count) FROM gold_entity_daily)
UNION ALL
SELECT 'frases_clave',
       (SELECT COUNT(*) FROM silver_news_key_phrases),
       (SELECT SUM(news_count) FROM gold_key_phrase_section_daily)
;

-- Q6a. Bytes escaneados CON filtro de particion
SELECT COUNT(*) AS frases, COUNT(DISTINCT key_phrase) AS frases_distintas
FROM silver_news_key_phrases
WHERE pub_date = '2026-10-22'
;

-- Q6b. Bytes escaneados SIN filtro de particion
SELECT COUNT(*) AS frases, COUNT(DISTINCT key_phrase) AS frases_distintas
FROM silver_news_key_phrases
