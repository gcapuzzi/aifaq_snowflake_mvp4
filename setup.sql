-- ============================================================
-- SETUP per SnowMind POC
-- Esegui questi comandi nel Snowflake worksheet
-- ============================================================

-- 1. Verifica che il dataset COVID sia disponibile
SHOW DATABASES LIKE 'COVID19%';

-- 2. Test Cortex LLM
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    'How many days are in a week? Answer in one word.'
) AS test_response;

-- 3. Preview delle tabelle principali
USE DATABASE COVID19_EPIDEMIOLOGICAL_DATA;
USE SCHEMA PUBLIC;

-- Casi globali - struttura
SELECT * FROM JHU_COVID_19 LIMIT 3;

-- Dati Italia - struttura  
SELECT * FROM PCM_DPS_COVID19 LIMIT 3;

-- Vaccinazioni - struttura
SELECT * FROM OWID_VACCINATIONS LIMIT 3;

-- 4. Query di esempio per verificare i dati
-- Totale casi Italia
SELECT 
    MAX(DATA) AS ultimo_aggiornamento,
    MAX(TOTALE_CASI) AS totale_casi,
    MAX(DECEDUTI) AS totale_deceduti
FROM PCM_DPS_COVID19
WHERE DENOMINAZIONE_REGIONE IS NULL OR DENOMINAZIONE_REGIONE = '';

-- Top 10 paesi per casi totali
SELECT 
    COUNTRY_REGION,
    MAX(CONFIRMED) AS total_confirmed,
    MAX(DEATHS) AS total_deaths
FROM JHU_COVID_19
WHERE PROVINCE_STATE IS NULL
GROUP BY COUNTRY_REGION
ORDER BY total_confirmed DESC
LIMIT 10;
