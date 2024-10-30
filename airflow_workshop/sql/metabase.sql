-- Доля учебных планов в статусе одобрено

SELECT
  status,
  COUNT(*) AS "count"
FROM
  cdm.up_actual_verified
WHERE
  ({{qualification_selected}} = 'все' OR qualification = {{qualification_selected}})
  [[AND ({{year}} IS NULL OR CAST(year AS TEXT) = {{year}})]]
  [[AND ({{unit}} IS NULL OR {{unit}} = '' OR unit ILIKE '%' || {{unit}} || '%')]]
GROUP BY
  status
ORDER BY
  status ASC


-- Доля учебных планов с некорректной трудоемкостью
SELECT
  laboriousness_status,
  COUNT(*) AS "count"
FROM
  cdm.up_actual_lab
WHERE
  ({{qualification_selected}} = 'все' OR qualification = {{qualification_selected}})
  [[AND ({{year}} IS NULL OR CAST(year AS TEXT) = {{year}})]]
  [[AND ({{unit}} IS NULL OR {{unit}} = '' OR unit ILIKE '%' || {{unit}} || '%')]]
GROUP BY
  laboriousness_status
ORDER BY
  laboriousness_status ASC


-- Заполнение аннотаций в динамике
WITH Sorted AS (
    SELECT
        *,
        CASE
            WHEN {{time_interval}} = 'День' THEN TO_CHAR(update_ts, 'YYYY-MM-DD')
            WHEN {{time_interval}} = 'Неделя' THEN TO_CHAR(update_ts, 'IYYY-IW')
            WHEN {{time_interval}} = 'Месяц' THEN TO_CHAR(update_ts, 'YYYY-MM')
            WHEN {{time_interval}} = 'Год' THEN TO_CHAR(update_ts, 'YYYY')
        END AS time_interval
    FROM cdm.wp_statuses_aggregation
  WHERE 1=1
    [[ AND ({{editor}} IS NULL OR {{editor}} = '' OR editor ILIKE '%' || {{editor}} || '%') ]]
    [[ AND ({{unit}} IS NULL OR {{unit}} = '' OR unit ILIKE '%' || {{unit}} || '%') ]]
),
Filtered AS (
    SELECT
        *,
        CASE
            WHEN {{time_interval}} = 'Месяц' OR {{time_interval}} = 'Год' THEN
                ROW_NUMBER() OVER (PARTITION BY wp_id, time_interval ORDER BY update_ts DESC)
            ELSE 1
        END AS rn
    FROM
        Sorted
)
SELECT
    time_interval AS "Временной период",
    COUNT(*) AS "Всего аннотаций",
    SUM(CASE WHEN wp_description IS NOT NULL AND wp_description <> '' THEN 1 ELSE 0 END) AS "Заполненных аннотаций",
    SUM(CASE WHEN wp_description IS NOT NULL AND wp_description <> '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS "Доля (%)"
FROM
    (SELECT * FROM Filtered WHERE rn = 1) AS Final
GROUP BY
    time_interval
ORDER BY
    time_interval;


-- Доля предметов в статусе "одобрено" в динамике
WITH Sorted AS (
    SELECT
        *,
        CASE
            WHEN {{time_interval}} = 'День' THEN TO_CHAR(update_ts, 'YYYY-MM-DD')
            WHEN {{time_interval}} = 'Неделя' THEN TO_CHAR(update_ts, 'IYYY-IW')
            WHEN {{time_interval}} = 'Месяц' THEN TO_CHAR(update_ts, 'YYYY-MM')
            WHEN {{time_interval}} = 'Год' THEN TO_CHAR(update_ts, 'YYYY')
        END AS time_interval
    FROM cdm.wp_statuses_aggregation
  WHERE 1=1
    [[ AND ({{editor}} IS NULL OR {{editor}} = '' OR editor ILIKE '%' || {{editor}} || '%') ]]
    [[ AND ({{unit}} IS NULL OR {{unit}} = '' OR unit ILIKE '%' || {{unit}} || '%') ]]
),
Filtered AS (
    SELECT
        *,
        CASE
            WHEN {{time_interval}} = 'Месяц' OR {{time_interval}} = 'Год' THEN
                ROW_NUMBER() OVER (PARTITION BY wp_id, time_interval ORDER BY update_ts DESC)
            ELSE 1
        END AS rn
    FROM
        Sorted
)
SELECT
    time_interval AS "Временной интервал",
    COUNT(*) AS "Всего предметов",
    SUM(CASE WHEN state_name = 'одобрено' THEN 1 ELSE 0 END) AS "Со статусом одобрено",
    SUM(CASE WHEN state_name = 'одобрено' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS "Доля (%)"
FROM
    (SELECT * FROM Filtered WHERE rn = 1) AS Final
GROUP BY
    time_interval
ORDER BY
    time_interval;
