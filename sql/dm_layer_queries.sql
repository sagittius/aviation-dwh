-- ============================================================
-- DWH Авиаперевозки: запросы слоя DM
-- Проект: Хранилище данных для анализа авиаперевозок США
-- Слой: DM (Data Marts — аналитические витрины)
-- DAG: dm_flights_full_etl
-- Источники: dds.flights, dds.airport, dds.weather
-- ============================================================


-- ============================================================
-- 1. dm.cancel_flights
--    Витрина отменённых рейсов
--    Источник: dds.flights WHERE cancelled_flg = 1
-- ============================================================

CREATE TABLE dm.cancel_flights AS
SELECT
    f.flight_rk                                             AS flight_id,
    f.carrier_flight_num,
    f.flight_dt::date                                       AS flight_date,
    f.actual_dep_tm,

    -- Собираем полный timestamp вылета из даты + строкового времени формата HHMM
    (
        f.flight_dt::date || ' ' ||
        SUBSTRING(f.actual_dep_tm, 1, 2) || ':' ||
        SUBSTRING(f.actual_dep_tm, 3, 2)
    )::timestamp                                            AS flight_dttm_local,

    f.origin_code,
    f.dest_code,
    f.carrier_code,
    f.tail_num,
    f.distance                                              AS distance_m,

    -- COALESCE: если задержка не указана — подставляем 0, чтобы агрегаты не ломались
    COALESCE(f.dep_delay_min,           0)                  AS dep_delay_min,
    COALESCE(f.arr_delay_min,           0)                  AS arr_delay_min,
    COALESCE(f.carrier_delay_min,       0)                  AS carrier_delay_min,
    COALESCE(f.weather_delay_min,       0)                  AS weather_delay_min,
    COALESCE(f.nas_delay_min,           0)                  AS nas_delay_min,
    COALESCE(f.security_delay_min,      0)                  AS security_delay_min,
    COALESCE(f.late_aircraft_delay_min, 0)                  AS late_aircraft_delay_min,

    f.processed_dttm

FROM dds.flights f
WHERE f.cancelled_flg = 1;   -- только отменённые рейсы


-- ============================================================
-- 2. dm.success_flights
--    Витрина выполненных рейсов
--    Источник: dds.flights WHERE cancelled_flg = 0
-- ============================================================

CREATE TABLE dm.success_flights AS
SELECT
    f.flight_rk                                             AS flight_id,
    f.carrier_flight_num,
    f.flight_dt::date                                       AS flight_date,
    f.actual_dep_tm,

    -- Полный timestamp вылета (дата + время в формате HHMM → TIMESTAMP)
    (
        f.flight_dt::date || ' ' ||
        SUBSTRING(f.actual_dep_tm, 1, 2) || ':' ||
        SUBSTRING(f.actual_dep_tm, 3, 2)
    )::timestamp                                            AS flight_dttm_local,

    f.origin_code,
    f.dest_code,
    f.carrier_code,
    f.tail_num,
    f.distance                                              AS distance_m,

    COALESCE(f.dep_delay_min,           0)                  AS dep_delay_min,
    COALESCE(f.arr_delay_min,           0)                  AS arr_delay_min,
    COALESCE(f.carrier_delay_min,       0)                  AS carrier_delay_min,
    COALESCE(f.weather_delay_min,       0)                  AS weather_delay_min,
    COALESCE(f.nas_delay_min,           0)                  AS nas_delay_min,
    COALESCE(f.security_delay_min,      0)                  AS security_delay_min,
    COALESCE(f.late_aircraft_delay_min, 0)                  AS late_aircraft_delay_min,

    f.processed_dttm

FROM dds.flights f
WHERE f.cancelled_flg = 0;   -- только выполненные рейсы


-- ============================================================
-- 3. dm.dep_arr_report
--    Отчёт по вылетам и прилётам: количество операций + средние задержки
--    Гранулярность: аэропорт × час
--    Используется в дашборде: гистограмма задержек, карта аэропортов
-- ============================================================

CREATE TABLE dm.dep_arr_report AS

WITH flight_data AS (
    -- Базовая выборка выполненных рейсов с timestamp-ами вылета и прилёта
    SELECT
        f.flight_rk,
        f.origin_code,
        f.dest_code,

        -- Timestamp вылета
        (
            f.flight_dt::date || ' ' ||
            SUBSTRING(f.actual_dep_tm, 1, 2) || ':' ||
            SUBSTRING(f.actual_dep_tm, 3, 2)
        )::timestamp                                        AS dep_timestamp,

        -- Timestamp прилёта
        (
            f.flight_dt::date || ' ' ||
            SUBSTRING(f.actual_arr_tm, 1, 2) || ':' ||
            SUBSTRING(f.actual_arr_tm, 3, 2)
        )::timestamp                                        AS arr_timestamp,

        f.dep_delay_min,
        f.arr_delay_min,
        f.cancelled_flg

    FROM dds.flights f
    WHERE f.cancelled_flg = 0
),

-- Агрегат по вылетам: сколько улетело и средняя задержка, в разрезе аэропорт × час
departures AS (
    SELECT
        fd.origin_code                                      AS airport_code,
        a.airport_nm                                        AS airport_name,
        a.city_nm                                           AS city,
        a.region_nm                                         AS region,
        a.country_iso_code                                  AS country,
        DATE_TRUNC('hour', fd.dep_timestamp)                AS hour_local,
        COUNT(*)                                            AS departure_count,
        AVG(fd.dep_delay_min)                               AS avg_dep_delay
    FROM flight_data fd
    LEFT JOIN dds.airport a ON fd.origin_code = a.airport_iata_code
    GROUP BY
        fd.origin_code, a.airport_nm, a.city_nm,
        a.region_nm, a.country_iso_code, hour_local
),

-- Агрегат по прилётам
arrivals AS (
    SELECT
        fd.dest_code                                        AS airport_code,
        a.airport_nm                                        AS airport_name,
        a.city_nm                                           AS city,
        a.region_nm                                         AS region,
        a.country_iso_code                                  AS country,
        DATE_TRUNC('hour', fd.arr_timestamp)                AS hour_local,
        COUNT(*)                                            AS arrival_count,
        AVG(fd.arr_delay_min)                               AS avg_arr_delay
    FROM flight_data fd
    LEFT JOIN dds.airport a ON fd.dest_code = a.airport_iata_code
    GROUP BY
        fd.dest_code, a.airport_nm, a.city_nm,
        a.region_nm, a.country_iso_code, hour_local
)

-- FULL OUTER JOIN: аэропорт в конкретный час может быть только вылетным или только прилётным
SELECT
    COALESCE(d.airport_code,    a.airport_code)             AS airport_code,
    COALESCE(d.airport_name,    a.airport_name)             AS airport_name,
    COALESCE(d.city,            a.city)                     AS city,
    COALESCE(d.country,         a.country)                  AS country,
    TO_CHAR(COALESCE(d.hour_local, a.hour_local),
            'YYYY-MM-DD')                                   AS hour_display,

    COALESCE(d.departure_count, 0)                          AS departure_count,
    COALESCE(a.arrival_count,   0)                          AS arrival_count,
    COALESCE(d.departure_count, 0)
        + COALESCE(a.arrival_count, 0)                      AS total_operations,

    d.avg_dep_delay,
    a.avg_arr_delay

FROM departures d
FULL OUTER JOIN arrivals a
    ON  d.airport_code = a.airport_code
    AND d.hour_local   = a.hour_local

ORDER BY total_operations DESC, hour_display, airport_code;


-- ============================================================
-- 4. dm.cancellation_report
--    Отчёт по погодным отменам с метеопараметрами
--    Гранулярность: аэропорт × час × погодные условия
--    Фильтр: только погодные отмены (cancellation_code = 'B')
--    Аэропорты: AVL, FAY, BIS, GFK
-- ============================================================

CREATE TABLE dm.cancellation_report AS

WITH cancelled_flights AS (
    -- Выборка только погодных отмен с расшифровкой кодов
    SELECT
        f.flight_rk,
        f.origin_code,
        f.dest_code,
        f.flight_dt::date                                   AS flight_date,

        SUBSTRING(f.actual_dep_tm, 1, 2) || ':' ||
        SUBSTRING(f.actual_dep_tm, 3, 2)                   AS dep_time,

        (
            f.flight_dt::date || ' ' ||
            SUBSTRING(f.actual_dep_tm, 1, 2) || ':' ||
            SUBSTRING(f.actual_dep_tm, 3, 2)
        )::timestamp                                        AS dep_timestamp,

        f.cancellation_code,
        f.weather_delay_min,

        -- Расшифровка кода отмены в человекочитаемый вид
        CASE f.cancellation_code
            WHEN 'A' THEN 'Airline/Carrier'
            WHEN 'B' THEN 'Weather'
            WHEN 'C' THEN 'National Air System'
            WHEN 'D' THEN 'Security'
            ELSE          'Other'
        END                                                 AS cancellation_reason

    FROM dds.flights f
    WHERE f.cancelled_flg      = 1
      AND f.cancellation_code  = 'B'   -- только погодные отмены
),

weather_analysis AS (
    -- Джойн с погодой по аэропорту + усечению часа (почасовая гранулярность METAR)
    SELECT
        cf.origin_code                                      AS airport_code,
        a.airport_nm                                        AS airport_name,
        a.city_nm                                           AS city,
        a.region_nm                                         AS region,
        cf.cancellation_reason,
        cf.flight_date,
        cf.dep_time,
        DATE_TRUNC('hour', cf.dep_timestamp)                AS hour_of_day,

        -- Метеопараметры из METAR-архива
        w.t                                                 AS temperature,
        w.p                                                 AS pressure,
        w.ff                                                AS wind_speed,
        w.ww                                                AS weather_phenomena,
        w.c                                                 AS cloud_cover,
        w.vv                                                AS visibility,

        COUNT(*)                                            AS cancellation_count,
        SUM(cf.weather_delay_min)                           AS total_weather_delay_min

    FROM cancelled_flights cf
    JOIN dds.airport  a ON cf.origin_code = a.airport_iata_code

    -- LEFT JOIN: не все отмены имеют совпадающую метеозапись — сохраняем все отмены
    LEFT JOIN dds.weather w
           ON  a.airport_rk = w.airport_rk
           -- Связываем рейс с погодой по ближайшему часу (гранулярность METAR = 1 час)
           AND DATE_TRUNC('hour', cf.dep_timestamp) = DATE_TRUNC('hour', w.local_time)

    GROUP BY
        cf.origin_code, a.airport_nm, a.city_nm, a.region_nm,
        cf.cancellation_reason, cf.flight_date, cf.dep_time, hour_of_day,
        w.t, w.p, w.ff, w.ww, w.c, w.vv
)

SELECT
    airport_code,
    airport_name,
    city,
    cancellation_reason,
    TO_CHAR(flight_date, 'YYYY-MM-DD')                      AS flight_date,
    dep_time,
    TO_CHAR(hour_of_day, 'HH24:MI')                         AS hour_of_day,
    temperature,
    pressure,
    wind_speed,
    weather_phenomena,
    cloud_cover,
    visibility,
    cancellation_count,
    total_weather_delay_min,

    -- Средняя задержка на один отменённый рейс
    -- NULLIF защищает от деления на 0, если cancellation_count = 0
    total_weather_delay_min / NULLIF(cancellation_count, 0) AS avg_delay_per_flight

FROM weather_analysis
WHERE airport_code IN ('AVL', 'FAY', 'BIS', 'GFK')

ORDER BY cancellation_count DESC, flight_date, hour_of_day;
