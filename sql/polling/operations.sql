
-- clean up
TRUNCATE TABLE public.polling_poc_event_queue RESTART IDENTITY;
TRUNCATE TABLE public.polling_poc_raw_data RESTART IDENTITY CASCADE;

-- single insert
INSERT INTO public.polling_poc_raw_data (
    name,
    processing_timestamp,
    value,
    origin_time_stamp,
    data_source_id
)
VALUES (
    'temperature_sensor_1',
    now(),
    42.5,
    now(),
    1
);

-- batch insert
INSERT INTO public.polling_poc_raw_data (
    name,
    processing_timestamp,
    value,
    origin_time_stamp,
    data_source_id
)
SELECT
    'temperature_sensor_' || i,
    now(),
    i::double precision,
    now(),
    1
FROM generate_series(1, 10) AS i;

-- view data
select * from polling_poc_raw_data;
select * from polling_poc_event_queue;
