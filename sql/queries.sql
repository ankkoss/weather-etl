CREATE TABLE IF NOT EXISTS public.weather
(
    id integer NOT NULL DEFAULT nextval('weather_id_seq'::regclass),
    temperature double precision,
    windspeed double precision,
    "time" timestamp without time zone,
    city text COLLATE pg_catalog."default",
    created_at timestamp without time zone DEFAULT now(),
    CONSTRAINT weather_pkey PRIMARY KEY (id)
)

SELECT * FROM weather;

SELECT AVG(temperature) FROM weather;

SELECT MAX(temperature) FROM weather;

select id, temperature
from weather
where temperature = (select max(temperature) from weather);

select id, temperature
from weather 
order by temperature desc, id desc
limit 1;