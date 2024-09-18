CREATE DOMAIN public."currency" AS text
    COLLATE "default"
    CONSTRAINT currency_check CHECK (upper(VALUE) = VALUE AND length(VALUE) = 3);

CREATE TABLE public.udt
(
    id         INT             NOT NULL PRIMARY KEY,
    mycurrency public.currency NOT NULL
);
INSERT INTO public.udt(id, mycurrency) VALUES (1, 'RUB');
