CREATE TABLE public.__test (
    id INT PRIMARY KEY,
    valA TEXT
);

INSERT INTO public.__test (id,valA) VALUES (1,'blablabla');

--

CREATE SCHEMA public2;

CREATE TABLE public2.__test (
    id INT PRIMARY KEY,
    valB TEXT
);
