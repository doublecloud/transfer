create extension if not exists hstore;
create extension if not exists ltree;
create extension if not exists citext;

create table if not exists  public.wtf_types
(
    __primary_key        serial primary key,

    t_hstore hstore,
    t_iner inet,
    t_cidr cidr,
    t_macaddr macaddr,
    -- macaddr8 not supported by postgresql 9.6 (which is in our recipes)
    -- ltree - should be in special table, i suppose
    t_citext citext,

    j json,
    jb jsonb
);

INSERT INTO public.wtf_types VALUES
(
    default,

    'a=>1,b=>2', -- t_hstore
    '192.168.1.5', -- t_iner
    '10.1/16', -- t_cidr
    '08:00:2b:01:02:03', -- t_macaddr
    'Tom', -- t_citext

    '{"k": "v", "ki": 42, "kf": 1.2, "kn": null, "ks": "Ho Ho Ho my name''s \"SANTA CLAWS\""}', -- j json
    '"\"String in quotes\""' -- jb jsonb
);

-- null case
INSERT INTO public.wtf_types (__primary_key) VALUES (default);
