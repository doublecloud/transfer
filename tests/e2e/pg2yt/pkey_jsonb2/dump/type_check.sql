create table permalinks_setup
(
    key        jsonb  not null,
    subkey     jsonb  not null,
    params     jsonb  not null,
    version_id bigint not null,
    primary key (key, subkey)
);

insert into permalinks_setup values (
    '{"geoCampaignId": 1071350, "permalink": 1088939469, "platform": "direct"}',
    '{"group_name": "1088939469-uf", "rubric_id": 31166}',
    '{}',
    2513747237
)
;

create table permalinks_setup2
(
    key        jsonb  not null,
    subkey     jsonb  not null,
    params     jsonb  not null,
    version_id bigint not null,
    primary key (key, subkey)
);

insert into permalinks_setup2 values (
    '{"geoCampaignId": 1071350, "permalink": 1088939469, "platform": "direct"}',
    '{"group_name": "1088939469-uf", "rubric_id": 31166}',
    '{}',
    2513747237
)
;

create table done
(
    key        int,
    primary key (key)
);
