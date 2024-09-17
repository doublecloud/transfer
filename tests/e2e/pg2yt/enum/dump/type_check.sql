create type dream_team as enum ('svemarch', 'timmyb32r', 'tserakhau', 'darkwingduck', 'vag-ekaterina', 'ovandriyanov', 'kry127', 'in-leskin', 'unikoid', 'daniilmarukh', 'scullyx13', 'sovictor', 'ejaku', 'abogutskiy');

create table __fullnames (
    usr dream_team primary key,
    badge_id int
);
create table __food_expenditure (
    id int not null primary key,
    usr dream_team REFERENCES __fullnames(usr),
    price int
);

insert into __fullnames values ('svemarch', 255), ('timmyb32r', 49892), ('tserakhau', 100500), ('darkwingduck', 9001),
                               ('vag-ekaterina', 1), ('ovandriyanov', 65535), ('kry127', 6134534)
;
insert into __food_expenditure values
        (1, 'timmyb32r', 525),
        (2, 'ovandriyanov', 315),
        (3, 'tserakhau', 345),
        (4, 'kry127', 260),
        (5, 'timmyb32r', 52),
        (6, 'tserakhau', 52),
        (7, 'darkwingduck', 430),
        (8, 'svemarch', 290),
        (9, 'kry127', 180)
;
