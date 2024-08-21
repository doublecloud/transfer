create table __test (
    id int not null,
    jb jsonb,
    v  int,
    primary key (id, jb)
);

insert into __test values (
    1,
    '{"fur":"chose","forgotten":"fully","copper":{"event":{"these":"build","pig":"funny","father":-1880059535,"such":-544181383.9750192,"character":{"free":"whole","occasionally":-609558599,"kitchen":{"actually":"reader","door":"leg","pocket":217064430,"basic":true,"compass":"gently","entirely":899627174.2691915},"sit":1265880636,"burn":503116911,"private":false},"soap":"elephant"},"house":-1506719842.3678143,"ball":true,"shoulder":"definition","street":true,"away":-378455498.2313981},"rays":"choice","avoid":true,"wonderful":"space"}',
    1
)
,
(
    2,
    '{"random":58,"random float":17.892,"bool":true,"date":"1986-09-17","regEx":"helloooooooooooooooooooooooooooooooooo world","enum":"online","firstname":"Candi","lastname":"Argus","city":"Boa Vista","country":"Slovenia","countryCode":"CC","email uses current data":"Candi.Argus@gmail.com","email from expression":"Candi.Argus@yopmail.com","array":["Sharlene","Katharina","Fidelia","Nita","Briney"],"array of objects":[{"index":0,"index start at 5":5},{"index":1,"index start at 5":6},{"index":2,"index start at 5":7}],"Demetris":{"age":89}}',
    2
)
,
(
    3,
    '{"also":true,"tiny":-1128401485.4129367,"key":false,"accept":1712681293.0974429,"cow":{"government":"there","victory":568454737.6474552,"inch":false,"picture":{"coast":171060425,"shells":"monkey","eager":true,"pour":1014611728,"unknown":{"master":true,"such":74968924.71636367,"plural":{"there":false,"dig":-414201758,"felt":false,"jack":false,"spin":-127200633,"system":true},"row":"vegetable","south":-1572826495.3433201,"joined":true},"upon":625580805.0322576},"period":-1837090778.0967252,"village":"sound"},"once":"laid"}',
    3
)
;
