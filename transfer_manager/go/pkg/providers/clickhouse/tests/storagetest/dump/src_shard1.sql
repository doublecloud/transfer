CREATE DATABASE mtmobproxy;

create table mtmobproxy.sample_table
(
    `date` Date,
    `value` String
)
ENGINE = MergeTree
Partition By toMonday(date)
ORDER BY date;

insert into mtmobproxy.sample_table
(`date`, `value`)
values
('2012-01-01', 'shard1-test1'), ('2012-01-01', 'shard1-test2'), ('2012-01-02', 'shard1-test3'), ('2012-01-02', 'shard1-test4'), ('2012-01-03', 'shard1-test5')
;
