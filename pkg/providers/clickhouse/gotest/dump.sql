CREATE DATABASE IF NOT EXISTS mtmobproxy;


-- MergeTree->MergeTree


CREATE TABLE mtmobproxy.logs_weekly__mt_mt
(
    `ServerName` String,
    `DC` FixedString(3),
    `RequestDate` Date,
    `RequestDateTime` DateTime,
    `VirtualHost` String,
    `Path` String,
    `BasePath` String DEFAULT 'misc',
    `Code` UInt16,
    `RequestLengthBytes` UInt32,
    `FullRequestTime` UInt16,
    `UpstreamResponseTime` UInt16,
    `IsUpstreamRequest` Enum8('false' = 0, 'true' = 1),
    `SSLHanshakeTime` UInt16,
    `IsKeepalive` Enum8('false' = 0, 'true' = 1),
    `StringHash` UInt32,
    `HTTPMethod` String
)
    ENGINE = MergeTree()
    PARTITION BY toMonday(RequestDate)
    ORDER BY (BasePath, Code, ServerName, StringHash)
    SAMPLE BY StringHash
    SETTINGS index_granularity = 8192;

INSERT INTO mtmobproxy.logs_weekly__mt_mt
(`ServerName`, `DC`, `RequestDate`, `RequestDateTime`, `VirtualHost`,     `Path`, `BasePath`, `Code`, `RequestLengthBytes`, `FullRequestTime`, `UpstreamResponseTime`, `IsUpstreamRequest`, `SSLHanshakeTime`, `IsKeepalive`, `StringHash`, `HTTPMethod`)
VALUES
    ('my-server', 'iva',  1546300800,   1546300800,        'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod')
;


-- NotUpdateableReplacingMergeTree->MergeTree


CREATE TABLE mtmobproxy.logs_weekly__nurmt_mt
(
    `ServerName` String,
    `DC` FixedString(3),
    `RequestDate` Date,
    `RequestDateTime` DateTime,
    `VirtualHost` String,
    `Path` String,
    `BasePath` String DEFAULT 'misc',
    `Code` UInt16,
    `RequestLengthBytes` UInt32,
    `FullRequestTime` UInt16,
    `UpstreamResponseTime` UInt16,
    `IsUpstreamRequest` Enum8('false' = 0, 'true' = 1),
    `SSLHanshakeTime` UInt16,
    `IsKeepalive` Enum8('false' = 0, 'true' = 1),
    `StringHash` UInt32,
    `HTTPMethod` String,
    `commit_time` UInt64,
    `delete_time` UInt64
)
    ENGINE = ReplacingMergeTree(commit_time)
    PARTITION BY toMonday(RequestDate)
    ORDER BY (BasePath, Code, ServerName, StringHash)
    SAMPLE BY StringHash
    SETTINGS index_granularity = 8192;

-- insert 3 rows (my-server2/my-server3/my-server4)
INSERT INTO mtmobproxy.logs_weekly__nurmt_mt
(`ServerName`, `DC`, `RequestDate`, `RequestDateTime`, `VirtualHost`,     `Path`, `BasePath`, `Code`, `RequestLengthBytes`, `FullRequestTime`, `UpstreamResponseTime`, `IsUpstreamRequest`, `SSLHanshakeTime`, `IsKeepalive`, `StringHash`, `HTTPMethod`, `commit_time`, `delete_time`)
VALUES
    ('my-server2', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,             0            ),('my-server3', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,             0),('my-server4', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,             0)
;

-- update 1 row (my-server2)
INSERT INTO mtmobproxy.logs_weekly__nurmt_mt
(`ServerName`, `DC`, `RequestDate`, `RequestDateTime`, `VirtualHost`,     `Path`, `BasePath`, `Code`, `RequestLengthBytes`, `FullRequestTime`, `UpstreamResponseTime`, `IsUpstreamRequest`, `SSLHanshakeTime`, `IsKeepalive`, `StringHash`, `HTTPMethod`, `commit_time`, `delete_time`)
VALUES
    ('my-server2', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        9,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 2,             0            )
;


-- NotUpdateableReplacingMergeTree->NotUpdateableReplacingMergeTree


CREATE TABLE mtmobproxy.logs_weekly__nurmt_nurmt
(
    `ServerName` String,
    `DC` FixedString(3),
    `RequestDate` Date,
    `RequestDateTime` DateTime,
    `VirtualHost` String,
    `Path` String,
    `BasePath` String DEFAULT 'misc',
    `Code` UInt16,
    `RequestLengthBytes` UInt32,
    `FullRequestTime` UInt16,
    `UpstreamResponseTime` UInt16,
    `IsUpstreamRequest` Enum8('false' = 0, 'true' = 1),
    `SSLHanshakeTime` UInt16,
    `IsKeepalive` Enum8('false' = 0, 'true' = 1),
    `StringHash` UInt32,
    `HTTPMethod` String,
    `commit_time` UInt64,
    `delete_time` UInt64
)
    ENGINE = ReplacingMergeTree(commit_time)
    PARTITION BY toMonday(RequestDate)
    ORDER BY (BasePath, Code, ServerName, StringHash)
    SAMPLE BY StringHash
    SETTINGS index_granularity = 8192;

-- insert 3 rows (my-server2/my-server3/my-server4)
INSERT INTO mtmobproxy.logs_weekly__nurmt_nurmt
(`ServerName`, `DC`, `RequestDate`, `RequestDateTime`, `VirtualHost`,     `Path`, `BasePath`, `Code`, `RequestLengthBytes`, `FullRequestTime`, `UpstreamResponseTime`, `IsUpstreamRequest`, `SSLHanshakeTime`, `IsKeepalive`, `StringHash`, `HTTPMethod`, `commit_time`, `delete_time`)
VALUES
    ('my-server2', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,             0            ),('my-server3', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,             0),('my-server4', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,             0)
;

-- update 1 row (my-server2)
INSERT INTO mtmobproxy.logs_weekly__nurmt_nurmt
(`ServerName`, `DC`, `RequestDate`, `RequestDateTime`, `VirtualHost`,     `Path`, `BasePath`, `Code`, `RequestLengthBytes`, `FullRequestTime`, `UpstreamResponseTime`, `IsUpstreamRequest`, `SSLHanshakeTime`, `IsKeepalive`, `StringHash`, `HTTPMethod`, `commit_time`, `delete_time`)
VALUES
    ('my-server2', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        9,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 2,             0            )
;


-- UpdateableReplacingMergeTree->MergeTree


CREATE TABLE mtmobproxy.logs_weekly__urmt_mt
(
    `ServerName` String,
    `DC` FixedString(3),
    `RequestDate` Date,
    `RequestDateTime` DateTime,
    `VirtualHost` String,
    `Path` String,
    `BasePath` String DEFAULT 'misc',
    `Code` UInt16,
    `RequestLengthBytes` UInt32,
    `FullRequestTime` UInt16,
    `UpstreamResponseTime` UInt16,
    `IsUpstreamRequest` Enum8('false' = 0, 'true' = 1),
    `SSLHanshakeTime` UInt16,
    `IsKeepalive` Enum8('false' = 0, 'true' = 1),
    `StringHash` UInt32,
    `HTTPMethod` String,
    `__data_transfer_commit_time` UInt64,
    `__data_transfer_delete_time` UInt64
)
    ENGINE = ReplacingMergeTree(__data_transfer_commit_time)
    PARTITION BY toMonday(RequestDate)
    ORDER BY (BasePath, Code, ServerName, StringHash)
    SAMPLE BY StringHash
    SETTINGS index_granularity = 8192;

-- insert 4 rows (my-server2/my-server3/my-server4)
INSERT INTO mtmobproxy.logs_weekly__urmt_mt
(`ServerName`, `DC`, `RequestDate`, `RequestDateTime`, `VirtualHost`,     `Path`, `BasePath`, `Code`, `RequestLengthBytes`, `FullRequestTime`, `UpstreamResponseTime`, `IsUpstreamRequest`, `SSLHanshakeTime`, `IsKeepalive`, `StringHash`, `HTTPMethod`, `__data_transfer_commit_time`, `__data_transfer_delete_time`)
VALUES
    ('my-server2', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,                             0                            ),('my-server3', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,             0),('my-server4', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,             0),('my-server5', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        1,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 1,             0)
;

-- update 1 row (my-server2)
INSERT INTO mtmobproxy.logs_weekly__urmt_mt
(`ServerName`, `DC`, `RequestDate`, `RequestDateTime`, `VirtualHost`,     `Path`, `BasePath`, `Code`, `RequestLengthBytes`, `FullRequestTime`, `UpstreamResponseTime`, `IsUpstreamRequest`, `SSLHanshakeTime`, `IsKeepalive`, `StringHash`, `HTTPMethod`, `__data_transfer_commit_time`, `__data_transfer_delete_time`)
VALUES
    ('my-server2', 'iva',  1546300800,   1546300800,       'my-virtual-host', 'a/b',  'a',        9,      2,                    3,                 4,                      'true',              5,                 'false',       6,            'HTTPMethod', 2,                             0                            )
;



-- Empty table


CREATE TABLE mtmobproxy.empty
(
    `BasePath` String DEFAULT 'misc',
    `Code` UInt16
)
    ENGINE = MergeTree()
    ORDER BY (BasePath)
    SETTINGS index_granularity = 8192;
