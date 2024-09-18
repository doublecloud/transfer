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
