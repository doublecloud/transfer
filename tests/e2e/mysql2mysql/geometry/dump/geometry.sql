create table geo_test (
    id integer not null auto_increment primary key,
    g     geometry /*!80003 SRID 4326 */,
    p     point /*!80003 SRID 4326 */,
    l     linestring /*!80003 SRID 4326 */,
    poly  polygon /*!80003 SRID 4326 */,
    mp    multipoint /*!80003 SRID 4326 */,
    ml    multilinestring /*!80003 SRID 4326 */,
    mpoly multipolygon /*!80003 SRID 4326 */,
    gs    geometrycollection /*!80003 SRID 4326 */
);

insert into geo_test(g, p, l, poly, mp, ml, mpoly, gs) values
(
    ST_GeomFromText('LINESTRING(15 15, 20 20)', 4326),
    ST_GeomFromText('POINT(15 20)', 4326),
    ST_GeomFromText('LINESTRING(0 0, 10 10, 20 25, 50 60)', 4326),
    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7, 5 7, 5 5))', 4326),
    ST_GeomFromText('MULTIPOINT(0 0, 20 20, 60 60)', 4326),
    ST_GeomFromText('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))', 4326),
    ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),((5 5, 7 5, 7 7, 5 7, 5 5)))', 4326),
    ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))', 4326)
);
