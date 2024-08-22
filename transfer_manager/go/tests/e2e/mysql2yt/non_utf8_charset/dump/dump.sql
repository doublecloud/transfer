CREATE TABLE `kek` (
    id          int PRIMARY KEY,
    col_char    char(100),
    col_varchar varchar(100),
    col_text    text
) ENGINE = InnoDB DEFAULT CHARSET = cp1251;

insert into `kek` values
    (1, 'Cъешь ещё этих', ' мягких французских булок,', ' да выпей чаю.'),
    (2, 'Быстрая коричневая', ' лиса перепрыгивает', ' ленивую собаку.');
