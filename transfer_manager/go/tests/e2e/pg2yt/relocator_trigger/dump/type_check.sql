CREATE TABLE wild_pokemon (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    home TEXT DEFAULT 'Forest'
);
INSERT INTO wild_pokemon VALUES
    (1, 'Bulbasaur', 'Grass'),
    (2, 'Ivysaur', 'Grass'),
    (3, 'Venusaur', 'Grass');
INSERT INTO wild_pokemon VALUES
    (4, 'Charmander', 'Fire', 'Cave'),
    (5, 'Charmeleon', 'Fire', 'Mountain'),
    (6, 'Charizard', 'Fire', 'Volcano'),
    (7, 'Squirtle', 'Water', 'River'),
    (8, 'Wartortle', 'Water', 'Island'),
    (9, 'Blastoise', 'Water', 'Ocean');

CREATE TABLE captured_pokemon (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    home TEXT DEFAULT 'Pokeball'
);
INSERT INTO captured_pokemon VALUES
    (25, 'Pikachu', 'Electric');

CREATE FUNCTION store_captured() RETURNS trigger AS $$
    BEGIN
        IF NEW.home LIKE '%%ball' THEN
            INSERT INTO captured_pokemon VALUES (NEW.*);
            DELETE FROM wild_pokemon WHERE id = NEW.id;
            RETURN NULL;
        END IF;

        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER capture
    BEFORE UPDATE OF home ON wild_pokemon
    FOR EACH ROW EXECUTE PROCEDURE store_captured();
