-- Create a demo table for Gachi-muchi reference personas
CREATE TABLE IF NOT EXISTS gachi_muchi_personas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    description TEXT,
    strength_level INT,
    special_move VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert Gachi-muchi personas data
INSERT INTO gachi_muchi_personas (name, description, strength_level, special_move) VALUES
('Billy Herrington', 'The legendary figure known for his iconic status in Gachi-muchi lore.', 100, 'Anvil Drop'),
('Van Darkholme', 'The dungeon master and meme icon, known for his intense presence.', 95, 'Whip of Submission'),
('Ricardo Milos', 'A viral dancer with unmatched energy and meme status.', 90, 'Twerk of Power'),
('Mark Wolff', 'One of the strong and bold Gachi-muchi personas.', 85, 'Wolf Howl Slam'),
('Kazuhiko', 'An energetic character with a playful spirit, popular in certain circles.', 80, 'Smiling Slam');
