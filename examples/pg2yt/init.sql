-- Create a demo table for users
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
