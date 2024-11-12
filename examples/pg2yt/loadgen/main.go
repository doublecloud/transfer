package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

var db *sql.DB

// Perform a CREATE operation: Insert a new user
func createUser() {
	name := fmt.Sprintf("User%d", rand.Intn(1000))
	email := fmt.Sprintf("%s@example.com", name)
	_, err := db.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", name, email)
	if err != nil {
		log.Printf("Error creating user: %v", err)
		return
	}
	log.Printf("Created user: %s with email %s", name, email)
}

// Perform a READ operation: Select a random user
func readUser() {
	row := db.QueryRow("SELECT id, name, email FROM users ORDER BY RANDOM() LIMIT 1")
	var id int
	var name, email string
	err := row.Scan(&id, &name, &email)
	if err != nil {
		log.Printf("Error reading user: %v", err)
		return
	}
	log.Printf("Read user: ID=%d, Name=%s, Email=%s", id, name, email)
}

// Perform an UPDATE operation: Update a random user's email
func updateUser() {
	row := db.QueryRow("SELECT id FROM users ORDER BY RANDOM() LIMIT 1")
	var id int
	err := row.Scan(&id)
	if err != nil {
		log.Printf("Error selecting user for update: %v", err)
		return
	}
	newEmail := fmt.Sprintf("updated%d@example.com", rand.Intn(1000))
	_, err = db.Exec("UPDATE users SET email = $1 WHERE id = $2", newEmail, id)
	if err != nil {
		log.Printf("Error updating user: %v", err)
		return
	}
	log.Printf("Updated user ID=%d with new email %s", id, newEmail)
}

// Perform a DELETE operation: Delete a random user
func deleteUser() {
	row := db.QueryRow("SELECT id FROM users ORDER BY RANDOM() LIMIT 1")
	var id int
	err := row.Scan(&id)
	if err != nil {
		log.Printf("Error selecting user for delete: %v", err)
		return
	}
	_, err = db.Exec("DELETE FROM users WHERE id = $1", id)
	if err != nil {
		log.Printf("Error deleting user: %v", err)
		return
	}
	log.Printf("Deleted user with ID=%d", id)
}

// Randomly perform CRUD operations
func performCrudOperations() {
	operations := []func(){createUser, readUser, updateUser, deleteUser}

	for {
		operation := operations[rand.Intn(len(operations))]
		operation()

		// Sleep for a second between operations
		time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	// Database connection details
	dbHost := os.Getenv("HOST")
	dbUser := os.Getenv("USER")
	dbPassword := os.Getenv("PASSWORD")
	dbName := os.Getenv("DB")

	// Connect to the PostgreSQL database
	var err error
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", dbHost, dbUser, dbPassword, dbName)
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer db.Close()

	// Verify the connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error pinging the database: %s: %v", dsn, err)
	}

	log.Println("Connected to PostgreSQL database. Starting CRUD load generation...")

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Perform CRUD operations in a loop
	performCrudOperations()
}
