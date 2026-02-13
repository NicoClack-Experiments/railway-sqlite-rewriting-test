package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := os.Getenv("DATABASE_PATH")
	if dbPath == "" {
		log.Fatal("DATABASE_PATH environment variable is required (e.g., ./test.db)")
	}

	delayStr := os.Getenv("DELAY")
	if delayStr == "" {
		log.Fatal("DELAY environment variable is required (in milliseconds)")
	}

	delay, err := strconv.Atoi(delayStr)
	if err != nil {
		log.Fatalf("Invalid DELAY value '%s': %v", delayStr, err)
	}

	// Open the database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Ensure the table exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_rows (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			payload BLOB,
			timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	fmt.Printf("Writer started.\nDatabase: %s\nDelay: %dms\nRow size: ~1KB\n", dbPath, delay)

	// Prepare 1KB payload
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}

	ticker := time.NewTicker(time.Duration(delay) * time.Millisecond)
	defer ticker.Stop()

	var count int64
	for range ticker.C {
		_, err := db.Exec("INSERT INTO test_rows (payload) VALUES (?)", payload)
		if err != nil {
			log.Printf("Error inserting row: %v", err)
			continue
		}
		count++
		if count%10 == 0 || delay >= 1000 {
			fmt.Printf("[%s] Inserted row #%d\n", time.Now().Format("15:04:05.000"), count)
		}
	}
}
