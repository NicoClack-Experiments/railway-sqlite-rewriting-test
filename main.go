package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

func applyPragmas(db *sql.DB, wal bool, pageSize int) {
	if pageSize > 0 {
		if _, err := db.Exec(fmt.Sprintf("PRAGMA page_size = %d", pageSize)); err != nil {
			log.Printf("Warning: Failed to set page_size: %v", err)
		}
	}
	if wal {
		if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
			log.Printf("Warning: Failed to set journal_mode = WAL: %v", err)
		}
	}
}

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

	maxRowsStr := os.Getenv("MAX_ROWS")
	var maxRows int
	if maxRowsStr != "" {
		var err error
		maxRows, err = strconv.Atoi(maxRowsStr)
		if err != nil {
			log.Fatalf("Invalid MAX_ROWS value '%s': %v", maxRowsStr, err)
		}
	}

	useTempFS := os.Getenv("USE_TEMP_FS") == "true"
	var tempDir string
	if useTempFS {
		var err error
		// os.MkdirTemp uses the directory returned by os.TempDir().
		// On Linux (Railway), you can set TMPDIR=/dev/shm for a true in-memory filesystem.
		tempDir, err = os.MkdirTemp("", "sqlite-mem-")
		if err != nil {
			log.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tempDir)
	}

	sqliteWAL := os.Getenv("SQLITE_WAL") == "true"
	pageSizeStr := os.Getenv("SQLITE_PAGE_SIZE")
	var sqlitePageSize int
	if pageSizeStr != "" {
		var err error
		sqlitePageSize, err = strconv.Atoi(pageSizeStr)
		if err != nil {
			log.Fatalf("Invalid SQLITE_PAGE_SIZE value '%s': %v", pageSizeStr, err)
		}
	}

	// Set up graceful shutdown (SIGINT + SIGTERM on Unix; SIGINT on Windows).
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Open the database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	applyPragmas(db, sqliteWAL, sqlitePageSize)

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

	if useTempFS {
		db.Close()
	}

	fmt.Printf("Writer started.\nDatabase: %s\nDelay: %dms\nRow size: ~1KB\n", dbPath, delay)
	if useTempFS {
		fmt.Printf("Mode: Temp file rewrite\n")
	}
	if sqliteWAL {
		fmt.Printf("WAL: Enabled\n")
	}
	if sqlitePageSize > 0 {
		fmt.Printf("Page Size: %d\n", sqlitePageSize)
	}
	if maxRows > 0 {
		fmt.Printf("Max rows: %d\n", maxRows)
	}

	// Prepare 1KB payload
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}

	ticker := time.NewTicker(time.Duration(delay) * time.Millisecond)
	defer ticker.Stop()

	var count int64
	for {
		select {
		case <-quit:
			log.Println("Shutdown signal received. Closing database...")
			if !useTempFS {
				db.Close()
			}
			log.Println("Database closed. Exiting.")
			return

		case <-ticker.C:
			var targetDB *sql.DB
			var currentPath string
			if useTempFS {
				currentPath = filepath.Join(tempDir, filepath.Base(dbPath))
				if err := copyFile(dbPath, currentPath); err != nil {
					log.Printf("Error copying to temp: %v", err)
					continue
				}
				tdb, err := sql.Open("sqlite", currentPath)
				if err != nil {
					log.Printf("Error opening temp db: %v", err)
					continue
				}
				applyPragmas(tdb, sqliteWAL, sqlitePageSize)
				targetDB = tdb
			} else {
				targetDB = db
			}

			_, err := targetDB.Exec("INSERT INTO test_rows (payload) VALUES (?)", payload)
			if err != nil {
				log.Printf("Error inserting row: %v", err)
				if useTempFS {
					targetDB.Close()
				}
				continue
			}
			count++

			if maxRows > 0 {
				_, err = targetDB.Exec("DELETE FROM test_rows WHERE id NOT IN (SELECT id FROM test_rows ORDER BY id DESC LIMIT ?)", maxRows)
				if err != nil {
					log.Printf("Error pruning rows: %v", err)
				}
			}

			if useTempFS {
				targetDB.Close()
				// Copy back to a temporary file on the same volume as the target, then rename for atomicity
				diskTemp := dbPath + ".swap"
				if err := copyFile(currentPath, diskTemp); err != nil {
					log.Printf("Error copying back to disk: %v", err)
				} else if err := os.Rename(diskTemp, dbPath); err != nil {
					log.Printf("Error renaming temp to original: %v", err)
				}
			}

			if count%10 == 0 || delay >= 1000 {
				fmt.Printf("[%s] Inserted row #%d\n", time.Now().Format("15:04:05.000"), count)
			}
		}
	}
}
