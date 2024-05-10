package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

type User struct{
	Id int
	Name string
}

func main() {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", "localhost", 5432, "postgres", "Abdu0811", "project")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fmt.Println("error")
		log.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		_, err := db.ExecContext(ctx, "INSERT INTO large_dataset (name) VALUES ($1)", "Abduazim")
		if err != nil {
			return fmt.Errorf("insert error: %w", err)
		}
		fmt.Println("Insert completed")
		return nil
	})

	g.Go(func() error {
		rows, err := db.QueryContext(ctx, "SELECT * FROM large_dataset LIMIT 2")
		if err != nil {
			return fmt.Errorf("select error: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var us User
			if err := rows.Scan(&us.Id,&us.Name); err != nil {
				return fmt.Errorf("scan error: %w", err)
			}
			fmt.Println(us.Id,"-",us.Name)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("rows iteration error: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		_, err := db.ExecContext(ctx, "UPDATE large_dataset SET name = $1 WHERE name = $2", "Abduazim", "Jasur")
		if err != nil {
			return fmt.Errorf("update error: %w", err)
		}
		fmt.Println("Update completed")
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("operation failed: %v", err)
	} else {
		fmt.Println("All operations completed without error.")
	}
}
