package main

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shopspring/decimal"
)

func main() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"suvrsz7m7k.us-west-2.aws.clickhouse.cloud:9440"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "<pwd>",
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "an-example-go-client", Version: "0.1"},
			},
		},
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	err = conn.Ping(context.Background())

	if err != nil {
		log.Fatalf("cannot ping: %v", err)
	}

	file, err := os.Open("GO_test_5m.csv")
	if err != nil {
		log.Fatalf("failed to open CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.ReuseRecord = true
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("failed to read CSV: %v", err)
	}

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO transactions")
	if err != nil {
		log.Fatalf("failed to prepare batch: %v", err)
	}

	startIdx := 0
	if records[0][0] == "transaction_id" {
		startIdx = 1
	}

	for i := startIdx; i < len(records); i++ {
		r := records[i]

		price, _ := decimal.NewFromString(r[8])
		quantity, _ := strconv.Atoi(r[9])
		totalPrice, _ := decimal.NewFromString(r[10])
		stockQty, _ := strconv.Atoi(r[11])
		transactionDate, _ := time.Parse("2006-01-02", r[1])
		addedDate, _ := time.Parse("2006-01-02", r[12])

		err = batch.Append(
			r[0],             // transaction_id
			transactionDate,  // transaction_date
			r[2],             // user_id
			r[3],             // country
			r[4],             // region
			r[5],             // product_id
			r[6],             // product_name
			r[7],             // category
			price,            // price
			uint16(quantity), // quantity
			totalPrice,       // total_price
			uint16(stockQty), // stock_quantity
			addedDate,        // added_date
		)
		if err != nil {
			log.Fatalf("failed to append record %d: %v", i, err)
		}

		if (i+1)%10000 == 0 {
			fmt.Printf("Committing batch at row %d\n", i+1)
			if err := batch.Send(); err != nil {
				log.Fatalf("failed to send batch: %v", err)
			}
			batch, _ = conn.PrepareBatch(context.Background(), "INSERT INTO transactions")
		}
	}

	if err := batch.Send(); err != nil {
		log.Fatalf("failed to send final batch: %v", err)
	}

	fmt.Println("✅ Import completed successfully.")
}
