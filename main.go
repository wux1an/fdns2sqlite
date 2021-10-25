package main

import (
	"encoding/json"
	"fmt"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Record of fdns
type Record struct {
	TimestampString string    `json:"timestamp" gorm:"-"`
	Timestamp       time.Time `gorm:"timestamp"`
	Name            string    `json:"name" gorm:"name"`
	Type            string    `json:"type" gorm:"type"`
	Value           string    `json:"value" gorm:"value"`
}

var (
	input  string
	output string
)

func main() {
	// parse arg
	if len(os.Args) < 2 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		fmt.Printf("Convert fdns.json to sqlite format. Large files will run slowly.\n"+
			"Usage: %s fdns.json\n", filepath.Base(os.Args[0]))
		return
	}

	input = os.Args[1]
	output = strings.TrimSuffix(filepath.Base(input), path.Ext(input))

	for i := 0; ; i++ {
		var name = output
		if i != 0 {
			name += "-" + strconv.Itoa(i)
		}
		name += ".sqlite3"

		if _, err := os.Open(name); os.IsNotExist(err) {
			output = name
			break
		}
	}

	// read file
	data, err := os.ReadFile(input)
	if err != nil {
		fmt.Println(err)
		return
	}

	// init database
	db, err := gorm.Open(sqlite.Open(output), &gorm.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = db.AutoMigrate(&Record{})

	var (
		lines       = strings.Split(strings.TrimSpace(string(data)), "\n")
		batchSize   = 100
		batchNum    = len(lines) / batchSize
		batchRemain = len(lines) % batchSize
	)

	for i := 0; i < batchNum; i++ {
		batch := make([]Record, batchSize)
		for j := 0; j < len(batch); j++ {
			var record Record
			err := json.Unmarshal([]byte(lines[i*batchSize+j]), &record)
			if err != nil {
				continue
			}
			batch[j] = record
		}
		db.CreateInBatches(batch, len(batch))
	}

	batch := make([]Record, batchRemain)
	for i := 0; i < len(batch); i++ {
		var record Record
		err := json.Unmarshal([]byte(lines[batchSize*batchNum+i]), &record)
		if err != nil {
			continue
		}
		batch[i] = record
	}
	db.CreateInBatches(batch, len(batch))

	absOutput, _ := filepath.Abs(output)
	fmt.Printf("Saved to \"%s\".", absOutput)
}
