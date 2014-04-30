package plugins

import (
	"fmt"
	"log"
	"time"
	"encoding/json"
	"github.com/moriyoshi/ik"
	"github.com/lib/pq"
	"database/sql"
)

type PgOutput struct {
	factory *PgOutputFactory
	logger *log.Logger
	db *sql.DB
	databaseUrl string
	tableName string
}

type PgOutputFactory struct {
}

func connectDb(databaseUrl string, tableName string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(`CREATE TABLE if not exists %s(
id bigserial,
timestamp timestamp,
tag varchar(255),
data json) `, tableName)
	_, err1 := db.Exec(sql)
	if err1 != nil {
		return db, err1
	}
	return db, err1
}

func (output *PgOutput) disconnectDb() error {
	err := output.db.Close()
	return err
}

func (outout *PgOutput) convertPgJson(data map[string]interface{}) (string, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (output *PgOutput) formatTime(timestamp uint64) string {
	timestamp_ := time.Unix(int64(timestamp), 0)
	return timestamp_.Format(time.RFC3339)
}

func (output *PgOutput) Emit(recordSets []ik.FluentRecordSet) error {
	logger := output.logger
	db := output.db
	tableName := output.tableName

	err := db.Ping()
	if err != nil {
		logger.Fatal(err)
		output.disconnectDb()
		db, err := connectDb(output.databaseUrl, output.tableName)
		if err != nil {
			return err
		}
		output.db = db
	}

	txn, err2 := db.Begin()
	if err2 != nil {
		log.Fatal(err2)
	}
	stmt, err3 := txn.Prepare(pq.CopyIn(tableName, "tag", "timestamp", "data"))
	if err3 != nil {
		log.Fatal(err3)
	}
	for _, recordSet := range recordSets {
		for _, record := range recordSet.Records {
			jsonStr, err4 := output.convertPgJson(record.Data)
			if err4 != nil {
				return err4
			}
			_, err = stmt.Exec(recordSet.Tag, output.formatTime(record.Timestamp), jsonStr)
			if err != nil {
				log.Fatal(err)
				txn.Rollback()
				return nil
			}
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

func (output *PgOutput) Factory() ik.Plugin {
	return output.factory
}

func (output *PgOutput) Run() error {
	time.Sleep(1000000000)
	return ik.Continue
}

func (output *PgOutput) Shutdown() error {
	err := output.disconnectDb()
	if err != nil {
		output.logger.Fatal(err)
	}
	return nil
}

func (output *PgOutput) Dispose () error {
	return output.Shutdown()
}


func newPgOutput(factory *PgOutputFactory, databaseUrl string, tableName string, logger *log.Logger) (*PgOutput, error) {
	logger.Print("PG Connect %s", databaseUrl)
	db, err := connectDb(databaseUrl, tableName)
	if err != nil {
		logger.Print("Error")
		logger.Fatal(err)
		return nil, err
	}
	return &PgOutput{
		factory: factory,
		logger: logger,
		db: db,
		tableName: tableName,
	}, nil
}

func (factory *PgOutputFactory) Name() string {
	return "pg"
}

func (factory *PgOutputFactory) BindScorekeeper(scorekeeper *ik.Scorekeeper) {
}


func (factory *PgOutputFactory) New(engine ik.Engine, config *ik.ConfigElement) (ik.Output, error) {
	databaseUrl := config.Attrs["db_url"]
	tableName := config.Attrs["table_name"]
	return newPgOutput(factory, databaseUrl, tableName,  engine.Logger())
}

var _ = AddPlugin(&PgOutputFactory{})
