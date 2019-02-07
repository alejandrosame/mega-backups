package main

import (
    "database/sql"
    "flag"
    "fmt"
    "log"
    "os"

    "github.com/JamesStewy/go-mysqldump"
    _ "github.com/go-sql-driver/mysql"
)

func main() {

    user := flag.String("user", "web", "MySQL database user")
    password := flag.String("pass", "123456", "MySQL database password")
    dbname := flag.String("dbname", "gcp_mt_pairs", "MySQL database name")
    dumpDir := flag.String("dump", "./tmp", "Location of database dumps before upload")
    flag.Parse()

    infoLog := log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime)
    errorLog := log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile)

    dsn := fmt.Sprintf("%s:%s@/%s?parseTime=true", *user, *password, *dbname)
    dumpFilenameFormat := fmt.Sprintf("%s-20060102T150405", *dbname)

    db, err := openDB(dsn)
    if err != nil {
        errorLog.Fatal("Error opening database:", err)
    }
    defer db.Close()

    // Register database with mysqldump
    dumper, err := mysqldump.Register(db, *dumpDir, dumpFilenameFormat)
    if err != nil {
        errorLog.Fatal("Error registering databse:", err)
        return
    }
    defer dumper.Close()

    // Dump database to file
    resultFilename, err := dumper.Dump()
    if err != nil {
        fmt.Println("Error dumping:", err)
        return
    }
    infoLog.Println(fmt.Sprintf("File is saved to %s", resultFilename))
}

func openDB(dsn string) (*sql.DB, error) {
    db, err := sql.Open("mysql", dsn)
    if err != nil {
        return nil, err
    }
    if err = db.Ping(); err != nil {
        return nil, err
    }
    return db, nil
}