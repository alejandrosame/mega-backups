package main

import (
    "bytes"
    "compress/gzip"
    "database/sql"
    "flag"
    "fmt"
    "io/ioutil"
    "log"
    "os"

    "github.com/JamesStewy/go-mysqldump"
    _ "github.com/go-sql-driver/mysql"
    mega "github.com/t3rm1n4l/go-mega"
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

    compressedFilename := fmt.Sprintf("%s.gz", resultFilename)

    file, err := os.Open(resultFilename)
    if err != nil {
        fmt.Println("Error opening dump file:", err)
    }
    defer file.Close()

    content, err := ioutil.ReadAll(file)
    if err != nil {
        fmt.Println("Error reading dump file:", err)
    }

    var b bytes.Buffer
    w := gzip.NewWriter(&b)
    w.Write([]byte(content))
    w.Close() // You must close this first to flush the bytes to the buffer.
    err = ioutil.WriteFile(compressedFilename, b.Bytes(), 0666)
    if err != nil {
        fmt.Println("Error compressing dump file:", err)
    }

    infoLog.Println(fmt.Sprintf("Compressing file"))

    megaUser := os.Getenv("MEGA_USER")
    megaPass := os.Getenv("MEGA_PASSWD")

    m := mega.New()
    err = m.Login(megaUser, megaPass)
    if err != nil{
        errorLog.Fatal("Error login to MEGA:", err)
        return
    }

    infoLog.Println("Starting file upload")

    _, err = m.UploadFile(compressedFilename, m.FS.GetRoot(), "", nil)
    if err != nil{
        errorLog.Fatal("Error uploading backup file to MEGA:", err)
        return
    }

    infoLog.Println("File uploaded successfully")
    
    err = os.Remove(resultFilename)
    if err != nil{
        errorLog.Fatal("Error deleting local backup file:", err)
        return
    }

    err = os.Remove(compressedFilename)
    if err != nil{
        errorLog.Fatal("Error deleting local backup file:", err)
        return
    }

    infoLog.Println("Local files deleted")
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