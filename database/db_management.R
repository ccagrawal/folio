library(RSQLite)
db.name <- './database/folio_data.db'

CreateTables <- function() {
  
  # Table types:
  # 1) Assets - show name, id, quantity 
  # 2) Values - daily values of assets
  # 3) Actions - anything that results in a cash change to the portfolio
  
  sql <- "CREATE TABLE IF NOT EXISTS Stocks(
            ID INTEGER PRIMARY KEY,
            Ticker TEXT UNIQUE,
            Name TEXT,
            Price NUMERIC,
            Quantity NUMERIC,
            Volume NUMERIC,
            Notes TEXT
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS StockOptions(
            ID INTEGER PRIMARY KEY,
            Name TEXT UNIQUE,
            Underlying INTEGER,
            Type TEXT,
            Expiration INTEGER,
            Strike NUMERIC,
            Price NUMERIC,
            Quantity NUMERIC,
            Volume NUMERIC,
            Notes TEXT,
            FOREIGN KEY (Underlying) REFERENCES Stocks(ID)
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Funds(
            ID INTEGER PRIMARY KEY,
            Quantity NUMERIC
          );"
  RunQuery(sql)
  
  sql <- "INSERT OR IGNORE INTO Funds(ID, Quantity)
          VALUES(1, 0);"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Values_Stock(
            ID INTEGER PRIMARY KEY,
            Stock INTEGER,
            Timestamp INTEGER,
            Value NUMERIC,
            Source TEXT,
            Notes TEXT,
            FOREIGN KEY (Stock) REFERENCES Stocks(ID)
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Values_StockOption(
            ID INTEGER PRIMARY KEY,
            Option INTEGER,
            Timestamp INTEGER,
            Value NUMERIC,
            Source TEXT,
            Notes TEXT,
            FOREIGN KEY (Option) REFERENCES StockOptions(ID)
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Values_Portfolio(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Investments NUMERIC,
            Cash NUMERIC,
            Portfolio NUMERIC,
            Source TEXT,
            Notes TEXT
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Stock(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Stock INTEGER,
            Price NUMERIC,
            Quantity NUMERIC,
            Fees NUMERIC,
            CashChange NUMERIC,
            Purpose TEXT,
            Notes TEXT,
            FOREIGN KEY (Stock) REFERENCES Stocks(ID)
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_StockOption(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Option INTEGER,
            Price NUMERIC,
            Quantity NUMERIC,
            Fees NUMERIC,
            CashChange NUMERIC,
            Purpose TEXT,
            Notes TEXT,
            FOREIGN KEY (Option) REFERENCES StockOptions(ID)
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Fund(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Method TEXT,
            CashChange NUMERIC,
            Notes TEXT
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Interest(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Underlying INTEGER,
            Type TEXT,
            CashChange NUMERIC,
            Notes TEXT,
            FOREIGN KEY (Underlying) REFERENCES Stocks(ID)
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Fee(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Underlying INTEGER,
            Type TEXT,
            CashChange NUMERIC,
            Notes TEXT,
            FOREIGN KEY (Underlying) REFERENCES Stocks(ID)
          );"
  RunQuery(sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Dividend(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Underlying INTEGER,
            CashChange NUMERIC,
            Notes TEXT,
            FOREIGN KEY (Underlying) REFERENCES Stocks(ID)
          );"
  RunQuery(sql)
}

WriteTable <- function(table, data) {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  dbWriteTable(conn, name = table, value = data, row.names = 0, append = TRUE)
  dbDisconnect(conn)
}

ReadTable <- function(table) {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  results <- dbReadTable(conn, name = table)
  dbDisconnect(conn)
  return(results)
}

RemoveTable <- function(table) {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  dbRemoveTable(conn, name = table)
  dbDisconnect(conn)
}

ListTables <- function() {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  print(dbListTables(conn))
  dbDisconnect(conn)
}

ListFields <- function(table) {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  print(dbListFields(conn, name = table))
  dbDisconnect(conn)
}

RunQuery <- function(sql) {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  result <- tryCatch(dbGetQuery(conn, sql), finally = dbDisconnect(conn))
  return(result)
}