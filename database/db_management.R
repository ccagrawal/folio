library(RSQLite)

CreateTables <- function(db.name = 'folio_data.db') {
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
  # Table types:
  # 1) Assets - show name, id, quantity 
  # 2) Values - daily values of assets
  # 3) Actions - anything that results in a cash change to the portfolio
  
  sql <- "CREATE TABLE IF NOT EXISTS Stocks(
            ID INTEGER PRIMARY KEY,
            Ticker TEXT UNIQUE,
            Name TEXT,
            Quantity NUMERIC,
            Volume NUMERIC,
            Notes TEXT
          );"
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS StockOptions(
            ID INTEGER PRIMARY KEY,
            Name TEXT UNIQUE,
            Underlying INTEGER,
            Type TEXT,
            Expiration INTEGER,
            Strike NUMERIC,
            Quantity NUMERIC,
            Volume NUMERIC,
            Notes TEXT,
            FOREIGN KEY (Underlying) REFERENCES Stocks(ID)
          );"
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Funds(
            ID INTEGER PRIMARY KEY,
            Quantity NUMERIC
          );"
  dbGetQuery(conn, sql)
  
  sql <- "INSERT ON CONFLICT IGNORE INTO Funds(Quantity)
          VALUES(0);"
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Values_Stock(
            ID INTEGER PRIMARY KEY,
            Stock INTEGER,
            Timestamp INTEGER,
            Value NUMERIC,
            Source TEXT,
            Notes TEXT,
            FOREIGN KEY (Stock) REFERENCES Stocks(ID)
          );"
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Values_StockOption(
            ID INTEGER PRIMARY KEY,
            Option INTEGER,
            Timestamp INTEGER,
            Value NUMERIC,
            Source TEXT,
            Notes TEXT,
            FOREIGN KEY (Option) REFERENCES StockOptions(ID)
          );"
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Values_Portfolio(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Investments NUMERIC,
            Cash NUMERIC,
            Portfolio NUMERIC,
            Source TEXT,
            Notes TEXT
          );"
  dbGetQuery(conn, sql)
  
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
  dbGetQuery(conn, sql)
  
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
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Fund(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Method TEXT,
            CashChange NUMERIC,
            Notes TEXT
          );"
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Interest(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Underlying INTEGER,
            Type TEXT,
            CashChange NUMERIC,
            Notes TEXT,
            FOREIGN KEY (Underlying) REFERENCES Stocks(ID)
          );"
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Fee(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Underlying INTEGER,
            Type TEXT,
            CashChange NUMERIC,
            Notes TEXT,
            FOREIGN KEY (Underlying) REFERENCES Stocks(ID)
          );"
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Dividend(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Underlying INTEGER,
            CashChange NUMERIC,
            Notes TEXT,
            FOREIGN KEY (Underlying) REFERENCES Stocks(ID)
          );"
  dbGetQuery(conn, sql)
  
  dbDisconnect(conn)
}

WriteTable <- function(table, data, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  dbWriteTable(conn, name = table, value = data, row.names = 0, append = TRUE)
  dbDisconnect(conn)
}

ReadTable <- function(table, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  results <- dbReadTable(conn, name = table)
  dbDisconnect(conn)
  return(results)
}

RemoveTable <- function(table, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  dbRemoveTable(conn, name = table)
  dbDisconnect(conn)
}

ListTables <- function(db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  print(dbListTables(conn))
  dbDisconnect(conn)
}

ListFields <- function(table, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  print(dbListFields(conn, name = table))
  dbDisconnect(conn)
}

RunQuery <- function(sql, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  result <- tryCatch(dbGetQuery(conn, sql), finally = dbDisconnect(conn))
  return(result)
}