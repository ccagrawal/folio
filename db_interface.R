library(RSQLite)

create.tables <- function(db.name = 'folio_data.db') {
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Stock(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            FOREIGN KEY(Stock) REFERENCES Stocks(ID),
            Price NUMERIC,
            Quantity NUMERIC,
            Fees NUMERIC,
            CashChange NUMERIC,
            Purpose TEXT,
            Notes TEXT
          );"
  run.query(sql, db.name)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Option(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            FOREIGN KEY(Option) REFERENCES Options(ID)
            Price NUMERIC,
            Quantity NUMERIC,
            Fees NUMERIC,
            CashChange NUMERIC,
            Purpose TEXT,
            Notes TEXT
          );"
  run.query(sql, db.name)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Fund(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Method TEXT,
            CashChange NUMERIC,
            Notes TEXT
          );"
  run.query(sql, db.name)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Interest(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            FOREIGN KEY(Underlying) REFERENCES Stocks(ID),
            Type TEXT,
            CashChange NUMERIC,
            Notes TEXT
          );"
  run.query(sql, db.name)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Fee(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            FOREIGN KEY(Underlying) REFERENCES Stocks(ID),
            Type TEXT,
            CashChange NUMERIC,
            Notes TEXT
          );"
  run.query(sql, db.name)
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Dividend(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            FOREIGN KEY(Underlying) REFERENCES Stocks(ID),
            CashChange NUMERIC,
            Notes TEXT
          );"
  run.query(sql, db.name)
  
  sql <- "CREATE TABLE IF NOT EXISTS Stocks(
            ID INTEGER PRIMARY KEY,
            Ticker TEXT,
            Name TEXT,
            Quantity NUMERIC,
            Volume NUMERIC,
            Notes TEXT
          );"
  run.query(sql, db.name)
  
  sql <- "CREATE TABLE IF NOT EXISTS Options(
            ID INTEGER PRIMARY KEY,
            Name TEXT,
            FOREIGN KEY(Underlying) REFERENCES Stocks(ID),
            Type TEXT,
            Expiration INTEGER,
            Strike NUMERIC,
            Quantity NUMERIC,
            Volume NUMERIC,
            Notes TEXT
          );"
  run.query(sql, db.name)
  
  sql <- "CREATE TABLE IF NOT EXISTS Funds(
            ID INTEGER PRIMARY KEY,
            Name TEXT,
            Quantity NUMERIC,
            Notes TEXT
          );"
  run.query(sql, db.name)
  
}

write.table <- function(table, data, db.name = 'folio_data.db') {
  connect <- dbConnect(drv = SQLite(), dbname = db.name)
  dbWriteTable(connect, name = table, value = data, row.names = 0, append = TRUE)
  closeConnection(connect)
}

read.table <- function(table, db.name = 'folio_data.db') {
  connect <- dbConnect(drv = SQLite(), dbname = db.name)
  results <- dbReadTable(connect, name = table)
  closeConnection(connect)
  return(results)
}

remove.table <- function(table, db.name = 'folio_data.db') {
  connect <- dbConnect(drv = SQLite(), dbname = db.name)
  dbRemoveTable(connect, name = table)
  closeConnection(connect)
}

list.tables <- function(db.name = 'folio_data.db') {
  connect <- dbConnect(drv = SQLite(), dbname = db.name)
  print(dbListTables(connect))
  closeConnection(connect)
}

list.fields <- function(table, db.name = 'folio_data.db') {
  connect <- dbConnect(drv = SQLite(), dbname = db.name)
  print(dbListFields(connect, name = table))
  closeConnection(connect)
}

run.query <- function(sql, db.name = 'folio_data.db') {
  connect <- dbConnect(drv = SQLite(), dbname = db.name)
  result <- tryCatch(dbGetQuery(connect, sql), finally = closeConnection(connect))
  return(result)
}

close.connection <- function(connect) {
  sqliteCloseConnection(connect)
  sqliteCloseDriver(SQLite())
}