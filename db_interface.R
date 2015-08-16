library(RSQLite)

create.tables <- function(db.name = 'folio_data.db') {
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
  sql <- "CREATE TABLE IF NOT EXISTS Stocks(
            ID INTEGER PRIMARY KEY,
            Ticker TEXT,
            Name TEXT,
            Quantity NUMERIC,
            Volume NUMERIC,
            Notes TEXT
          );"
  dbGetQuery(conn, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS Options_Stock(
            ID INTEGER PRIMARY KEY,
            Name TEXT,
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
            Name TEXT,
            Quantity NUMERIC,
            Notes TEXT
          );"
  dbGetQuery(conn, sql)
  
  sql <- "INSERT INTO Funds(Name, Quantity)
          VALUES(\"Cash\", 0);"
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
  
  sql <- "CREATE TABLE IF NOT EXISTS Actions_Option(
            ID INTEGER PRIMARY KEY,
            Timestamp INTEGER,
            Option INTEGER,
            Price NUMERIC,
            Quantity NUMERIC,
            Fees NUMERIC,
            CashChange NUMERIC,
            Purpose TEXT,
            Notes TEXT,
            FOREIGN KEY (Option) REFERENCES Options(ID)
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

add.action.stock <- function(timestamp, ticker, price, quantity, fees, cash.change, purpose, notes, db.name = 'folio_data.db') {
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
  # Check if stock exists in stocks table
  sql <- paste0(
          "SELECT * FROM Stocks
           WHERE Ticker = \"", ticker, "\";"
         )
  stock.row <- dbGetQuery(conn, sql)
  
  # If stock doesn't exist, add it; else, update stock's quantity and volume
  if (nrow(stock.row) == 0) {
    sql <- paste0(
            "INSERT INTO Stocks(Ticker, Quantity, Volume)
             VALUES(
                \"", ticker, "\", ", 
                quantity, ", ", 
                quantity, 
            ");"
           )
    dbGetQuery(conn, sql)
    
    sql <- "SELECT last_insert_rowid();"
    stock.id <- dbGetQuery(conn, sql)[1, 1]
  } else {
    sql <- paste0(
            "UPDATE Stocks
             SET Quantity = Quantity + ", quantity,
              ", Volume = Volume + ", abs(quantity),
           " WHERE Ticker = \"", ticker, "\";"
           )
    dbGetQuery(conn, sql)
    
    stock.id <- stock.row[1, 'ID']
  }
  
  # Add transaction into actions table
  sql <- paste0(
          "INSERT INTO Actions_Stock(Timestamp, Stock, Price, Quantity, Fees, CashChange)
           VALUES(",
              timestamp, ", ",
              stock.id, ", ",
              price, ", ",
              quantity, ", ",
              fees, ", ",
              cash.change,
          ");"
         )
  dbGetQuery(conn, sql)
  
  sql <- "SELECT last_insert_rowid();"
  action.id <- dbGetQuery(conn, sql)[1, 1]
  
  # Add purpose and notes if they exist
  if (!missing(purpose)) {
    sql <- paste0(
            "UPDATE Actions_Stock
             SET Purpose = \"", purpose, "\",
             WHERE ID = ", action.id, ";"
           )
    dbGetQuery(conn, sql)
  }
  
  if (!missing(notes)) {
    sql <- paste0(
            "UPDATE Actions_Stock
             SET Notes = \"", notes, "\",
             WHERE ID = ", action.id, ";"
    )
    dbGetQuery(conn, sql)
  }
  
  # Update funds
  sql <- paste0(
          "UPDATE Funds
           SET Quantity = Quantity + ", cash.change,
         " WHERE Name = \"Cash\";"
         )
  dbGetQuery(conn, sql)
  
  dbDisconnect(conn)
}

write.table <- function(table, data, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  dbWriteTable(conn, name = table, value = data, row.names = 0, append = TRUE)
  dbDisconnect(conn)
}

read.table <- function(table, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  results <- dbReadTable(conn, name = table)
  dbDisconnect(conn)
  return(results)
}

remove.table <- function(table, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  dbRemoveTable(conn, name = table)
  dbDisconnect(conn)
}

list.tables <- function(db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  print(dbListTables(conn))
  dbDisconnect(conn)
}

list.fields <- function(table, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  print(dbListFields(conn, name = table))
  dbDisconnect(conn)
}

run.query <- function(sql, db.name = 'folio_data.db') {
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  result <- tryCatch(dbGetQuery(conn, sql), finally = dbDisconnect(conn))
  return(result)
}