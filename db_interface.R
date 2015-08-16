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
  
  sql <- "CREATE TABLE IF NOT EXISTS StockOptions(
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
            Quantity NUMERIC
          );"
  dbGetQuery(conn, sql)
  
  sql <- "INSERT INTO Funds(Quantity)
          VALUES(0);"
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

add.action.stock <- function(timestamp, ticker, price, quantity, fees, cash.change, purpose, notes, db.name = 'folio_data.db') {
  
  # Make timestamp an integer if it isn't
  timestamp <- as.numeric(timestamp)
  
  # If cash.change not supplied, just calculate it
  if (missing(cash.change)) {
    cash.change <- -1 * price * quantity - fees
  }
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
  # Check if stock exists in stocks table
  sql <- paste0(
          "SELECT * FROM Stocks
           WHERE Ticker = \"", ticker, "\";"
         )
  asset.info <- dbGetQuery(conn, sql)
  
  # If stock doesn't exist, add it; else, update stock's quantity and volume
  if (nrow(asset.info) == 0) {
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
    asset.id <- dbGetQuery(conn, sql)[1, 1]
  } else {
    asset.id <- asset.info[1, 'ID']
    
    sql <- paste0(
            "UPDATE Stocks
             SET Quantity = Quantity + ", quantity,
              ", Volume = Volume + ", abs(quantity),
           " WHERE ID = ", asset.id, ";"
           )
    dbGetQuery(conn, sql)
  }
  
  # Add transaction into actions table
  sql <- paste0(
          "INSERT INTO Actions_Stock(Timestamp, Stock, Price, Quantity, Fees, CashChange)
           VALUES(",
              timestamp, ", ",
              asset.id, ", ",
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
         " WHERE ID = 1;"
         )
  dbGetQuery(conn, sql)
  
  dbDisconnect(conn)
}

add.action.stock.option <- function(timestamp, underlying, type, expiration, strike, price, quantity, fees, cash.change, purpose, notes, db.name = 'folio_data.db') {
  
  # Make option name
  name <- paste0(
            underlying, 
            format(expiration, '%y%m%d'), 
            substr(type, 0, 1),
            gsub("\\.", "", sprintf("%09.03f", 120))
          )
  
  # Make timestamp an integer if it isn't
  timestamp <- as.numeric(timestamp)
  
  # If cash.change not supplied, just calculate it
  if (missing(cash.change)) {
    cash.change <- -1 * price * quantity - fees
  }
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
  # Check if underlying exists in underlying table
  sql <- paste0(
          "SELECT * FROM Stocks
           WHERE Ticker = \"", underlying, "\";"
         )
  underlying.info <- dbGetQuery(conn, sql)
  
  # If underlying doesn't exist, add it
  if (nrow(underlying.info) == 0) {
    sql <- paste0(
            "INSERT INTO Stocks(Ticker, Quantity, Volume)
             VALUES(\"", ticker, "\", 0, 0);"
           )
    dbGetQuery(conn, sql)
    
    sql <- "SELECT last_insert_rowid();"
    underlying.id <- dbGetQuery(conn, sql)[1, 1]
  } else {
    underlying.id <- underlying.info[1, 'ID']
  }
  
  # Check if stock option exists in stock options table
  sql <- paste0(
          "SELECT * FROM StockOptions
           WHERE Name = \"", name, "\";"
         )
  asset.info <- dbGetQuery(conn, sql)
  
  # If stock option doesn't exist, add it; else, update stock option's quantity and volume
  if (nrow(asset.info) == 0) {
    sql <- paste0(
            "INSERT INTO StockOptions(Name, Underlying, Type, Expiration, Strike, Quantity, Volume)
             VALUES(
                \"", name, "\", ",
                underlying.id, ", ",
                "\"", type, "\", ",
                expiration, ", ",
                strike, ", ",
                quantity, ", ",
                quantity, ", ",
            ");"
           )
    dbGetQuery(conn, sql)
    
    sql <- "SELECT last_insert_rowid();"
    asset.id <- dbGetQuery(conn, sql)[1, 1]
  } else {
    asset.id <- asset.info[1, 'ID']
    
    sql <- paste0(
            "UPDATE StockOptions
             SET Quantity = Quantity + ", quantity,
              ", Volume = Volume + ", abs(quantity),
           " WHERE Name = \"", name, "\";"
           )
    dbGetQuery(conn, sql)
  }
  
  # Add transaction into actions table
  sql <- paste0(
          "INSERT INTO Actions_StockOption(Timestamp, Option, Price, Quantity, Fees, CashChange)
           VALUES(",
              timestamp, ", ",
              asset.id, ", ",
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
            "UPDATE Actions_StockOption
             SET Purpose = \"", purpose, "\",
             WHERE ID = ", action.id, ";"
           )
    dbGetQuery(conn, sql)
  }
  
  if (!missing(notes)) {
    sql <- paste0(
            "UPDATE Actions_StockOption
             SET Notes = \"", notes, "\",
             WHERE ID = ", action.id, ";"
           )
    dbGetQuery(conn, sql)
  }
  
  # Update funds
  sql <- paste0(
          "UPDATE Funds
           SET Quantity = Quantity + ", cash.change,
         " WHERE ID = 1;"
         )
  dbGetQuery(conn, sql)
  
  dbDisconnect(conn)
}

add.action.fund <- function(timestamp, method, cash.change, notes, db.name = 'folio_data.db') {
  
  # Make timestamp an integer if it isn't
  timestamp <- as.numeric(timestamp)
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
  # Add transaction into actions table
  sql <- paste0(
          "INSERT INTO Actions_Fund(Timestamp, Method, CashChange)
           VALUES(",
              timestamp, ", ",
              "\"", method, "\", ",
              cash.change,
          ");"
         )
  dbGetQuery(conn, sql)
  
  sql <- "SELECT last_insert_rowid();"
  action.id <- dbGetQuery(conn, sql)[1, 1]
  
  # Add notes if they are there
  if (!missing(notes)) {
    sql <- paste0(
            "UPDATE Actions_Fund
             SET Notes = \"", notes, "\",
             WHERE ID = ", action.id, ";"
    )
    dbGetQuery(conn, sql)
  }
  
  # Update funds
  sql <- paste0(
          "UPDATE Funds
           SET Quantity = Quantity + ", cash.change,
         " WHERE ID = 1;"
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