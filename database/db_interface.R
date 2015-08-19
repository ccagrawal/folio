library(RSQLite)

GetStockInfo <- function(ticker, timestamp, information = 'Assets', db.name = 'folio_data.db') {
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
  # First get the asset info so we can get the stock's ID
  sql <- paste0(
          "SELECT * FROM Stocks
           WHERE Ticker = \"", ticker, "\";"
         )
  asset.info <- dbGetQuery(conn, sql)
  
  # If the stock doesn't exist in our assets table, it won't exist elsewhere
  if (nrow(asset.info) == 0) {
    return(NA)
  } else {
    id <- asset.info[1, 'ID']
  }
  
  # Use the stock's ID to get its info in other tables
  if (information == 'Assets') {
    return(asset.info)
  } else if (information == 'Values') {
    sql <- paste0(
            "SELECT * FROM Values_Stock
             WHERE Stock = ", id, ";"
           )
    return(dbGetQuery(conn, sql))
  } else if (information == 'Actions') {
    sql <- paste0(
            "SELECT * FROM Actions_Stock
             WHERE Stock = ", id, ";"
           )
    return(dbGetQuery(conn, sql))
  }
  
  dbDisconnect(conn)
}

UpdateStock <- function(ticker, quantity, name, notes, db.name = 'folio_data.db') {
  
  stock.info <- GetStockInfo(ticker)
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
  # If quantity not included, just set it to 0
  if (missing(quantity)) {
    quantity <- 0
  }
  
  # Update quantity and volume information
  if (nrow(stock.info) == 0) {
    sql <- paste0(
            "INSERT INTO Stocks(Ticker, Quantity, Volume)
             VALUES(
              \"", ticker, "\", ", 
              quantity, ", ", 
              abs(quantity), 
            ");"
           )
    dbGetQuery(conn, sql)
    
    # Get ID of the stock we just inserted
    sql <- "SELECT last_insert_rowid();"
    asset.id <- dbGetQuery(conn, sql)[1, 1]
  } else {
    sql <- paste0(
            "UPDATE Stocks
             SET Quantity = Quantity + ", quantity, ", 
                 Volume = Volume + ", abs(quantity), "
             WHERE Ticker = ", ticker, ";"
           )
    dbGetQuery(conn, sql)
    
    asset.id <- stock.info[1, 'ID']
  }
  
  # Add name and notes if they were supplied
  if (!missing(name)) {
    sql <- paste0(
            "UPDATE Actions_Stock
             SET Name = \"", name, "\",
             WHERE Ticker = ", ticker, ";"
           )
    dbGetQuery(conn, sql)
  }
  
  if (!missing(notes)) {
    sql <- paste0(
            "UPDATE Actions_Stock
             SET Notes = \"", notes, "\",
             WHERE Ticker = ", ticker, ";"
           )
    dbGetQuery(conn, sql)
  }
  
  dbDisconnect(conn)
  
  # Return asset.id just in case it's needed
  return(asset.id)
}

UpdateActionStock <- function(timestamp, ticker, price, quantity, fees, cash.change, purpose, notes, db.name = 'folio_data.db') {
  
  action.stock.info <- GetStockInfo(ticker)
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
  # If quantity not included, just set it to 0
  if (missing(quantity)) {
    quantity <- 0
  }
  
  # Update quantity and volume information
  if (nrow(stock.info) == 0) {
    sql <- paste0(
      "INSERT INTO Stocks(Ticker, Quantity, Volume)
      VALUES(
      \"", ticker, "\", ", 
      quantity, ", ", 
      abs(quantity), 
      ");"
      )
    dbGetQuery(conn, sql)
    
    # Get ID of the stock we just inserted
    sql <- "SELECT last_insert_rowid();"
    asset.id <- dbGetQuery(conn, sql)[1, 1]
  } else {
    sql <- paste0(
      "UPDATE Stocks
      SET Quantity = Quantity + ", quantity, ", 
      Volume = Volume + ", abs(quantity), "
      WHERE Ticker = ", ticker, ";"
    )
    dbGetQuery(conn, sql)
    
    asset.id <- stock.info[1, 'ID']
  }
  
  # Add name and notes if they were supplied
  if (!missing(name)) {
    sql <- paste0(
      "UPDATE Actions_Stock
      SET Name = \"", name, "\",
      WHERE Ticker = ", ticker, ";"
    )
    dbGetQuery(conn, sql)
  }
  
  if (!missing(notes)) {
    sql <- paste0(
      "UPDATE Actions_Stock
      SET Notes = \"", notes, "\",
      WHERE Ticker = ", ticker, ";"
    )
    dbGetQuery(conn, sql)
  }
  
  dbDisconnect(conn)
  
  # Return asset.id just in case it's needed
  return(asset.id)
}


UpdateActionStock <- function(timestamp, ticker, price, quantity, fees, cash.change, purpose, notes, db.name = 'folio_data.db') {
  
  # Make timestamp an integer if it isn't
  timestamp <- as.numeric(timestamp)
  
  # If cash.change not supplied, just calculate it
  if (missing(cash.change)) {
    cash.change <- -1 * price * quantity - fees
  }
  
  # Update asset table
  asset.id <- UpdateStock(ticker, quantity)
  
  conn <- dbConnect(drv = SQLite(), dbname = db.name)
  
 
  
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
           SET Quantity = Quantity + ", cash.change, "
           WHERE ID = 1;"
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
             SET Quantity = Quantity + ", quantity, ", 
                 Volume = Volume + ", abs(quantity), "
             WHERE Name = \"", name, "\";"
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
           SET Quantity = Quantity + ", cash.change, "
           WHERE ID = 1;"
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
           SET Quantity = Quantity + ", cash.change, "
           WHERE ID = 1;"
  )
  dbGetQuery(conn, sql)
  
  dbDisconnect(conn)
}
