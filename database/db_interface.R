source('./database/db_management.R')

GetStocks <- function(ticker, name, quantity, volume, notes) {
  
  sql <- "SELECT * FROM Stocks"
  
  # Conditions will contain each WHERE clause
  conditions <- c()
  
  # Go through each parameter and add a clause if necessary
  if (!missing(ticker)) {
    conditions <- c(conditions, paste0("Ticker IN ('", paste(ticker, collapse = "\', \'"), "')"))
  }
  
  if (!missing(name)) {
    conditions <- c(conditions, paste0("Name IN ('", paste(name, collapse = "\', \'"), "')"))
  }
  
  if (!missing(quantity)) {
    if (quantity == 'positive') {
      conditions <- c(conditions, "Quantity > 0")
    } else if (quantity == 'negative') {
      conditions <- c(conditions, "Quantity < 0")
    } else if (quantity == 'nonzero') {
      conditions <- c(conditions, "Quantity != 0")
    }
  }
  
  if (!missing(volume)) {
    if (volume == 'positive') {
      conditions <- c(conditions, "Volume > 0")
    } else {
      conditions <- c(conditions, paste0("Volume >= ", volume))
    }
  }
  
  
  if (!missing(notes)) {
    conditions <- c(conditions, paste0("Notes LIKE \"", notes, "\""))
  }
  
  # Create full sql statement by collapsing conditions
  if (length(conditions) > 0) {
    sql <- paste0(sql, " WHERE ", paste(conditions, collapse = " AND "), ";")
  } else {
    sql <- paste0(sql, ";")
  }
  
  return(RunQuery(sql))
}

UpdateStock <- function(ticker, name, quantity, notes, action = 'add') {
  
  sql <- "INSERT OR REPLACE INTO Stocks(ID, Ticker, Quantity, Volume, Name, Notes) "
  values <- paste0("VALUES((SELECT ID FROM Stocks WHERE Ticker = \"", ticker, "\"), \"", ticker, "\"")
  
  # If quantity not included, just set it to 0
  if (missing(quantity)) {
    quantity <- 0
  }
  
  # If we're deleting an action, flip the quantity and volume
  if (action == 'delete') {
    quantity <- -quantity
    volume <- -abs(quantity)
  } else {
    volume <- abs(quantity)
  }
  
  values <- paste0(values, ",
              COALESCE((SELECT Quantity FROM Stocks WHERE Ticker = \"", ticker, "\"), 0) + ", quantity, ",
              COALESCE((SELECT Volume FROM Stocks WHERE Ticker = \"", ticker, "\"), 0) + ", volume)
  
  # Add name and notes if they were supplied
  if (!missing(name)) {
    values <- paste0(values, ", \"", name, "\"")
  } else {
    values <- paste0(values, ", (SELECT Name FROM Stocks WHERE Ticker = \"", ticker, "\")")
  }
  
  if (!missing(notes)) {
    values <- paste0(values, ", \"", notes, "\"")
  } else {
    values <- paste0(values, ", (SELECT Notes FROM Stocks WHERE Ticker = \"", ticker, "\")")
  }
  
  sql <- paste0(sql, values, ");")
  RunQuery(sql)
}

GetActionsStock <- function(id, timestamp, ticker, price, quantity, cash.change, purpose, notes) {
  
  sql <- "SELECT Actions_Stock.ID, 
                 Actions_Stock.Timestamp,
                 Stocks.Ticker,
                 Actions_Stock.Price,
                 Actions_Stock.Quantity,
                 Actions_Stock.Fees,
                 Actions_Stock.CashChange,
                 Actions_Stock.Purpose,
                 Actions_Stock.Notes
          FROM Actions_Stock
          JOIN Stocks ON Actions_Stock.Stock = Stocks.ID"
  
  # Conditions will contain each WHERE clause
  conditions <- c()
  
  # Go through each parameter and add a clause if necessary
  if (!missing(id)) {
    conditions <- c(conditions, paste0("ID = ", id))
  }
  
  if (!missing(timestamp)) {
    timestamp <- as.numeric(timestamp)
    conditions <- c(conditions, paste0("Timestamp >= ", timestamp[1]))
    if (length(timestamp) == 2) {
      conditions <- c(conditions, paste0("Timestamp <= ", timestamp[2]))
    }
  }
  
  if (!missing(ticker)) {
    conditions <- c(conditions, paste0("Ticker IN (", paste(ticker, collapse = ", "), ")"))
  }
  
  if (!missing(price)) {
    conditions <- c(conditions, paste0("Price >= ", price[1]))
    if (length(price) == 2) {
      conditions <- c(conditions, paste0("Price <= ", price[2]))
    }
  }
  
  if (!missing(quantity)) {
    if (quantity == 'positive') {
      conditions <- c(conditions, "Quantity > 0")
    } else if (quantity == 'negative') {
      conditions <- c(conditions, "Quantity < 0")
    } else if (quantity == 'nonzero') {
      conditions <- c(conditions, "Quantity != 0")
    }
  }
  
  if (!missing(cash.change)) {
    conditions <- c(conditions, paste0("CashChange >= ", cash.change[1]))
    if (length(cash.change) == 2) {
      conditions <- c(conditions, paste0("CashChange <= ", cash.change[2]))
    }
  }
  
  if (!missing(purpose)) {
    conditions <- c(conditions, paste0("Purpose LIKE \"", purpose, "\""))
  }
  
  if (!missing(notes)) {
    conditions <- c(conditions, paste0("Notes LIKE \"", notes, "\""))
  }
  
  # Create full sql statement by collapsing conditions
  if (length(conditions) > 0) {
    sql <- paste0(sql, " WHERE ", paste(conditions, collapse = " AND "), ";")
  } else {
    sql <- paste0(sql, ";")
  }
  
  return(RunQuery(sql))
}

DeleteActionsStock <- function(id) {
  action.info <- GetActionsStock(id)
  
  # Undo change to funds
  UpdateFunds(-action.info[1, 'CashChange'])
  
  # Undo change to stock position and volume
  UpdateStock(ticker = action.info[1, 'Ticker'], 
              quantity = action.info[1, 'Quantity'],
              action = 'delete')
  
  sql <- paste0("DELETE FROM Actions_Stock WHERE ID = ", id, ";")
  RunQuery(sql)
  
  return(action.info)
}

UpdateActionsStock <- function(id, timestamp, ticker, price, quantity, fees, cash.change, purpose, notes) {
  
  sql <- "INSERT OR REPLACE INTO Actions_Stock("
  values <- paste0("VALUES(")
  
  if (!missing(id)) {
    action.info <- DeleteActionsStock(id)
    sql <- paste0(sql, "ID, ")
    values <- paste0(values, id, ", ")
  }
  
  # Make timestamp an integer if it isn't
  timestamp <- as.numeric(timestamp)
  
  # If cash.change not supplied, just calculate it
  if (missing(cash.change)) {
    cash.change <- -1 * price * quantity - fees
  }
  
  # Update Stock table
  UpdateStock(ticker = ticker, quantity = quantity)
  
  # Get Stock ID
  stock.sql <- paste0("SELECT ID FROM Stocks WHERE Ticker = \"", ticker, "\"")
  stock.id <- RunQuery(stock.sql)[1, 1]
  
  sql <- paste0(sql, "Timestamp, Stock, Price, Quantity, Fees, CashChange")
  values <- paste0(values, 
                   timestamp, ", ",
                   stock.id, ", ",
                   price, ", ",
                   quantity, ", ",
                   fees, ", ",
                   cash.change)
  
  # Add purpose and notes if they were supplied
  if (!missing(purpose)) {
    sql <- paste0(sql, ", Purpose")
    values <- paste0(values, ", \"", purpose, "\"")
  } else if (!missing(id) & !is.na(action.info[1, 'Purpose'])) {
    sql <- paste0(sql, ", Purpose")
    values <- paste0(values, ", \"", action.info[1, 'Purpose'], "\"")
  }
  
  if (!missing(notes)) {
    sql <- paste0(sql, ", Notes")
    values <- paste0(values, ", \"", notes, "\"")
  } else if (!missing(id) & !is.na(action.info[1, 'Notes'])) {
    sql <- paste0(sql, ", Notes")
    values <- paste0(values, ", \"", action.info[1, 'Notes'], "\"")
  }
  
  sql <- paste0(sql, ") ", values, ");")
  RunQuery(sql)
}

GetFunds <- function() {
  sql <- "SELECT * FROM Funds"
  return(RunQuery(sql)[1, 'Quantity'])
}

UpdateFunds <- function(quantity) {
  sql <- paste0(
          "UPDATE Funds
           SET Quantity = Quantity + ", quantity, "
           WHERE ID = 1"
         )
  RunQuery(sql)
}

# Outdated

add.action.stock.option <- function(timestamp, underlying, type, expiration, strike, price, quantity, fees, cash.change, purpose, notes) {
  
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

add.action.fund <- function(timestamp, method, cash.change, notes) {
  
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
