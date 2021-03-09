library(DBI)
library(odbc)
library(dplyr)
driver <- "SQL Server"
server <- "localhost\\SQLEXPRESS"
db <- "RDatabase"

write_table_sql <- function(driver, server, db, trusted="True",
                        schema = "dbo", tbl, append=FALSE, 
                        overwrite = TRUE, tbl_name, ...){
  
  params <- list(driver=driver, server=server, 
                 database=db, trusted=trusted, db_schema=schema,
                 db_tbl=tbl, append_bool=append, 
       overwrite_bool=overwrite, table_name=tbl_name, ...)
  
  cat("Establishing connection to:", server, "\n")
  con <- dbConnect(odbc(), 
                   Driver = driver , 
                   Server = server , 
                   Database = db, 
                   Trusted_Connection = "True")
  
  
  if(tbl_name == "" | !is.character(tbl_name) | length(tbl_name)<0){
    stop("The table name must be specified")
  }
  
  cat("Writing to database - please wait...", "\n")
  # Write the data to the database
  
  timings <- system.time(
  odbc::dbWriteTable(con, Id(schema = schema, 
                             table = tbl_name), 
                     tbl, append = append, overwrite = overwrite, ...)
  )
  
  params <- list(input_params=params, run_time=as.double(timings[3]))
  
  cat("Finished writing to database:", db, "in",
      as.double(timings[3]), 
      "seconds.","\n")
  
  return(params)
  
}

#Use new function
mort_ons <- ons_mortality
test_table <- write_table_sql(driver, server, db, schema="data",
            tbl=mort_ons, append=TRUE, overwrite=FALSE,
            tbl_name="onsMortality")




