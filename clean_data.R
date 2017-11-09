
library(RMySQL)
library(jsonlite)
library(lubridate)
library(dplyr)

keys<-fromJSON("../keys.json")$localJoeR_turnstile
con <- dbConnect(RMySQL::MySQL(), host = keys$host, dbname = keys$dbname, user = keys$id, password = keys$pw)

query <- paste0("SELECT CA, UNIT, SCP, COUNT(*) as mycount from raw_data",
                " where `DESC`='REGULAR'",
                " group by CA, UNIT, SCP")
turnstiles <- dbGetQuery(con, query)

#loop through each turnstile and create entries and exits numbers:
for (i in 4958:nrow(turnstiles)){
  CA = turnstiles[i,1]
  UNIT = turnstiles[i,2]
  SCP = turnstiles[i,3]
  
  #select all records for particular turnstile:
  query <- paste0("SELECT * FROM raw_data WHERE",
                  " `DESC`='REGULAR'",
                  " AND CA='",CA,
                  "' AND UNIT='", UNIT,
                  "' AND SCP='", SCP,
                  "' ORDER BY DATETIME")
  res <- dbGetQuery(con, query)
  
  if (nrow(res)>1){
  res<- res %>% mutate(ENTRIES = CUM_ENTRIES - lag(CUM_ENTRIES),
                 EXITS = CUM_EXITS - lag(CUM_EXITS))
  diff_table <- res %>% mutate(from_date = lag(DATETIME),
                               to_date = DATETIME)
  
  
  #convert small negatives to zeroes:
  diff_table[which(diff_table$ENTRIES < 0 & diff_table$ENTRIES >= -100),c("ENTRIES")] <- 0
  diff_table[which(diff_table$EXITS < 0 & diff_table$EXITS >= -100),c("EXITS")] <- 0
  
  #detect when counter rolled over and set to CUM value:
   diff_table[which(abs(diff_table$CUM_ENTRIES/diff_table$ENTRIES) <= 0.01),c("ENTRIES")] <- 
     diff_table[which(abs(diff_table$CUM_ENTRIES/diff_table$ENTRIES) <= 0.01),c("CUM_ENTRIES")]
   diff_table[which(abs(diff_table$CUM_EXITS/diff_table$EXITS) <= 0.01),c("EXITS")] <- 
     diff_table[which(abs(diff_table$CUM_EXITS/diff_table$EXITS) <= 0.01),c("CUM_EXITS")]
  
    
    #detect when CUM resets to random new state:
    diff_table$ENTRIES_avg <- tryCatch({
      rollapply(diff_table$ENTRIES, min(30,nrow(diff_table)-1), median, align='right', fill='extend', na.rm = T)
    }, error = function(e) {
      median(diff_table$ENTRIES[1:nrow(diff_table)], na.rm=T)
    })
    diff_table$ENTRIES_sd <- tryCatch({
      rollapply(diff_table$ENTRIES, min(30,nrow(diff_table)-1), sd, align='right', fill='extend', na.rm = T)
    }, error = function(e) {
      sd(diff_table$ENTRIES[1:nrow(diff_table)], na.rm=T)
    })
    diff_table <- diff_table %>%
      mutate(ENTRIES_z = (abs(ENTRIES)-lag(ENTRIES_avg))/lag(ENTRIES_sd),
             ENTRIES_out = (abs(ENTRIES_z) > 5))
    
    diff_table$EXITS_avg <- tryCatch({
      rollapply(diff_table$EXITS, min(30,nrow(diff_table)-1), median, align='right', fill='extend', na.rm = T)
    }, error = function(e) {
      median(diff_table$EXITS[1:nrow(diff_table)], na.rm=T)
    })
    diff_table$EXITS_sd <- tryCatch({
      rollapply(diff_table$EXITS, min(30,nrow(diff_table)-1), sd, align='right', fill='extend', na.rm = T)
    }, error = function(e) {
      sd(diff_table$EXITS[1:nrow(diff_table)], na.rm=T)
    })
    diff_table <- diff_table %>%
      mutate(EXITS_z = (abs(EXITS)-lag(EXITS_avg))/lag(EXITS_sd),
             EXITS_out = (abs(EXITS_z) > 5))
  
    #temp crete lag column of average so it can replace the outlier values
    diff_table <- diff_table %>%
      mutate(ENTRIES_avg_lag = lag(ENTRIES_avg),
             EXITS_avg_lag = lag(EXITS_avg))
    
    diff_table[which(diff_table$ENTRIES_out == T),c("ENTRIES")] <-
      diff_table[which(diff_table$ENTRIES_out == T),c("ENTRIES_avg_lag")]
    diff_table[which(diff_table$EXITS_out == T),c("EXITS")] <-
      diff_table[which(diff_table$EXITS_out == T),c("EXITS_avg_lag")]
    
    #reset diff_table to normal columns:
    diff_table <- diff_table[,c(1:15)]
    
  
  #correct backwards counter for specific station (A011,R080):
    diff_table[which(diff_table$ENTRIES < 0),c("ENTRIES")] <- 
      abs(diff_table[which(diff_table$ENTRIES < 0),c("ENTRIES")])
    diff_table[which(diff_table$EXITS < 0),c("EXITS")] <- 
      abs(diff_table[which(diff_table$EXITS < 0),c("EXITS")])
  
  res2 <- dbWriteTable(con, value = diff_table, name = "clean_data1", append = TRUE, row.names = FALSE)
  print(paste0("Wrote data (",i,
               "/",nrow(turnstiles),
               "): CA=",CA,
               " UNIT=",UNIT,
               " SCP=",SCP
               ))
  }
}

write.table(diff_table,"clipboard-10240",sep="\t", row.names=FALSE)
