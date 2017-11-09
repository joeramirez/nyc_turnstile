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



finagg <- data.frame(CA=character(), 
                  UNIT=character(), 
                  SCP=character(), 
                  month=numeric(), 
                  year=numeric(), 
                  ENTRIES=numeric(),
                  EXITS=numeric())
finclean <- data.frame(CA=character(), 
                       UNIT=character(), 
                       SCP=character(), 
                       STATION=character(),
                       LINENAME=character(),
                       DIVISION=character(),
                       DATETIME=as.POSIXct(character()),
                       DESC=character(),
                       CUM_ENTRIES=numeric(),
                       CUM_EXITS=numeric(),
                       ENTRIES=numeric(),
                       EXITS=numeric(),
                       CUS=character(),
                       from_date=as.POSIXct(character()),
                       to_date=as.POSIXct(character()),
                       hours=numeric(),
                       EN_hour=numeric(),
                       EX_hour=numeric())

#loop through each turnstile and create entries and exits numbers:
for (i in 1:nrow(turnstiles)){
  
  CA = turnstiles[i,1]
  UNIT = turnstiles[i,2]
  SCP = turnstiles[i,3]
  
  query <- paste0("SELECT * FROM raw_data WHERE",
                  " `DESC`='REGULAR'",
                  " AND CA='",CA,
                  "' AND UNIT='", UNIT,
                  "' AND SCP='", SCP,
                  "' ORDER BY DATETIME")
  res <- dbGetQuery(con, query)
  
  
  if (nrow(res)>1){
    
    res <- res[!duplicated(res$DATETIME),]
    res<- res %>% mutate(ENTRIES = abs(CUM_ENTRIES - lag(CUM_ENTRIES)),
                         EXITS = abs(CUM_EXITS - lag(CUM_EXITS)),
                         CUS = paste0(CA,"-",UNIT,"-",SCP))
    
    
    
    diff_table <- res %>% mutate(DATETIME = as.POSIXct(DATETIME, format = "%Y-%m-%d %H:%M:%S"),
                                 from_date = lag(DATETIME),
                                 to_date = DATETIME,
                                 hours = as.double(to_date - from_date, units = 'hours'),
                                 EN_hour = ENTRIES/hours,
                                 EX_hour = EXITS/hours)
                         
    
    #detect when counter reset:
    diff_table <- FUN_fixoutliers3(diff_table)
    diff_table <- FUN_removejumps(diff_table)
    
    
    #correct backwards counter for specific stations (A011,R080):
    #diff_table <- FUN_fixbackwards(diff_table)
    
    
    #aggregate by month
    if (nrow(diff_table) > 0){
      diff_table$MONTH <- month(diff_table$DATETIME)
      diff_table$YEAR <- year(diff_table$DATETIME)
      #agg <- aggregate(cbind(ENTRIES, EXITS) ~ CA + UNIT + SCP + MONTH + YEAR, diff_table, sum)
      #finagg <- rbind(finagg, agg)
      #finclean <- rbind(finclean, diff_table[2:19])
      res2 <- dbWriteTable(con, value = diff_table[2:19], name = "clean_data2", append = TRUE, row.names = FALSE)
      print(paste0("Wrote data (",i,
                   "/",nrow(turnstiles),
                   "): CA=",CA,
                   " UNIT=",UNIT,
                   " SCP=",SCP
      ))
    }
  }
  
}
  
write.table(diff_table,"clipboard-10240",sep="\t", row.names=FALSE)
  
  

FUN_fixoutliers <- function (df){
  rep <- 5
  repeat{
    df <- FUN_calcouts2(df)
    go_again <- any(df$ENTRIES_out, na.rm=T) | any(df$EXITS_out, na.rm=T)
    #print(paste0("Removing Outliers:"))
    #print(df[which(df$ENTRIES_out == T),c("ENTRIES")])
    #print(df[which(df$EXITS_out == T),c("EXITS")])
    
    df[which(df$ENTRIES_out == T),c("ENTRIES")] <- NA
    df[which(df$EXITS_out == T),c("EXITS")] <- NA
    print(paste0("Pass, rep=",rep))
    
    
    
    if (go_again == T){
      rep <- 5
    } else {
      rep <- rep - 1
      if (rep == 0){
        break
      }
    }
  }
  return(df)
}
  
# FUN_calcrolls <- function (df){
#   df$ENTRIES_avg <- tryCatch({
#     rollapply(df$ENTRIES, min(30,nrow(df)-1), median, align='right', fill='extend', na.rm = T)
#   }, error = function(e) {
#     median(df$ENTRIES[1:nrow(df)], na.rm=T)
#   })
#   
#   df$ENTRIES_sd <- tryCatch({
#     rollapply(df$ENTRIES, min(30,nrow(df)-1), sd, align='right', fill='extend', na.rm = T)
#   }, error = function(e) {
#     sd(df$ENTRIES[1:nrow(df)], na.rm=T)
#   })
#   
#   df$EXITS_avg <- tryCatch({
#     rollapply(df$EXITS, min(30,nrow(df)-1), median, align='right', fill='extend', na.rm = T)
#   }, error = function(e) {
#     median(df$EXITS[1:nrow(df)], na.rm=T)
#   })
#   
#   df$EXITS_sd <- tryCatch({
#     rollapply(df$EXITS, min(30,nrow(df)-1), sd, align='right', fill='extend', na.rm = T)
#   }, error = function(e) {
#     sd(df$EXITS[1:nrow(df)], na.rm=T)
#   })
#   
#   return(df)
# }
# 
# FUN_calcouts <- function(df){
#   df <- df %>%
#     mutate(ENTRIES_z = (abs(ENTRIES)-lag(ENTRIES_avg))/lag(ENTRIES_sd),
#            ENTRIES_out = (abs(ENTRIES_z) > 500))
#   
#   df <- df %>%
#     mutate(EXITS_z = (abs(EXITS)-lag(EXITS_avg))/lag(EXITS_sd),
#            EXITS_out = (abs(EXITS_z) > 500))
# }

FUN_calcouts2 <- function(df){
  samp_thresh <- .05
  
  #if only 2 rows, then just say they are both NOT outliers
  if (nrow(df) == 2){
    df <- df %>%
      mutate(ENTRIES_z = NA,
             ENTRIES_out = FALSE,
             EXITS_z = NA,
             EXITS_out = FALSE)
    return(df)
  }
  
  #if less than 500 rows, use the full df to calc stdev
  if (nrow(df) > 500){
    df_sam <- df[sample(2:nrow(df), max(2,round(samp_thresh*nrow(df)))),c("ENTRIES","EXITS")]
  } else {
    df_sam <- df[,c("ENTRIES","EXITS")]
  }

  ENTRIES_avg <- median(df$ENTRIES, na.rm=T)
  ENTRIES_sd <- sd(df_sam$ENTRIES, na.rm=T)
  
  
  if (ENTRIES_sd == 0){
    ENTRIES_sd <- sd(df$ENTRIES, na.rm=T)
  }
  if (ENTRIES_sd != 0){
    df <- df %>%
      mutate(ENTRIES_z = (abs(ENTRIES)-ENTRIES_avg)/ENTRIES_sd,
             ENTRIES_out = (abs(ENTRIES_z) > 500))
  } else {
    warning(paste0("EN SD=0 for CA=",unique(df$CA),", UNIT=", unique(df$UNIT),
                   ", SCP=", unique(df$SCP)))
  }
  
  
  EXITS_avg <- median(df$EXITS, na.rm=T)
  EXITS_sd <- sd(df_sam$EXITS, na.rm=T)
  if (EXITS_sd == 0) {
    EXITS_sd <- sd(df$EXITS, na.rm=T)
  }
  if (EXITS_sd != 0){
    df <- df %>%
      mutate(EXITS_z = (abs(EXITS)-EXITS_avg)/EXITS_sd,
             EXITS_out = (abs(EXITS_z) > 500))
  } else {
    warning(paste0("EX SD=0 for CA=",unique(df$CA),", UNIT=", unique(df$UNIT),
                   ", SCP=", unique(df$SCP)))
  }
  print(paste0("ENTRIES_avg=",ENTRIES_avg,
               "  ENTRIES_sd=",ENTRIES_sd,
               "  EXITS_avg=",EXITS_avg,
               "  EXITS_sd=",EXITS_sd))

  return(df)
}

FUN_fixbackwards <- function(df){
  df[which(df$ENTRIES < 0),c("ENTRIES")] <- 
    abs(df[which(df$ENTRIES < 0),c("ENTRIES")])
  df[which(df$EXITS < 0),c("EXITS")] <- 
    abs(df[which(df$EXITS < 0),c("EXITS")])
  return(df)
}




#send it just a vector of values and return T/F vector
FUN_calcouts3 <- function(x){
  
  #if only 2 rows, then just say they are both NOT outliers
  if (length(x) == 2){
    return(rep(F,length(x)))
  }
  
  
  x_med <- median(x, na.rm=T)
  x_avg <- mean(x, na.rm=T)
  x_sd <- sd(x, na.rm=T)
  
  #if stdev == 0, then all vals must be the same, so return false for all
  if (x_sd == 0){
    return(rep(F,length(x)))
  }
  
  
  x_z <- (abs(x) - x_med)/x_sd
  if (x_med == 0){
    x_dev <- abs(x)/x_avg
  } else {
    x_dev <- abs(x)/x_med
  }
  x_out <- (x_z >= 500) | (x_dev >= 10)    #what if one is NA?
  x_df <- data.frame(x = x, x_out = x_out)
  
  return(x_out)
}

FUN_removejumps <- function(df){
  thresh_days <- 30
  df <- df[which(df$hours < thresh_days*24),]
  return(df)
}

FUN_fixoutliers3 <- function (df){

  df$EN_out <- FUN_calcouts3(df$EN_hour)
  df$EX_out <- FUN_calcouts3(df$EX_hour)
  

  
  df[which(df$EN_out == T),c("ENTRIES")] <- NA
  df[which(df$EX_out == T),c("EXITS")] <- NA
    

  return(df)
}


ggplot(data = diff_table, aes(x=DATETIME)) +
  geom_line(aes(y = ENTRIES, colour = "Entries")) +
  geom_point(aes(y = ENTRIES, colour = "Entries")) +
  geom_line(aes(y = EXITS, colour = "Exits")) +
  geom_point(aes(y = EXITS, colour = "Exits")) +
  scale_y_continuous(name = "SHARE", labels = comma)
