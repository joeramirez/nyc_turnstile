library(rvest)
library(RMySQL)
library(jsonlite)
library(lubridate)
library(RCurl)
library(tidyr)
library(dplyr)
library(httr)


keys<-fromJSON("../keys.json")$localJoeR_turnstile
con <- dbConnect(RMySQL::MySQL(), host = keys$host, dbname = keys$dbname, user = keys$id, password = keys$pw)

homepage <- "http://web.mta.info/developers/turnstile.html"
filepath <- "http://web.mta.info/developers/"
mta <- read_html(homepage)

files <- data.frame(filedate = mta %>% html_nodes(".last a") %>% html_text(),
           filepath = mta %>% html_nodes(".last a") %>% html_attr("href"))

check_insert <- function(){
  query <- "SELECT * FROM raw_data WHERE data_id=(SELECT MAX(data_id) FROM raw_data)"
  df <- dbGetQuery(con,query)
  check_date <- (substr(df[1,8],1,10) != "0000-00-00" | is.na(df[1,8]))
  if (check_date == FALSE){
    stop("Date Error.  Check input file")
  }
  query <- "SELECT count(*) FROM raw_data WHERE MONTH(DATETIME)=0"
  df <- dbGetQuery(con,query)
  check_zero <- df[1,1] == 0
  if (check_zero == FALSE) {
    stop("Zero dates were loaded!")
  }
}

file_status <- dbGetQuery(con, "SELECT * FROM files")
for (f in 2:1){
  
  file_date <- as.Date(files[f,1], "%A, %B %d, %Y")
  file_name <- files[f,2]
  
  if (!(file_name %in% file_status[which(file_status$status == 'loaded'),c("file_name")])){
    
    if (file_date > '2014-10-11'){

      dat <- tryCatch({
        read.csv(textConnection(getURL(paste0(filepath,files[f,2]))), header = T)
      }, error = function(e) {
        print(paste0("File error... using HTTR: ", file_name))
        return(read.csv(textConnection(content(GET(paste0(filepath,files[f,2])))), header = T))
      })
      write.table(dat,paste0("files/",file_date,".csv"))
      print(paste0("Current file downloaded: ",file_name))
      
      dat$DATETIME <- paste(dat$DATE,dat$TIME)
      dat$DATETIME <- strptime(dat$DATETIME, format="%m/%d/%Y %H:%M:%S")
      dat <- dat[,c(1:6,12,9:11)]
      names(dat) <- c('CA','UNIT','SCP','STATION','LINENAME','DIVISION','DATETIME',
                      'DESC','CUM_ENTRIES','CUM_EXITS')
      res <- dbWriteTable(con, value = dat, name = "raw_data", append = TRUE, row.names = FALSE)
    } else {
      
      dat <- tryCatch({
        read.csv(textConnection(getURL(paste0(filepath,files[f,2]))), header = F)
      }, error = function(e) {
        print(paste0("File error... using HTTR: ", file_name))
        return(read.csv(textConnection(content(GET(paste0(filepath,files[f,2])))), header = F))
      })
      print(paste0("Old file downloaded: ",file_name))
      write.table(dat,paste0("files/",file_date,".csv"))
      names(dat) <- c('CA','UNIT','SCP',
                      'f1.DATE','f1.TIME','f1.DESC','f1.ENTRIES','f1.EXITS',
                      'f2.DATE','f2.TIME','f2.DESC','f2.ENTRIES','f2.EXITS',
                      'f3.DATE','f3.TIME','f3.DESC','f3.ENTRIES','f3.EXITS',
                      'f4.DATE','f4.TIME','f4.DESC','f4.ENTRIES','f4.EXITS',
                      'f5.DATE','f5.TIME','f5.DESC','f5.ENTRIES','f5.EXITS',
                      'f6.DATE','f6.TIME','f6.DESC','f6.ENTRIES','f6.EXITS',
                      'f7.DATE','f7.TIME','f7.DESC','f7.ENTRIES','f7.EXITS',
                      'f8.DATE','f8.TIME','f8.DESC','f8.ENTRIES','f8.EXITS')
      
      dat1 <- dat[,c(1,2,3,4:8)]
      dat2 <- dat[,c(1,2,3,9:13)]
      dat3 <- dat[,c(1,2,3,14:18)]
      dat4 <- dat[,c(1,2,3,19:23)]
      dat5 <- dat[,c(1,2,3,24:28)]
      dat6 <- dat[,c(1,2,3,29:33)]
      dat7 <- dat[,c(1,2,3,34:38)]
      dat8 <- dat[,c(1,2,3,39:43)]
      names(dat1) <- c('CA','UNIT','SCP','DATE','TIME','DESC','ENTRIES','EXITS')
      names(dat2) <- c('CA','UNIT','SCP','DATE','TIME','DESC','ENTRIES','EXITS')
      names(dat3) <- c('CA','UNIT','SCP','DATE','TIME','DESC','ENTRIES','EXITS')
      names(dat4) <- c('CA','UNIT','SCP','DATE','TIME','DESC','ENTRIES','EXITS')
      names(dat5) <- c('CA','UNIT','SCP','DATE','TIME','DESC','ENTRIES','EXITS')
      names(dat6) <- c('CA','UNIT','SCP','DATE','TIME','DESC','ENTRIES','EXITS')
      names(dat7) <- c('CA','UNIT','SCP','DATE','TIME','DESC','ENTRIES','EXITS')
      names(dat8) <- c('CA','UNIT','SCP','DATE','TIME','DESC','ENTRIES','EXITS')
      datx <- rbind(dat1,dat2,dat3,dat4,dat5,dat6,dat7,dat8)
      datx <- datx[which((is.na(datx$ENTRIES) | is.na(datx$EXITS)) == FALSE),]
      datx$STATION <- NA
      datx$LINENAME <- NA
      datx$DIVISION <- NA
      datx$DATE <- as.Date(datx$DATE, "%m-%d-%y")
      datx$DATETIME <- paste(datx$DATE,datx$TIME)
      datx$DATETIME <- strptime(datx$DATETIME, format="%Y-%m-%d %H:%M:%S")
      datx <- datx[,c(1:3,9:11,12,6:8)]
      datx <- datx[with(datx, order(CA, UNIT, SCP, DATETIME)), ]
      rownames(datx) <- 1:nrow(datx)
      res <- dbWriteTable(con, value = datx, name = "raw_data1", append = TRUE, row.names = FALSE)
    }
    
    if (res == TRUE){
      check_insert()
      print(paste0("Wrote data: ",file_name))
      fstat <- data.frame(file_name = file_name,
                          file_date = file_date,
                          status = 'loaded')
      res2 <- dbWriteTable(con, value = fstat,
                   name = "files", append = TRUE, row.names = FALSE)
      if (res2 == TRUE){
        print(paste0("Confirmed write: ",file_name))
      } else {
        stop(paste0("Error confirming write: ", file_name))
      }
    } else {
      stop(paste0("Error writing data: ", file_name))
    }
  }
}

