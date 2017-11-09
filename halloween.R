library(RMySQL)
library(jsonlite)
library(dplyr)
library(purrr)
library(ggplot2)


keys<-fromJSON("../keys.json")$localJoeR_turnstile
con <- dbConnect(RMySQL::MySQL(), host = keys$host, dbname = keys$dbname, user = keys$id, password = keys$pw)

# CA UNIT and STATION LINENAME associations
query <- paste0("SELECT DISTINCT CA,UNIT,STATION,LINENAME from raw_data WHERE STATION IS NOT NULL")
cu_stations <- dbGetQuery(con, query)


#time series of ca, unit combinations
cu_ts <- finagg
cu_ts <- cu_ts %>% mutate(CU = paste0(CA,"-",UNIT))
cu_ts <- cu_ts %>% mutate(CUS = paste0(CA,"-",UNIT,"-",SCP))

#join on ca and unit to get station that is clean (ie, no differnt spellings)
cu_stations <- cu_stations %>% mutate(CU = paste0(CA,"-",UNIT))
cu_stations_uq <- cu_stations[!duplicated(cu_stations$CU),]
cu_ts <- left_join(cu_ts, cu_stations_uq, by = c("CA","UNIT"))
cu_ts <- cu_ts[,1:10]
names(cu_ts)[8] <- "CU"


#remove cu combinations that dont have station name and start in 2011 only
cu_ts <- cu_ts %>% filter(!is.na(STATION))
cu_ts <- cu_ts %>% mutate(DATE = as.Date(paste0(YEAR,"-",MONTH,"-01")))
cu_ts <- cu_ts %>% filter(DATE >= '2011-01-01' & DATE <= '2017-10-31')




#aggregate by station & systemwide
stations_ts <- aggregate(cbind(ENTRIES,EXITS) ~ STATION + MONTH + YEAR + DATE, cu_ts, sum)
system_ts <- aggregate(cbind(ENTRIES, EXITS) ~ MONTH + YEAR + DATE, stations_ts, sum)

ggplot(data = system_ts, aes(x=DATE)) +
  geom_line(aes(y = ENTRIES, colour="ENTRIES")) +
  geom_point(aes(y = ENTRIES), shape = 16) +
  geom_line(aes(y = EXITS, colour="EXITS")) +
  geom_point(aes(y = EXITS), shape=18) +
  scale_y_continuous(name = "ENTRIES, EXITS", labels = comma)

aggregate(ENTRIES ~ YEAR, system_ts2, sum)

# get percent of total by station by month
stations_ts <- left_join(stations_ts, system_ts, by = c("DATE"))
stations_ts <- stations_ts[,c(1:6,9:10)]
names(stations_ts) <- c("STATION","MONTH","YEAR", "DATE", "EN_STAT","EX_STAT",
                        "EN_SYS", "EX_SYS")
stations_ts <- stations_ts %>%
  mutate(EN_SHARE = EN_STAT/EN_SYS,
         EX_SHARE = EX_STAT/EX_SYS)


#plot share for specific station
stations_by <- by(stations_ts, stations_ts$STATION, function(x){x})
mystat <- stations_by$`42 ST-TIMES SQ`
ggplot(data = mystat, aes(x=DATE)) +
  geom_line(aes(y = EN_SHARE, colour = "Entries")) +
  geom_point(aes(y = EN_SHARE, colour = "Entries")) +
  geom_line(aes(y = EX_SHARE, colour = "Exits")) +
  geom_point(aes(y = EX_SHARE, colour = "Exits")) +
  scale_y_continuous(name = "% SHARE of System", labels = percent)

# get percent of total by turnstile by month
cu_ts <- left_join(cu_ts, system_ts, by = c("DATE"))
cu_ts <- cu_ts[,c(1:11,14:15)]
names(cu_ts)[4:7] <- c("MONTH", "YEAR", "EN_STAT","EX_STAT")
names(cu_ts)[12:13] <- c("EN_SYS","EX_SYS")
cu_ts <- cu_ts %>%
  mutate(EN_SHARE = EN_STAT/EN_SYS,
         EX_SHARE = EX_STAT/EX_SYS)


#plot share for specific turnstile
cu_by <- by(cu_ts, cu_ts$CUS, function(x){x})
for (v in 1:length(US_turns)){
myturn <- cu_by[[US_turns[v]]]
ggplot(data = myturn, aes(x=DATE)) +
  geom_line(aes(y = EN_SHARE, colour = "Entries")) +
  geom_point(aes(y = EN_SHARE, colour = "Entries")) +
  geom_line(aes(y = EX_SHARE, colour = "Exits")) +
  geom_point(aes(y = EX_SHARE, colour = "Exits")) +
  scale_y_continuous(name = "SHARE", labels = percent)
ggsave(paste0(US_turns[[v]],".png"), device="png")
}

GC_turns <- unique(cu_ts[which(cu_ts$STATION == '42 ST-GRD CNTRL'),c("CUS")])
US_turns <- unique(cu_ts[which(cu_ts$STATION == '14 ST-UNION SQ'),c("CUS")])

#check each time series for issues
fun_checkts <- function(df){
  
  num_months <- nrow(df)
  ENT_avg <- mean(df$ENTRIES)
  ENT_sd <- sd(df$ENTRIES)
  ENT_z <- (df$ENTRIES - ENT_avg)/ENT_sd
  ENT_zmax <- max(ENT_z)
  ENT_zmin <- min(ENT_z)
  
  EX_avg <- mean(df$EXITS)
  EX_sd <- sd(df$EXITS)
  EX_z <- (df$EXITS - EX_avg)/EX_sd
  EX_zmax <- max(EX_z)
  EX_zmin <- min(EX_z)
  
  ENT_zero <- nrow(df[which(df$ENTRIES == 0),])/nrow(df)
  EX_zero <- nrow(df[which(df$EXITS == 0),])/nrow(df)
  
  check <- data.frame(num_months = num_months,
                      ENT_min = min(df$ENTRIES),
                      ENT_max = max(df$ENTRIES),
                      ENT_avg = ENT_avg,
                      ENT_sd = ENT_sd,
                      ENT_zmax = ENT_zmax,
                      ENT_zmin = ENT_zmin,
                      ENT_zero = ENT_zero,
                      EX_min = min(df$EXITS),
                      EX_max = max(df$EXITS),
                      EX_avg = EX_avg,
                      EX_sd = EX_sd,
                      EX_zmax = EX_zmax,
                      EX_zmin = EX_zmin,
                      EX_zero = EX_zero)
  
  return(check)
  
}

cu_checks <- lapply(cu_by, fun_checkts)
cu_checks <- data.frame(CU = names(cu_checks),
                  num_months = cu_checks %>% map_dbl("num_months"),
                  ENT_min = cu_checks %>% map_dbl("ENT_min"),
                  ENT_max = cu_checks %>% map_dbl("ENT_max"),
                  ENT_avg = cu_checks %>% map_dbl("ENT_avg"),
                  ENT_sd = cu_checks %>% map_dbl("ENT_sd"),
                  ENT_zmax = cu_checks %>% map_dbl("ENT_zmax"),
                  ENT_zmin = cu_checks %>% map_dbl("ENT_zmin"),
                  ENT_zero = cu_checks %>% map_dbl("ENT_zero"),
                  EX_min = cu_checks %>% map_dbl("EX_min"),
                  EX_max = cu_checks %>% map_dbl("EX_max"),
                  EX_avg = cu_checks %>% map_dbl("EX_avg"),
                  EX_sd = cu_checks %>% map_dbl("EX_sd"),
                 EX_zmax = cu_checks %>% map_dbl("EX_zmax"),
                 EX_zmin = cu_checks %>% map_dbl("EX_zmin"),
                 EX_zero = cu_checks %>% map_dbl("EX_zero"),
                 row.names = NULL)

stations_checks <- lapply(stations_by, fun_checkts)
stations_checks <- data.frame(STATION = names(stations_checks),
                              num_months = cu_checks %>% map_dbl("num_months"),
                              ENT_min = cu_checks %>% map_dbl("ENT_min"),
                              ENT_max = cu_checks %>% map_dbl("ENT_max"),
                              ENT_avg = cu_checks %>% map_dbl("ENT_avg"),
                              ENT_sd = cu_checks %>% map_dbl("ENT_sd"),
                              ENT_zmax = cu_checks %>% map_dbl("ENT_zmax"),
                              ENT_zmin = cu_checks %>% map_dbl("ENT_zmin"),
                              ENT_zero = cu_checks %>% map_dbl("ENT_zero"),
                              EX_min = cu_checks %>% map_dbl("EX_min"),
                              EX_max = cu_checks %>% map_dbl("EX_max"),
                              EX_avg = cu_checks %>% map_dbl("EX_avg"),
                              EX_sd = cu_checks %>% map_dbl("EX_sd"),
                              EX_zmax = cu_checks %>% map_dbl("EX_zmax"),
                              EX_zmin = cu_checks %>% map_dbl("EX_zmin"),
                              EX_zero = cu_checks %>% map_dbl("EX_zero"),
                        row.names = NULL)




##### daily analysis
## aggregate into days instead of hours
query <- paste0("SELECT CA, UNIT, SCP, CUS, DATE(DATETIME), SUM(ENTRIES), SUM(EXITS)",
                " FROM clean_data2",
                " GROUP BY CUS, DATE(DATETIME)")
cus_dts <- dbGetQuery(con, query)
names(cus_dts) <- c("CA","UNIT","SCP","CUS","DATE","ENTRIES","EXITS")
cus_dts <- left_join(cus_dts, cu_stations_uq, by = c("CA","UNIT"))
cus_dts <- cus_dts[,1:9]
cus_dts <- cus_dts %>%
  mutate(YEAR = year(DATE),
         MONTH = month(DATE),
         DAY = day(DATE),
         MONTHDAY = paste0(MONTH,"-",DAY),
         HALLOWEEN = (MONTH == 10) & (DAY == 31))

#remove cu combinations that dont have station name and start in 2011 only
cus_dts <- cus_dts %>% filter(!is.na(STATION))
cus_dts <- cus_dts %>% filter(DATE >= '2011-01-01' & DATE <= '2017-10-31')

#aggregate by station
station_dts <- aggregate(cbind(ENTRIES,EXITS) ~ STATION + DATE + YEAR + MONTH + DAY + MONTHDAY + HALLOWEEN,
                         cus_dts, sum)
system_dts <- aggregate(cbind(ENTRIES, EXITS) ~ DATE + YEAR + MONTH + DAY + MONTHDAY + HALLOWEEN,
                        station_dts, sum)
system_dts2 <- system_dts %>%
  filter(DATE != "2014-12-06") %>%
  filter(DATE != "2011-09-24")

ggplot(data = system_dts, aes(x=DATE, y= ENTRIES)) +
  geom_line(aes(y = ENTRIES, colour="ENTRIES")) +
  geom_point(aes(y = ENTRIES, colour="ENTRIES"), shape = 16) +
  geom_line(aes(y = EXITS, colour="EXITS")) +
  geom_point(aes(y = EXITS, colour="EXITS"), shape=18) +
  scale_y_continuous(name = "ENTRIES, EXITS", labels = comma)

#share of total by station
station_dts <- left_join(station_dts, system_dts[,c("DATE","ENTRIES","EXITS")], by = c("DATE"))
names(station_dts) <- c("STATION","DATE","YEAR", "MONTH","DAY","MONTHDAY","HALLOWEEN", "EN_STAT","EX_STAT",
                        "EN_SYS", "EX_SYS")
station_dts <- station_dts %>%
  mutate(EN_SHARE = EN_STAT/EN_SYS,
         EX_SHARE = EX_STAT/EX_SYS)

#plot share for specific station
station_dts_by <- by(station_dts, station_dts$STATION, function(x){x})
mystat <- station_dts_by$`103 ST`
ggplot(data = mystat, aes(x=DATE)) +
  geom_line(aes(y = EN_SHARE, colour = "Entries")) +
  geom_point(aes(y = EN_SHARE, colour = "Entries")) +
  geom_line(aes(y = EX_SHARE, colour = "Exits")) +
  geom_point(aes(y = EX_SHARE, colour = "Exits")) +
  scale_y_continuous(name = "% SHARE of System", labels = percent)

hall_dts <- aggregate(cbind(EN_SHARE, EX_SHARE) ~ STATION + HALLOWEEN, station_dts,
                      function(x) c(mean = mean(x), med = median(x), sd = sd(x)))
hall_dts <- do.call(data.frame,hall_dts)

station_dts_sd <- aggregate(cbind(EN_SHARE, EX_SHARE) ~ STATION, station_dts, 
                            function(x) c(sd = sd(x), n = length(x)))
station_dts_sd <- do.call(data.frame, station_dts_sd)


names(station_dts_sd) <- c("STATION","EN_SHARE_all.sd","EN_SHARE_all.n",
                           "EX_SHARE_all.sd","EX_SHARE_all.n")

station_hall <- left_join(station_dts_sd, 
                          hall_dts[which(hall_dts$HALLOWEEN == F),-2], 
                          by = "STATION")

names(station_hall)[6:11] <- c("EN_SHARE_not.mean",
                              "EN_SHARE_not.med",
                              "EN_SHARE_not.sd",
                              "EX_SHARE_not.mean",
                              "EX_SHARE_not.med",
                              "EX_SHARE_not.sd")

station_hall <- left_join(station_hall,
                          hall_dts[which(hall_dts$HALLOWEEN == T),-2], 
                          by = "STATION")

names(station_hall)[12:17] <- c("EN_SHARE_hall.mean",
                              "EN_SHARE_hall.med",
                              "EN_SHARE_hall.sd",
                              "EX_SHARE_hall.mean",
                              "EX_SHARE_hall.med",
                              "EX_SHARE_hall.sd")

station_hall <- station_hall %>%
  mutate(EN_mean_diff = EN_SHARE_hall.mean - EN_SHARE_not.mean,
         EN_med_diff = EN_SHARE_hall.med - EN_SHARE_not.med,
         EN_t = (EN_SHARE_hall.med - EN_SHARE_not.med)/(EN_SHARE_all.sd*sqrt(EN_SHARE_all.n)),
         EX_mean_diff = EX_SHARE_hall.mean - EX_SHARE_not.mean,
         EX_med_diff = EX_SHARE_hall.med - EX_SHARE_not.med,
         EX_t = (EX_SHARE_hall.med - EX_SHARE_not.med)/(EX_SHARE_all.sd*sqrt(EX_SHARE_all.n)))

write.table(station_hall,"clipboard-10240",sep="\t", row.names=FALSE)

station_hall <- station_hall[order(-station_hall$EN_mean_diff),]
topEN <- station_hall[2:11, c("STATION","EN_SHARE_not.mean","EN_SHARE_hall.mean","EN_mean_diff")]
station_hall <- station_hall[order(station_hall$EN_mean_diff),]
botEN <- station_hall[1:10, c("STATION","EN_SHARE_not.mean","EN_SHARE_hall.mean","EN_mean_diff")]
topEN$sign <- "top"
botEN$sign <- "bot"
EN <- rbind(topEN, botEN)


ggplot(data = EN, aes(x = reorder(STATION,EN_mean_diff),
                              y = EN_mean_diff,
                              fill = sign,
                              label = format(EN_mean_diff,digits=4))) +
  #geom_text(nudge_y = -1, color = "black") +
  scale_fill_manual(values = c("top" = "black", "bot" = "orange")) +
  geom_col() +
  coord_flip() +
  labs(y = "% Share Difference Halloween minus non-Halloween",
       x = "STATION") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
        legend.position = "none") +
  scale_y_continuous(labels = percent)

station_hall <- station_hall[order(-station_hall$EX_mean_diff),]
topEX <- station_hall[2:11, c("STATION","EX_SHARE_not.mean","EX_SHARE_hall.mean","EX_mean_diff")]
station_hall <- station_hall[order(station_hall$EX_mean_diff),]
botEX <- station_hall[1:10, c("STATION","EX_SHARE_not.mean","EX_SHARE_hall.mean","EX_mean_diff")]
topEX$sign <- "top"
botEX$sign <- "bot"
EX <- rbind(topEX, botEX)

ggplot(data = EX, aes(x = reorder(STATION,EX_mean_diff),
                      y = EX_mean_diff,
                      fill = sign,
                      label = format(EX_mean_diff,digits=4))) +
  #geom_text(nudge_y = -1, color = "black") +
  scale_fill_manual(values = c("top" = "black", "bot" = "orange")) +
  geom_col() +
  coord_flip() +
  labs(y = "% Share Difference Halloween minus non-Halloween",
       x = "STATION") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
        legend.position = "none") +
  scale_y_continuous(labels = percent)
