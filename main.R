library(RCurl)
library(XML)
library(stringr)
library(tidyverse)

#################################################################################################
#<- we use the functions defined in flixbus_crawler to generate data on the german cities with flixbus connection 
#<- derive data from flixbus on the cities within their connection network
#<- for functions see flixbus_crawler.R
#################################################################################################
flixbus_cities <- get_all_cities()
city_dbs <- adding_city_data(flixbus_cities) #<-derives a list with city level data, the nodes in the travel network




#################################################################################################
# <- merge wikipedia page with data on the population of german cities
#################################################################################################
city_data_url <- "https://de.wikipedia.org/wiki/Liste_der_Gro%C3%9F-_und_Mittelst%C3%A4dte_in_Deutschland" #contains city-level data
page <- getURL(city_data_url)
tpage <- htmlParse(page)


#adapted function from the lab to get the city-level data from wikipedia
extract_info_per_row <- function(tpage, node_nr, column_numbers){ #<- too large parts taken from the lab
  
  node <-  xpathSApply(tpage, paste0("/html/body/div[3]/div[3]/div[4]/div/table[2]/tbody/tr[",node_nr,"]"))[[1]]
  cells <- c()
  for(i in 1:length(column_numbers)){
    cells[i] <- xpathSApply(node, paste0(".//td[",column_numbers[i],"]"), xmlValue)
  }
  
  return(cells)
  
}


city_data <- lapply(1:700, function(x) extract_info_per_row(tpage = tpage, 
                                                        node_nr = x, 
                                                        column_numbers = 1:9))
city_meta_information <- do.call(rbind, city_data)



#################################################################################################
# <- merging data on cities from flixbus and wikipedia
#################################################################################################

city_dbs$V1 <- as.character(city_dbs$V1) 

city_meta_information$V2 <- as.character(city_meta_information$V2)
city_meta_information <- as.data.frame(city_meta_information)
expanded_city_dbs <- dplyr::left_join(city_dbs, city_meta_information, 
                                      by = c("V1" = "V2")) %>% #<- we merge by city name, works perfect except for some smaller cities not listed in wikipedia, which are irrelavent to our research though
                      drop_na() %>% 
                      select(V1, V2, lat, long, city_id, V8, V9)
  
colnames(expanded_city_dbs) <- c("city", "link", "lat", "long", "city_id", "city_size", "county")
expanded_city_dbs$city_size <- as.numeric(gsub(x = expanded_city_dbs$city_size, pattern = "\\.",
             replacement = ""))

#this is the final dataset in this regard containing all cities in the german flixbus network including georeference and city size
save(expanded_city_dbs,
     file="expanded_city_dbs.RData") #because we better make sure

#################################################################################################
# <- now decision to be made to only keep larger cities within our network to reduce
# <- computational workload
#################################################################################################

#a dataset containing all cities larger than 80k which we use for a first investigation and data scraping of 
#flixbus connections
cities_above_80k <- dplyr::filter(expanded_city_dbs, city_size > 80000 )





#################################################################################################
# <- merging with the hot city connection links
#################################################################################################
library(readr)
#this file is a result of the get_city_level_data function in flixbus_crawler.r
#which contains all the listed "popular connections" per city
popular_connections_dbs <- read_csv("popular_connections_dbs.csv", 
                                    col_names = FALSE)
popular_connections_dbs$X1 <- NULL


popular_connections_dbs <- popular_connections_dbs %>% #<- filter for some artifacts
  filter(X2 != "Start") %>% 
  filter(X2 != "V1")
colnames(popular_connections_dbs) <- c("start", "end", "id_generated_from") #<- prper colnames



#################################################################################################
# <- join cities above 80k and popular connections
# <- merging process with expanded_city_dbs here is required to add data on city size and city_id to the popular connections
# <- thus merging happens twice, per each arrival and departure city
#################################################################################################
tmp <- dplyr::left_join(popular_connections_dbs, expanded_city_dbs, by = c("start" = "city"))  %>% # to generate ID of city
       select(start, end, id_generated_from, link, city_id) %>% 
       mutate(start_city_id  = city_id)
tmp <- dplyr::left_join(tmp, expanded_city_dbs, by = c("end" = "city")) %>% 
        mutate(end_city_id = city_id.y) %>% 
       select(start, end, id_generated_from, link.x, start_city_id, end_city_id) 
      
popular_connections_id <- tmp



#################################################################################################
# <- now we would like to download/scrape/collect actually scheduled connections among those popular cities
#################################################################################################

# <- first we reduce the popular connections to only those connecting german cities that arge above 80k citizens
# <- in order to not scrape for decades
popular_german_connections <- dplyr::filter(popular_connections_id, start_city_id %in% as.vector(cities_above_80k$city_id))
popular_german_connections <- dplyr::filter(popular_german_connections, end_city_id %in% as.vector(cities_above_80k$city_id))
popular_german_connections <- dplyr::filter(popular_german_connections, start_city_id != end_city_id)

# <- since the website appears to block this scraping sometimes, I wrote a function to run it
# <- in batches based on the index in the popular connections dataframe, so I can stop
# <- as soon as I get kicked and can resume at this point and later merge all scarping efforts


scrape_connections <- function(start_index){
connections_scraped <<- list()


for(index in seq(start_index,nrow(popular_german_connections))){
  
  print(paste("currently at", index))
singular_connection <- travel_options_crawl(popular_german_connections$start_city_id[index],
                                            popular_german_connections$end_city_id[index],
                                            "06.05.2020") #<- sampled date

connections_scraped <<- list.append(connections_scraped, singular_connection) # <- I use a global variable so I can stop the function
# more flexible but still can keep the results and just cut out those where it started to fail
}
}


#these are respective batches which I needed
#connections_scraped_a <- connections_scraped
#connections_scraped_b <- connections_scraped
#connections_scraped_c <- connections_scraped
#connections_scraped_d <- connections_scraped
#connections_scraped_e <- connections_scraped
#connections_scraped_f <- connections_scraped

save(connections_scraped_a,
     connections_scraped_b,
     connections_scraped_c,
     connections_scraped_d,
     connections_scraped_e,
     connections_scraped_f,
     file="data_raw_scraped_batches.RData") #this is the crawled data on bus connectons on 6.5.2020




#################################################################################################
#<- as the flixbus scraper returns lists with an internal structure, the part below converts the returned lists into dataframes 
#################################################################################################

list_to_dataframe <- function(connections_defined){

connections_dbs_a <- data.frame()
#go through the list merge dataframes
for(index in seq(1, length(connections_defined))){

  if (connections_defined[[index]] == "Bug" | connections_defined[[index]] == "It´s a mess") {
    next
  }
  
  
  #for those where both direct and indirect lines are given
  if(typeof(connections_defined[[index]][[1]]) == "list" & typeof(connections_defined[[index]][[2]]) == "list"){
    try(tmp <- dplyr::bind_rows(connections_defined[[index]][[1]], connections_defined[[index]][[2]]))
    try(connections_dbs_a <- dplyr::bind_rows(connections_dbs_a, tmp))
    tmp <- NULL
    
  }
  
  
  if(typeof(connections_defined[[index]][[1]]) == "list"){
    try(tmp <- dplyr::bind_rows(connections_defined[[index]][[1]]))
    try(connections_dbs_a <- dplyr::bind_rows(connections_dbs_a, tmp))
    tmp <- NULL
  }

  
  if(typeof(connections_defined[[index]][[2]]) == "list"){
    try(tmp <- dplyr::bind_rows(connections_defined[[index]][[2]]))
    try(connections_dbs_a <- dplyr::bind_rows(connections_dbs_a, tmp))
    tmp <- NULL
  }
  
  
}
return(connections_dbs_a)
}





#################################################################################################
# <- using the stored data object, the scraped bus connections need to be sorted 
#################################################################################################

connections_dataframe <- dplyr::bind_rows(list_to_dataframe(connections_scraped_a),
                                          list_to_dataframe(connections_scraped_b),
                                          list_to_dataframe(connections_scraped_c),
                                          list_to_dataframe(connections_scraped_d),
                                          list_to_dataframe(connections_scraped_e),
                                          list_to_dataframe(connections_scraped_f))

save(connections_dataframe, file="data_connections_dataframe.RData") #the dataframe converted from the returned lists


#################################################################################################
# <- cleaning this dataframe
#################################################################################################

#<- for some connections, the flixbus website also suggest travel options on the next day, even when on the selected 
#<- day there are already some connections available, to filter these, the for loop below...

overdue_connections <- c(1)

while(length(overdue_connections) > 0){
overdue_connections <- c()
for(index in seq(2, nrow(connections_dataframe))){
  
  if(connections_dataframe$From[index] == connections_dataframe$From[index-1] &
     connections_dataframe$Destination[index] == connections_dataframe$Destination[index-1] &
     connections_dataframe$Departure[index] < connections_dataframe$Departure[index-1] &
     is.na(connections_dataframe$proposed_date[index]) &
     is.na(connections_dataframe$shift_time[index])){
      overdue_connections <- append(overdue_connections, index)
  }
}

connections_dataframe <- connections_dataframe[-overdue_connections, ]
}

save(connections_dataframe, file="dataframe_cleaned_overdue.RData") #<- save the data





#################################################################################################
# <- fix and clean the time information in the connections dataframe
#################################################################################################
cities <- unique(as.vector(city_dbs$V1))


test <- connections_dataframe %>% 
        mutate(Duration = round(ifelse(grepl(x = Duration,
                                       pattern = "min"),
                                 as.double(gsub(x = Duration, pattern = "min", replacement = ""))/60,
                                 ifelse(is.na(as.double(str_extract(str_extract(Duration, 
                                                                                ":[0-9]{1,2}"), "[0-9]{1,2}"))),
                                        as.double(Duration),
                                        as.double(str_extract(Duration, "[0-9]{1,2}")) +
                                          as.double(str_extract(str_extract(Duration, 
                                                                            ":[0-9]{1,2}"), "[0-9]{1,2}"))/60)),2),
               shift_time = round(ifelse(grepl(pattern = "[0-9]{2}", shift_time),
                                   as.double(shift_time)/60,
                                   as.double(shift_time)),2))



#################################################################################################
# <- we now split the connection data to different pools according to their design and meaning
#################################################################################################
# 1. First split in between those that are on the searched day and precise locations (primary connections)
# and those which have either differing locations or a different date (secondary connections)


primary_connections <- dplyr::filter(test, is.na(test$proposed_date)) %>% 
  select(From, Destination, Departure, Arrival, Price, Duration, shift_time, shift_location) %>% 
  dplyr::left_join(expanded_city_dbs, by = c("From" = "city_id")) %>% 
  mutate(from_long = long,
         from_lat  = lat,
         from_city_name  = city,
         from_city_size  = city_size) %>% 
  dplyr::left_join(expanded_city_dbs, by = c("Destination" = "city_id")) %>% 
  mutate(destination_long = long.y,
         destination_lat  = lat.y,
         destination_city_name  = city.y,
         destination_city_size  = city_size.y) %>% 
  select(From, Destination, Departure, Arrival, Price, Duration, shift_time, shift_location,
         from_long, from_lat, from_city_name, from_city_size,
         destination_long, destination_lat, destination_city_name, destination_city_size)
  

primary_connections <- distinct(primary_connections) 

save(primary_connections, file="primary_connections_data.RData") #<- this is gonna be the main dataset for analysis of bus connections


secondary_connections <- dplyr::filter(test, !is.na(test$proposed_date))



#################################################################################################
# using the primary connections, we calculate a link intensity based on the number of 
# scheduled bus lines for each city-to-city connection
#################################################################################################





link_intensity <- primary_connections %>% 
  distinct() %>% 
  count(From, Destination) 


primary_connections_intensity <- dplyr::left_join(primary_connections, link_intensity,
                                                  by = c("From" = "From", "Destination" = "Destination"))



#now primary_connections_intensity basicaly contains a count for each city-to-city connection link 
#describing the number of scheduled lines on the selected date



#we break this dataset further down to bus connections leaving the "megacities" and arriving in cities with >300.000 people
#to deal with a more reasonable dataset to scrape for connections from the train website
primary_connections_intensity_leaving_big_4 <-  dplyr::filter(primary_connections_intensity, from_city_size > 1000000 &
                                                                destination_city_size > 300000) %>% 
  distinct() 




#recoding some of the umlaute-cities (shitty task but figured out now better one)
primary_connections_intensity_leaving_big_4[primary_connections_intensity_leaving_big_4=="DÃ¼sseldorf"]<- "Duesseldorf"
primary_connections_intensity_leaving_big_4[primary_connections_intensity_leaving_big_4=="NÃ¼rnberg"]<- "nuernberg"
primary_connections_intensity_leaving_big_4[primary_connections_intensity_leaving_big_4=="KÃ¶ln"]<- "koeln"
primary_connections_intensity_leaving_big_4[primary_connections_intensity_leaving_big_4=="MÃ¼nchen"]<- "muenchen"
primary_connections_intensity_leaving_big_4[primary_connections_intensity_leaving_big_4=="MÃ¼nster"]<- "muenster"







#routine to fix the bug in the durations
#-> first convert arrival and departure time to decimal system
#-> thus we convert departure and arrival time to decimal system and based on the difference calculate the travel duration
primary_connections_intensity_leaving_big_4 <- primary_connections_intensity_leaving_big_4 %>% 
  mutate(departure_minutes_dec  = as.double(gsub(x = Departure,
                                                 pattern = "([0-9]{1,2}):([0-9]{1,2})",
                                                 replacement = " \\2"))/60,
         
         
         depature_hours  = as.double(gsub(x = gsub(Departure,
                                                   pattern = "\n",
                                                   replacement = ""),
                                          pattern = "([0-9]{1,2}):([0-9]{1,2})",
                                          replacement = " \\1")),
         depature = round(depature_hours + departure_minutes_dec,2), 
         
         
         
         arrival_minutes_dec  = as.double(gsub(x = Arrival,
                                               pattern = "([0-9]{1,2}):([0-9]{1,2})",
                                               replacement = " \\2"))/60,
         
         
         arrival_hours  = as.double(gsub(x = gsub(Arrival,
                                                  pattern = "\n",
                                                  replacement = ""),
                                         pattern = "([0-9]{1,2}):([0-9]{1,2})",
                                         replacement = " \\1")),
         arrival = round(arrival_hours + arrival_minutes_dec,2),
         duration = ifelse(depature > arrival, 
                           (24- depature) + arrival,
                           arrival - depature)) %>% 
  select(From, Destination, Departure, Arrival, depature, arrival, duration, depature_hours,
         Price, shift_time, shift_location, from_long, from_lat, from_city_size, from_city_name,
         destination_long, destination_lat, destination_city_name, destination_city_size)


save(primary_connections_intensity_leaving_big_4,
     file = "primary_connections_intensity_leaving_big_4.RData") #<- dataset contains information on bus connections leaving megacities to
#cities with >300.000 population








#################################################################################################
# <- TRAIN DATA SCRAPING
# <- using the crawler in german_train_crawler.R
# -> we use the scraper here to scrape the primary popular connections leaving the four cities koeln
# -> hamburg, berlin, muenchen (i.e. the megacities)
#################################################################################################

train_connection_db <- data.frame()
for(index in seq(1, nrow(primary_connections_intensity_leaving_big_4))){
  
  test <- scrape_connection("06.05.2020", "1:00", 
                            primary_connections_intensity_leaving_big_4$from_city_name[index], 
                            primary_connections_intensity_leaving_big_4$destination_city_name[index],
                            primary_connections_intensity_leaving_big_4$From[index],
                            primary_connections_intensity_leaving_big_4$Destination[index])
  
  train_connection_db <- dplyr::bind_rows(train_connection_db, test)
  
}




save(train_connection_db,
     file="raw_scraped_train_data.RData") #because we better make sure

#################################################################################################
# -> train connection data cleaning
# -> this part below converts the time data to decimal system
#################################################################################################

train_connection_db_cleaned <- train_connection_db %>% 
  
  mutate(normal_price = gsub(x = str_extract(normal_price, "[0-9]{1,3},[0-9]{2}"), pattern = ",",
                             replacement = "."),
         saver_price = gsub(x = str_extract(saver_price, "[0-9]{1,3},[0-9]{2}"), pattern = ",",
                            replacement = "."),
         arrival_time = gsub(x = arrival_time, pattern = ":",
                             replacement="."),
         duration_minutes_dec  = as.double(gsub(x = gsub(duration,
                                                         pattern = "\n",
                                                         replacement = ""),
                                                pattern = "([0-9]{1,2}):([0-9]{1,2})",
                                                replacement = " \\2"))/60,
         duration_hours  = as.double(gsub(x = gsub(duration,
                                                   pattern = "\n",
                                                   replacement = ""),
                                          pattern = "([0-9]{1,2}):([0-9]{1,2})",
                                          replacement = " \\1")),
         duration_dec = round(duration_hours + duration_minutes_dec,2)) %>% 
  select(departure_time, arrival_time, normal_price, saver_price,
         change_time, number_changes, date, departure_city, arrival_city, duration_dec,
         departure_city_id, arrival_city_id)


save(train_connection_db_cleaned,
     file="scraped_train_data.RData") #because we better make sure

#################################################################################################
# -> now we are left with a clean dataset of train and bus connections on the selected day
# -> Voilá 
#################################################################################################


#################################################################################################
# -> using google API to calculate distances in kilometer and driving time as a baseline
#################################################################################################
google_api_key <- "please enter your bank account details here"

get_google_api_data <- function(departure_city, destination_city, google_api_key){
  url <- paste0("https://maps.googleapis.com/maps/api/distancematrix/xml?origins=",
                departure_city,
                "&destinations=",
                destination_city,
                "&mode=car&language=fr-FR&key=",
                google_api_key)
  
  page <- getURL(url)
  tpage <- htmlParse(page)
  
  driving_time <- round(as.double(xpathSApply(tpage, "//duration/value", xmlValue))/60/60,2)
  distance <- round(as.double(xpathSApply(tpage, "//distance/value", xmlValue))/1000,2)  
  return(c(distance, driving_time))
}


#we generate a table with city name and city ID to pass the API for merging later from the
#train connection dataset
connection_google_data <- train_connection_db_cleaned %>% 
  select(departure_city, arrival_city, departure_city_id, arrival_city_id) %>% 
  distinct()

for(index in seq(1, nrow(connection_google_data))){
  google_return <- get_google_api_data(connection_google_data$departure_city[index],
                                       connection_google_data$arrival_city[index],
                                       google_api_key)
  
  connection_google_data$distance[index] <- google_return[1]
  connection_google_data$driving_time[index] <- google_return[2]
}

save(connection_google_data, file="google_connection_data.RData") #<- this is gonna be the main dataset for analysis of bus connections




#  
#  
# 
#   __       _______ .___________.     ______    __    __  .______         .______        ______   .______     ______   .___________.    _______.
#  |  |     |   ____||           |    /  __  \  |  |  |  | |   _  \        |   _  \      /  __  \  |   _  \   /  __  \  |           |   /       |
#  |  |     |  |__   `---|  |----`   |  |  |  | |  |  |  | |  |_)  |       |  |_)  |    |  |  |  | |  |_)  | |  |  |  | `---|  |----`  |   (----`
#  |  |     |   __|      |  |        |  |  |  | |  |  |  | |      /        |      /     |  |  |  | |   _  <  |  |  |  |     |  |        \   \    
#  |  `----.|  |____     |  |        |  `--'  | |  `--'  | |  |\  \----.   |  |\  \----.|  `--'  | |  |_)  | |  `--'  |     |  |    .----)   |   
#  |_______||_______|    |__|         \______/   \______/  | _| `._____|   | _| `._____| \______/  |______/   \______/      |__|    |_______/    
#     _______   ______           _______. __    __  .______       _______  __  .__   __.   _______                                                
#    /  _____| /  __  \         /       ||  |  |  | |   _  \     |   ____||  | |  \ |  |  /  _____|                                               
#   |  |  __  |  |  |  |       |   (----`|  |  |  | |  |_)  |    |  |__   |  | |   \|  | |  |  __                                                 
#   |  | |_ | |  |  |  |        \   \    |  |  |  | |      /     |   __|  |  | |  . `  | |  | |_ |                                                
#   |  |__| | |  `--'  |    .----)   |   |  `--'  | |  |\  \----.|  |     |  | |  |\   | |  |__| |                                                
#    \______|  \______/     |_______/     \______/  | _| `._____||__|     |__| |__| \__|  \______|                                                
# 
# 
# 
#  Found at patagonia.com/robots.txt
# 



