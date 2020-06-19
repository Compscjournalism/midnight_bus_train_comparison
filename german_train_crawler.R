library(RSelenium)
library(XML)
library(stringr)
library(tidyverse)
#################################################################################################
# <- Algorithm procedure to scrape Deutsche Bahn
# <- Open the website for the searched connection starting at 1am
# <- Use Selenium to show as many entries as possible (mostly around 12/13 connections)
# <- scrape these available
# <- take the last departure time found and use it in the build_url function to load later connections
# <- again use selenium to show as many entries as possible
# <- scrape displayed entries, repeat until midnight is reached
#################################################################################################



#' Head-method for scraping train connection data for a selected day, time, destination and departure-city
#'
#' @param day 
#' @param time 
#' @param from 
#' @param destination 
#' @param from_id 
#' @param destination_id 
#'
#' @return dataframe with found connections on that particular day
#'
#' @examples scrape_connection("06.05.2020", "01:00", "koeln", "frankfurt", "21", "112")
scrape_connection <- function(day, time, from, destination, from_id, destination_id){
  
  
  #establish remote driver for RSelinium
  remDr <- rsDriver(verbose = T,
                    remoteServerAddr = "localhost",
                    port = 4443L,
                    browser=c("firefox"))
  rm <- remDr$client
  rm$getStatus()
  
  
  
  initial_url <- build_url(day, time, from, destination) #create the first search-url searching for a connection, usually gets passed 1am as time parameter
  expanded_first_page <- advance_table(initial_url, remDr, rm) #method uses selenium to show more connections displayed on the website
  connections <- scrape_entries(expanded_first_page) #having all connections displayed, here I scrape these entries for train-level data

  connections <- connections[-1,] #<-remove the header here  

  #repeat the above described scheme as long as the connections scraped are increasing in departure time --> stops when jumping from e.g. 23:15
  # to 2am in the morning
  while(all(diff(as.double(as.character(connections$V1))) >= -4)){ 
    #use the last departure time found on the previous page to start searching for next train connections
    next_start_time <- gsub(x = connections$V1[nrow(connections)],
                            pattern = "\\.",
                            replacement = ":")
    #following the scheme described for the first page
    continue_url <- build_url(day, next_start_time, from, destination)
    expanded_second_page <- advance_table(continue_url, remDr, rm)
    connections_second_page <- scrape_entries(expanded_second_page)
    
    connections <- dplyr::bind_rows(connections, connections_second_page) %>% 
      dplyr::filter(V1 != "departure") 
    

    
  }
  
    #data cleaning
  connections <- connections %>% 
                 distinct(V1, V2, .keep_all = TRUE) %>% 
    mutate(date = day,
           departure_city = from,
           arrival_city = destination,
           departure_city_id = from_id,
           arrival_city_id = destination_id)
  
  colnames(connections) <- c("departure_time", "arrival_time", "duration", "normal_price", "saver_price",
                             "departure_time_control", "arrival_time_control", "change_time", "number_changes",
                             "date", "departure_city", "arrival_city", "departure_city_id", "arrival_city_id")
  
  #closing remove driver
  rm$close()
  rm(remDr)
  rm(rm)
  gc()
  return(connections)
  }








#' Create query URL link
#'
#' Given the search parameters listed below, creates a link which is valid for the bahn.de website to search for respectively
#' defined train connections
#'
#' @param date 
#' @param time 
#' @param from 
#' @param destination 
build_url <- function(date, time, from, destination){
  
  url <- paste0("https://reiseauskunft.bahn.de/bin/query.exe/dn?revia=yes&existOptimizePrice-deactivated=1&country=DEU&dbkanal_007=L01_S01_D001_KIN0001_qf-bahn-svb-kl2_lz03&start=1&protocol=https%3A&S=",
                from,
                "&REQ0JourneyStopsSID=&Z=",
                destination,
                "&REQ0JourneyStopsZID=&date=Mi%2C+",
                date,
                "&time=",
                time,
             "%3A00&timesel=depart&returnDate=&returnTime=&returnTimesel=depart&optimize=0&auskunft_travelers_number=1&tariffTravellerType.1=E&tariffTravellerReductionClass.1=0&tariffClass=2&rtMode=DB-HYBRID&externRequest=yes&HWAI=JS%21js%3Dyes%21ajax%3Dyes%21")
                
  return(url)
  }




#' Expand table entries
#'
#' Method uses Selenium to expand a table by clicking several times on the respective button to allow a dynamic (AJAX) 
#' loading process of additional train connections
#' @param url which specifies the page with the train connections table
#' @param remDr Selenium specific 
#' @param rm Selenium specific
#'
#' @return returns code from page with loaded additional train connection content
advance_table <- function(url, remDr, rm){
rm$navigate(url) 
# loop three times over the button to show more entries
for(i in seq(1,3)){ #<- click the button 3x, which is the max. possible value
rm$findElement(using = "xpath", 
               '//a[@class="buttonGreyBg later"]')$clickElement()
}

page <- unlist(rm$getPageSource())
return(page)
}












#' Scrape connection data
#'
#'Given the raw page html/code, method draws data on displayed train connections
#'
#' @param page the html doc holding train connections
#'
#' @return return train connection data as a dataframe
#' @export
#'
#' @examples
scrape_entries <- function(page){

tpage <- htmlParse(page)

#<- test holds then a list with the tree-structure parts representing each connection
test <- xpathSApply(tpage, "//tbody[@class='boxShadow  scheduledCon']") 
#define the dataframe layout/variables we wish to scrape and store later in that particular dataframe
connections <- c("departure", "arrival", "duration", "casual price", "savers_price")
# for each entry in test, representing a single connection, we use this for loop to scrape relevant information
for(index in seq(1, length(test))){


departure <- xpathSApply(test[[index]], ".//td[@class='time']", xmlValue)[1] #<- for the departure algorithm I clean departure value here
departure <- str_extract(departure, "[0-9]{1,2}:[0-9]{1,2}")
departure <- gsub("(0?)([0-9]{1,2}):([0-9]{1,2})", "\\2 . \\3",  x = departure)
departure <- (gsub(x = departure, pattern = " ", replacement = ""))



arrival <- xpathSApply(test[[index]], ".//td[@class='time']", xmlValue)[2]
arrival <- str_extract(arrival, "[0-9]{1,2}:[0-9]{1,2}")
arrival <- gsub("(0?)([0-9]{1,2}):([0-9]{1,2})", "\\2:\\3",  x = arrival)





duration <- as.character(xpathSApply(test[[index]], ".//td[@class='duration lastrow']",
                        xmlValue))
casual_price <- as.character(xpathSApply(test[[index]], ".//td[@class='fareStd lastrow button-inside tablebutton']//span[@class='fareOutput']",
                            xmlValue))
savers_price <- as.character(xpathSApply(test[[index]], ".//td[@class='farePep lastrow button-inside tablebutton borderright']//span[@class='fareOutput']",
                            xmlValue))
connections <- rbind(connections, c(departure, arrival, duration, casual_price, savers_price))
}



# data on the number of times the train has to be changed during the connection, and the particular waiting time involved is stored elsewhere
# thus the following part scrapes relevant data on that


#this information is for each connection stored in a javascript type block, thus
# I search for all javascript elements and select those containing the relevant keyword ("reiseabschnitt") indicating the 
# occurence of the data I´m looking for
elements <- xpathSApply(tpage, "//script[@type='text/javascript']", xmlValue)
analyzed_blocks <- elements[grepl("reiseabschnitt", elements)]

change_times <- c("departure", "arrival", "change_time", "number_changes")

#loop through those javascript blocks holding relevant data and scrape data
#to double check, I also scrape data on departure and arrival-time here so I can be sure to match to the respective train connections
#scraped in the part of this function above
for(elem in analyzed_blocks){
  test_departure <- str_extract(elem, "verbindung.abfahrt.*T[0-9]{1,2}:[0-9]{1,2}")
  test_departure <- str_extract(test_departure, "(0?)([0-9]{1,2}):([0-9]{1,2})")
  test_departure <- gsub("(0?)([0-9]{1,2}):([0-9]{1,2})", "\\2.\\3",  x = test_departure)
  
  
  test_arrival <- str_extract(elem, "verbindung.ankunft.*T[0-9]{1,2}:[0-9]{1,2}")
  test_arrival <- str_extract(test_arrival, "(0?)([0-9]{1,2}):([0-9]{1,2})")
  
  
  number_changes <- str_extract_all(elem, "verbindung.umstiege = [0-9]") 
  number_changes <- as.double(str_extract(number_changes, "[0-9]"))
  
  change_time <- 0
  
  if(number_changes > 0){ #if there are changes of trains required, scrape the time it takes to change them i.e. waiting time
  change_time <- str_extract_all(elem, "reiseabschnitt.aufenthaltszeit = \"[0-9]{1,2}:[0-9]{1,2}")
  change_time <- str_extract(as.vector(change_time[[1]]), "[0-9]{1,2}:[0-9]{1,2}")
  change_time <- sum(as.double(gsub(change_time, pattern = ":", replacement = ".")))
  
  }
 change_times <- rbind(change_times, c(test_departure, test_arrival, change_time, number_changes))
}

#finally I bind the data on changing times to the previously created data on the connections
connections <- cbind(connections, change_times)
return(as.data.frame(connections))
}


