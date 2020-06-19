library(RCurl)
library(XML)
library(stringr)

#################################################################################################
#<- This script contains the methods used to scrape the Flixbus.de website for both the list of 
#<- cities and popular connections as well as the actually scheduled connections for a particular day
#################################################################################################


#store popular connections in a global variable
popular_connections_database <<- data.frame()






#' getting all the cities involved in the network
#'
#' @return dataframe of all cities in Germany connected to Flixbus network
get_all_cities <- function(){
url <- "https://www.flixbus.de/fernbus/deutschland"
page <- getURL(url)
tpage <- htmlParse(page)
#location/ city name
xpathSApply(tpage, "/html/body/section[2]/div[2]/div/div[2]/div/div/div[1]/div[2]/div[1]/ul/li[1]/a", xmlValue)
#link/ the link to the city-specifc page e.g. https://www.flixbus.de/fernbus/aichach
xpathSApply(tpage, "/html/body/section[2]/div[2]/div/div[2]/div/div/div[1]/div[2]/div[1]/ul/li[1]/a", xmlAttrs)
#all location names
test <- cbind(xpathSApply(tpage, "/html/body/section[2]/div[2]/div/div[2]/div/div//a", xmlValue), 
              xpathSApply(tpage, "/html/body/section[2]/div[2]/div/div[2]/div/div//a", xmlAttrs))


test <- as.data.frame(test)
return(test)
}




#' City-level data
#' Method scrapes the city-specific page of a city and generates data on the city ID, geo-reference and popular connections
#' of that city (city link e.g. https://www.flixbus.de/fernbus/bochum)
#' @param url_bayreuth 
#'
#' @return
get_city_level_data <- function(url_bayreuth){

  mycurl <- getCurlHandle()
curlSetOpt(cookiejar= "~/Rcookies", curl = mycurl)

rawpage <- getURL(url_bayreuth, 
                    curl= mycurl,
                    useragent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                    timeout = 60, 
                    followlocation = TRUE)  
  tpage_bayreuth <- htmlParse(rawpage)
#number in doubled square brackets is the index of element to assess


# getting the cityID and locations which are stored in a javascript, luckily the javascript with relevant information is always on 9th 
# place among all js elements, otherwise we would´ve needed to check for all
tmp <- xpathSApply(tpage_bayreuth, "//script", xmlValue)[[9]]
city_id_row <- str_extract(tmp, "city1Id: \"[0-9]{1,3}")
city_id <- as.numeric(str_extract(city_id_row, "[0-9]{2,3}"))

# geo_locations
city_lat <- gsub(x = str_extract(tmp, "lat: [0-9]{1,2}\\.[0-9]{1,9}"), pattern = "lat: ", replacement = "" )
city_long <-gsub(x = str_extract(tmp, "lon: [0-9]{1,2}\\.[0-9]{1,9}"), pattern = "lon: ", replacement = "")


#the node storing popular connections
popular_connections <- xpathSApply(tpage_bayreuth, "//a[@class='popular-connection__links__item__connection']")

#loop through that node, if there are popular connections found, and store the popular connections as a tupel
if(length(popular_connections) > 0){
  travel_tupel_basic <- c("Start", "End")
  
  
  for(index in seq(1, length(popular_connections))){
    
    travel_tupel <- xpathSApply(popular_connections[[index]], "./div/div", xmlValue)
    
    travel_tupel_basic <- rbind(travel_tupel_basic, travel_tupel)
    
  }
  
  city_favorable_connections <- as.data.frame(travel_tupel_basic)
  city_favorable_connections$generated_from <- city_id 
  #add popular connections per city (the .csv file is once created manually)
  popular_connections_database <- dplyr::bind_rows(popular_connections_database,
                                                   city_favorable_connections)
  
}  

#writeLines(rawpage, paste0(getwd(), city_id, "citypage.html"))
return(list(city_id, c(city_lat, city_long)))
}





# adding city data to the overall frame



#' Add city-level data
#'
#'Method uses the list of city names create in the function get_all_cities and applies the
#'get_city_level data to merge both city names and data for the cities
#' @param test 
#'
#' @return
#' @export
#'
#' @examples
adding_city_data <- function(test){
  pb = txtProgressBar(min = 0, max = nrow(test), initial = 0) 
  
  favorite_connections <- data.frame()
for(i in seq(1, nrow(test))){

  setTxtProgressBar(pb,i)
  
  
  scraped_info <- get_city_level_data(test$V2[i])
  #handling the returned list from get_city_level_data to be fitted to the dataframe format
  test$lat[i] <- scraped_info[[2]][1]
  test$long[i] <- scraped_info[[2]][2]
  test$city_id[i] <- scraped_info[[1]]
  
}
return(test_entire_complete)
}






#' Scraping bus connection data
#' For a defined connection (from, destination and date) this method generates a dataframe holding all
#' scheduled bus connections from the flixbus website
#'
#' @param from 
#' @param destination 
#' @param date 
#'
#' @examples https://shop.flixbus.de/search?departureCity=309&arrivalCity=88&rideDate=27.04.2020
travel_options_crawl <- function(from, destination, date){
  
 
    #create the URL structure where to find the scheduled connection data  
   travel_page_string <- paste("https://shop.flixbus.de/search?departureCity=",
                       from, 
                       "&arrivalCity=",
                       destination,
                       "&rideDate=",
                       date,
                       sep = "")
  
  #section to get the HTML doc
  mycurl <- getCurlHandle()
  
  curlSetOpt(cookiejar= "~/Rcookies", curl = mycurl)
  
  rawpage <- getURL(travel_page_string, 
                    curl= mycurl,
                    useragent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                    timeout = 60, 
                    followlocation = TRUE)  
  travel_page <- htmlParse(rawpage)
  
  


tmp <- xpathSApply(travel_page,
                   "//div[@id='search-result-direct']//div[@class='b col-xs-12 rides-list currency-eur']//div[@data-transport-type='bus']")

#create the daframe layout for direct and indirect connections (which are stored seperately)
direct_connections_dataframe <- c("departure", "arrival","price_edited", "duration")
indirect_connections_dataframe <- c("departure", "arrival","price_edited", "duration", "shift_duration", "shift_location")


#if the page does not contain any connection information, sth. wen´t wrong (which occured in 0.5% of all scraped connections)
website_bug <- xpathSApply(travel_page, "/html/body/div[1]/section[2]/div/div/div[1]/div[2]/div/p", xmlValue)

if(typeof(website_bug) == "character"){
  
  if(grepl("Etwas ist schief gelaufen", website_bug)){ #"Etwas ist schief gelaufen" here is the error message displayed by flixbus
    #usually returned when the website blocked the scraping process (thus see city_data_crawler why crawling is made in batches)
    return("Bug")
  }
}




#we use these booleans later, for default we set them to FALSE
direct_line_present <- FALSE
indirect_line_present <- FALSE


#if we observe bus connections on the website (length(tmp) > 0) then go through each listed connection stored in tmp and derive the data
if(length(tmp) > 0){
for(index in seq(1, length(tmp))){
  
  

travel_data_table <- xpathSApply(tmp[[index]], ".//div[@class='col-xs-12 ride-stations']//tbody")[[1]]
#get departure time
departure_raw <- xpathSApply(travel_data_table, "./tr/td/div[@class='flix-connection__time departure-time']", xmlValue)
departure <- gsub(x = departure_raw, pattern = "\n", "")
departure <- gsub(x = departure, pattern = " ", "")

#get arrival time
arrival_raw <- xpathSApply(travel_data_table, "./tr/td/div[@class='flix-connection__time']", xmlValue)
arrival <- gsub(x = arrival_raw, pattern = "\n", "")
arrival <- gsub(x = arrival, pattern = " ", "")

#get price
price_raw <- xpathSApply(tmp[[index]], ".//span[@class='num currency-small-cents']", xmlValue)[[1]]
price_edited <- as.double(gsub(x = str_extract(price_raw, "[0-9]{1,3},[0-9]{1,3}"), pattern = ",", replacement = "."))

#duration
duration_raw <- xpathSApply(tmp[[1]], ".//div[@class='col-xs-12 duration ride__duration ride__duration-messages']", 
                        xmlValue)[[1]]
duration <- gsub(x = duration_raw, pattern = "\n", "")
duration <- gsub(x = duration, pattern = " ", "")
duration <- (gsub(x = duration, pattern = "Std.", ""))



#check if change of bus line is required

# if this element exists, then there is a change of line
change_of_lines <- ifelse(length(xpathSApply(tmp[[index]], ".//div[contains(@id, 'transf-num-popup-interconnection-')]")) > 0,
                          TRUE, FALSE)

#depending on the binary value of change_of_lines we then either check for the change of line or not
if(!change_of_lines){
  direct_line_present <- TRUE
  direct_connections_dataframe <-  rbind(direct_connections_dataframe, c(departure, arrival,price_edited, duration))
}
  


if(change_of_lines){
  indirect_line_present <- TRUE
  
  waiting_time_raw <- xpathSApply(tmp[[index]], ".//div[contains(@id, 'transf-num-popup-interconnection-')]/span//div[@class='light-gray']", xmlValue)[[1]]
  waiting_time <- as.numeric(str_extract(waiting_time_raw, "[0-9]{1,3}"))
  location_change <- xpathSApply(tmp[[index]], ".//div[contains(@id, 'transf-num-popup-interconnection-')]/span/div", xmlValue)[[1]]
  
  indirect_connections_dataframe <- rbind(indirect_connections_dataframe, c(departure, arrival,price_edited, duration,
                                                                            waiting_time, location_change))
   
  
  }




}
  
  #bind the results from the loop scraping data together to one dataframe, again differentiating direct and indirect lines
  direct_connections <- direct_connections_dataframe
  if(direct_line_present){
  
  #edit the direct connections saved
  direct_connections <- as.data.frame(direct_connections, 
                                      col.names = c("departure", "arrival","price_edited", "duration"),
                                      row.names = NULL)
  direct_connections <- direct_connections[-1,]
  direct_connections$from <- from
  direct_connections$destination <- destination
  direct_connections$date <- date
  colnames(direct_connections) <- c("Departure", "Arrival", "Price", "Duration", "From", "Destination",
                                    "Date")
  
  }
  if(!direct_line_present){
    direct_connections <- "no direct lines found"
  }
  
  
  indirect_connections <- indirect_connections_dataframe
  if(indirect_line_present){
  
  indirect_connections <- as.data.frame(indirect_connections)
  indirect_connections <- indirect_connections[-1,]
  indirect_connections$from <- from
  indirect_connections$destination <- destination
  indirect_connections$date <- date
  colnames(indirect_connections) <- c("Departure", "Arrival", "Price", "Duration",
                                    "shift_time", "shift_location",
                                    "From", "Destination",
                                    "Date")
  }
  
  if(!indirect_line_present){
    indirect_connections <- "no indrect lines found"
  }
  
  
  
  #we return dataframes for direct and indirect bus connections seperately
  
  return(list(direct_connections, indirect_connections))

}



#################################################################################################
#<- this section handles when there are no connections on the searched day
#<- Flixbus displays a link to alternative connections which the crawler follows and in a similar way
#<- as displayed above scrapes the alternative connections
#################################################################################################

new_link <- xpathSApply(travel_page, '/html/body/div[1]/section[2]/div/div/div[1]/div[2]/div/div[2]/div[1]/div/div[3]/div/div/div/div[2]/ul/li/a', xmlAttrs)[2]
new_link <- paste0("https://shop.flixbus.de/", new_link)
page <- getURL(new_link)

travel_page <- htmlParse(page)
#################################################################################################
#<- basically now the preceding script can be insert to handle the new page in the exact same way
#################################################################################################




tmp <- xpathSApply(travel_page,
                   "//div[@id='search-result-direct']//div[@class='b col-xs-12 rides-list currency-eur']//div[@data-transport-type='bus']")



direct_connections_dataframe <- c("departure", "arrival","price_edited", "duration")
indirect_connections_dataframe <- c("departure", "arrival","price_edited", "duration", "shift_duration", "shift_location")




direct_line_present <- FALSE
indirect_line_present <- FALSE


if(length(tmp) > 0){
  
  for(index in seq(1, length(tmp))){
    
    
    travel_data_table <- xpathSApply(tmp[[index]], ".//div[@class='col-xs-12 ride-stations']//tbody")[[1]]
    #get departure time
    departure_raw <- xpathSApply(travel_data_table, "./tr/td/div[@class='flix-connection__time departure-time']", xmlValue)
    departure <- gsub(x = departure_raw, pattern = "\n", "")
    departure <- gsub(x = departure, pattern = " ", "")
    
    #get arrival time
    arrival_raw <- xpathSApply(travel_data_table, "./tr/td/div[@class='flix-connection__time']", xmlValue)
    arrival <- gsub(x = arrival_raw, pattern = "\n", "")
    arrival <- gsub(x = arrival, pattern = " ", "")
    
    #get price
    price_raw <- xpathSApply(tmp[[index]], ".//span[@class='num currency-small-cents']", xmlValue)[[1]]
    price_edited <- as.double(gsub(x = str_extract(price_raw, "[0-9]{1,3},[0-9]{1,3}"), pattern = ",", replacement = "."))
    
    #duration
    duration_raw <- xpathSApply(tmp[[1]], ".//div[@class='col-xs-12 duration ride__duration ride__duration-messages']", 
                                xmlValue)[[1]]
    duration <- gsub(x = duration_raw, pattern = "\n", "")
    duration <- gsub(x = duration, pattern = " ", "")
    duration <- (gsub(x = duration, pattern = "Std.", ""))
    
    
    #for some alternatively proposed connections the departure city and/or arrival city might vary (as it suggest a 
    #city which is spatially close to the destination or depature city)
    departure_city <- xpathSApply(travel_data_table, ".//div[@class='station-name-label']", xmlValue)[1]
    arrival_city <- xpathSApply(travel_data_table, ".//div[@class='station-name-label']", xmlValue)[2]
    
    
    
    
    #check if the proposed ones are on a new day
    
    proposed_date <- str_extract(new_link, "[0-9]{2}\\.[0-9]{2}\\.2020")
    
    
    #check if change of bus line is required
    
    change_of_lines <- ifelse(length(xpathSApply(tmp[[index]], ".//div[contains(@id, 'transf-num-popup-interconnection-')]")) > 0,
                              TRUE, FALSE)
    
    if(!change_of_lines){
      direct_line_present <- TRUE
      direct_connections_dataframe <-  rbind(direct_connections_dataframe, c(departure, arrival,price_edited, duration,
                                                                             departure_city, arrival_city, proposed_date))
    }
    
    
    
    if(change_of_lines){
      indirect_line_present <- TRUE
      
      waiting_time_raw <- xpathSApply(tmp[[index]], ".//div[contains(@id, 'transf-num-popup-interconnection-')]/span//div[@class='light-gray']", xmlValue)[[1]]
      waiting_time <- as.numeric(str_extract(waiting_time_raw, "[0-9]{1,3}"))
      location_change <- xpathSApply(tmp[[index]], ".//div[contains(@id, 'transf-num-popup-interconnection-')]/span/div", xmlValue)[[1]]
      
      indirect_connections_dataframe <- rbind(indirect_connections_dataframe, c(departure, arrival,price_edited, duration,
                                                                                departure_city, arrival_city,
                                                                                proposed_date,
                                                                                waiting_time, location_change))
      
      
    }
    
    
    
    
  }
  
  direct_connections <- direct_connections_dataframe
  if(direct_line_present){
    
    #edit the direct connections saved
    direct_connections <- as.data.frame(direct_connections, 
                                        col.names = c("departure", "arrival","price_edited", "duration", 
                                                      "departure_city", "arrival_city", "proposed_date"),
                                        row.names = NULL)
    direct_connections <- direct_connections[-1,]
    direct_connections$from <- from
    direct_connections$destination <- destination
    direct_connections$date <- date
    colnames(direct_connections) <- c("Departure", "Arrival", "Price", "Duration",
                                      "departure_city", "arrival_city", 
                                      "proposed_date",
                                      "From", "Destination",
                                      "Date")
    
  }
  if(!direct_line_present){
    direct_connections <- "no direct lines found"
  }
  
  
  indirect_connections <- indirect_connections_dataframe
  if(indirect_line_present){
    
    indirect_connections <- as.data.frame(indirect_connections)
    indirect_connections <- indirect_connections[-1,]
    indirect_connections$from <- from
    indirect_connections$destination <- destination
    indirect_connections$date <- date
    colnames(indirect_connections) <- c("Departure", "Arrival", "Price", "Duration",
                                        "departure_city", "arrival_city", 
                                        "proposed_date",
                                        "shift_time", "shift_location",
                                        "From", "Destination",
                                        "Date")
  }
  
  if(!indirect_line_present){
    indirect_connections <- "no indrect lines found"
  }
  
  
  
  
  
  return(list(direct_connections, indirect_connections))
  
}

#################################################################################################
#<- if no alternative connections are found, this is where we end up. Approximately, the data collection
#<- process showed, around 2% of connections requested are "a mess" and thus no scheduled bus lines found
#################################################################################################
print("It´s a mess")




}









