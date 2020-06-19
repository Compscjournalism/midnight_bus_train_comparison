




#################################################################################################
#->for the analysis we draw on 3 datasets wich are relevant in this context and generated
#->within the city_data_crawler script. 
#-> for comparison of train and bus data we use the datasets respectively which are reduced
#-> to connections leaving the four megacities Koeln, Berlin, Hamburg, Muenchen and arriving
#-> in cities with a population larger than 300.000
load("expanded_city_dbs.RData")
load("scraped_train_data.RData")
load("primary_connections_intensity_leaving_big_4.RData")
load("google_connection_data.RData")
library(ggmap)
library(tidyverse)
library(XML)
#################################################################################################




#################################################################################################
# -> Creating visuals
#################################################################################################
cities_above_80k <- dplyr::filter(expanded_city_dbs, city_size > 80000 )
cities_above_80k <-  cities_above_80k %>% 
  mutate(long = as.double(long),
         lat = as.double(lat))

primary_connections_intensity_leaving_big_4[primary_connections_intensity_leaving_big_4=="koeln"]<- "Koeln"
primary_connections_intensity_leaving_big_4[primary_connections_intensity_leaving_big_4=="muenchen"]<- "Muenchen"

# Display the popular connections starting from the big 4
qmplot(data = dplyr::filter(cities_above_80k, city_size > 300000), long, lat,
       geom = "point",
       
       maptype = "toner-background", darken = .7, legend = "none",
       color = I("black"))+
  scale_size_continuous(name = "Population [thsd.]") +
  geom_segment(data = dplyr::filter(primary_connections_intensity_leaving_big_4, from_city_size > 1000000 &
                                      destination_city_size > 300000), 
               aes(x = from_long, y = from_lat, xend = destination_long,
                   yend = destination_lat),
               color = "red",
               
               alpha = 0.2) +
  facet_wrap(from_city_name ~ .)+
  theme(legend.position = "none",
        plot.caption = element_text(hjust = 0))+
  labs(caption = "Fig 1: Popular Connections departing German \n Transportation Hubs")


#################################################################################################
# -> Dataset Descriptive Statistics --> average duration and average price per connection
#################################################################################################



summary_bus <- primary_connections_intensity_leaving_big_4 %>% 
  mutate(Price = as.double(Price)) %>% 
  group_by(From, Destination) %>% 
  summarise(
    mean_price_bus = mean(Price, na.rm = TRUE),
    mean_duration_bus = mean(duration, na.rm = TRUE))
  
summary_train <- train_connection_db_cleaned %>% 
  group_by(departure_city_id, arrival_city_id) %>% 
  mutate(normal_price = as.double(normal_price),
         saver_price = as.double(saver_price)) %>% 
  summarise(
    mean_normal_price_train = mean(normal_price, na.rm = TRUE),
    mean_saver_price_train = mean(saver_price, na.rm = TRUE),
    mean_duration_train = mean(duration_dec, na.rm = TRUE))
  
  
price_duration_cross_comparison <- dplyr::left_join(summary_bus, summary_train,
                                                    by = c("From" = "departure_city_id",
                                                           "Destination" = "arrival_city_id"))

#replace ID by city name for the plot visualization
price_duration_cross_comparison[price_duration_cross_comparison== 88]<- "Berlin"
price_duration_cross_comparison[price_duration_cross_comparison== 107]<- "Cologne"
price_duration_cross_comparison[price_duration_cross_comparison== 118]<- "Hamburg"
price_duration_cross_comparison[price_duration_cross_comparison== 94]<- "Munich"


ggplot(data = price_duration_cross_comparison, aes(x = mean_duration_train, y = mean_duration_bus))+
  geom_point()+
  facet_wrap(From ~ .)+
  xlim(0,
       max(price_duration_cross_comparison$mean_duration_bus, na.rm = TRUE))+
  ylim(0,
       max(price_duration_cross_comparison$mean_duration_bus, na.rm = TRUE))+
  geom_abline(intercept = 0, slope = 1,
              colour = "red",
              linetype = "dashed")+
  labs(
       x = "Mean bus travel duration [h]",
       y = "Mean train travel duration [h]",
       caption = "Fig 2: Comparison of travel durations for popular \n connections departing German  transportation hubs")+
  theme(
        plot.caption = element_text(hjust = 0))
  



#are longer journeys more expensive? 
ggplot(data = price_duration_cross_comparison, aes(x = mean_duration_train, 
                                                   y = mean_normal_price_train))+
  geom_point(color = "red", size = 2)+
  geom_point(data = price_duration_cross_comparison, aes(x = mean_duration_train,
                                                         y = mean_saver_price_train),
             color = "black", size = 2)+
  geom_point(data = price_duration_cross_comparison, aes(x= mean_duration_bus,
                                                         y = mean_price_bus),
             color = "green", size = 2)+
  geom_smooth(data = price_duration_cross_comparison, aes(x = mean_duration_train, 
                                                          y = mean_normal_price_train),
              color = "red")+
  geom_smooth(data = price_duration_cross_comparison, aes(x = mean_duration_train,
                                                          y = mean_saver_price_train),
              color = "black")+
  geom_smooth(data = price_duration_cross_comparison, aes(x= mean_duration_bus,
                                                          y = mean_price_bus),
              color = "green")+
  labs(title = "Paying per hour?",
       subtitle = "Comparison of duration and price of popular connections departing German \n transportation hubs",
       x= "Mean duratation of popular connection [h]",
       y = "Mean price of popular connection [€]")+
  geom_curve(aes(xend = 8, yend = 140, x = 12, y  = 120),
             curvature = 0.2,
             arrow = arrow(length = unit(0.03, "npc")),
             size = 0.4)+
  geom_curve(aes(xend = 8, yend = 70, x = 12, y  = 80),
             curvature = -0.2,
             arrow = arrow(length = unit(0.03, "npc")),
             size = 0.4)+
  geom_curve(aes(xend = 6, yend = 10, x = 10, y  = 4),
             curvature = -0.2,
             arrow = arrow(length = unit(0.03, "npc")),
             size = 0.4)+
  annotate(geom="text", x=12, y=115, label="Normal Train Price Offers",
           color="black")+
  annotate(geom="text", x=12, y=85, label="Savers Train Price Offers",
           color="black")+
  annotate(geom="text", x=12, y=5, label="Bus Price Offers",
           color="black")
  



#################################################################################################
# -> incorporating google dataset into the research
#################################################################################################
#re-create the dataset from above just without the city-names being replaced
summary_bus <- primary_connections_intensity_leaving_big_4 %>% 
  mutate(Price = as.double(Price)) %>% 
  group_by(From, Destination) %>% 
  summarise(
    mean_price_bus = mean(Price, na.rm = TRUE),
    mean_duration_bus = mean(duration, na.rm = TRUE))



summary_train <- train_connection_db_cleaned %>% 
  group_by(departure_city_id, arrival_city_id) %>% 
  mutate(normal_price = as.double(normal_price),
         saver_price = as.double(saver_price)) %>% 
  summarise(
    mean_normal_price_train = mean(normal_price, na.rm = TRUE),
    mean_saver_price_train = mean(saver_price, na.rm = TRUE),
    mean_duration_train = mean(duration_dec, na.rm = TRUE))


price_duration_cross_comparison <- dplyr::left_join(summary_bus, summary_train,
                                                    by = c("From" = "departure_city_id",
                                                           "Destination" = "arrival_city_id"))




#now merge with the google data
price_duration_cross_comparison <- dplyr::left_join(price_duration_cross_comparison,
                                                    connection_google_data, 
                                                    by = c("From" = "departure_city_id",
                                                    "Destination" = "arrival_city_id"))


ggplot(data = price_duration_cross_comparison, aes(x = distance, y = mean_duration_bus))+
  geom_point( color = "green")+
  geom_point(data = price_duration_cross_comparison, aes(x = distance, y = mean_duration_train),
             color = "red", alpha = 0.4)+
  labs(
       caption = "Fig 3: Comparison of average travel duration by \n bus (green) and train (red)  relative to distance",
       x = "Distance [km]",
       y = "Average travel time [h]"
       )+
  geom_smooth(data = price_duration_cross_comparison, aes(x = distance, y = mean_duration_train),
              color = "red")+
  geom_smooth(data = price_duration_cross_comparison, aes(x = distance, y = mean_duration_bus),
              color = "green")+
  theme(
        plot.caption = element_text(hjust = 0))




#################################################################################################

price_duration_cross_comparison <- dplyr::filter(price_duration_cross_comparison, departure_city != "NA")
ggplot(data = price_duration_cross_comparison, aes(x = distance, 
                                                   y = mean_normal_price_train))+
  geom_point(color = "red", size = 2)+
  geom_point(data = price_duration_cross_comparison, aes(x = distance,
                                                         y = mean_saver_price_train),
             color = "black", size = 2)+
  geom_point(data = price_duration_cross_comparison, aes(x= distance,
                                                         y = mean_price_bus),
             color = "green", size = 2)+
  geom_smooth(data = price_duration_cross_comparison, aes(x = distance, 
                                                          y = mean_normal_price_train),
              color = "red")+
  geom_smooth(data = price_duration_cross_comparison, aes(x = distance,
                                                          y = mean_saver_price_train),
              color = "black")+
  geom_smooth(data = price_duration_cross_comparison, aes(x= distance,
                                                          y = mean_price_bus),
              color = "green")+
labs(
     caption = "Fig4: Comparison of duration and price of popular connections: \n 
     bus prices (green), train savers prices (black), train normal prices (red)",
     x= "Mean distance of popular connection [km]",
     y = "Mean price of popular connection [€]")+
  facet_wrap(departure_city ~ .)+
  theme(
        panel.grid = element_blank(),
        plot.caption = element_text(hjust = 0))






#she took the midnight train
train_connection_db_cleaned$departure_time <- as.double(train_connection_db_cleaned$departure_time)
ggplot(data=train_connection_db_cleaned, aes(departure_time)) + 
  geom_histogram(bins = 24, aes(y = ..density..),
                 fill = "red", color = "black")+
  
  labs(y = "Share of connections",
       x = "Time",
       
       caption = "Fig 5: Comparison of shares of busses (green) and trains (red)  \n departing by hour of the day")+
  geom_histogram(data = primary_connections_intensity_leaving_big_4,
                 bins = 24, aes(x = depature_hours ,y = -..density..),
                 fill = "green", color = "black")+
  
  scale_x_continuous(breaks=c(0,6,9, 12, 18, 22))+
  
  
  geom_curve(aes(xend = 7, yend = 0.060, x = 10, y  = 0.08),
             curvature = 0.2,
             arrow = arrow(length = unit(0.03, "npc")),
             size = 0.4)+
  annotate(geom="text", x=10, y=0.085, label="Train-Commuters Time",
           color="black")+
  
  geom_curve(aes(xend = 20, yend = -0.07, x = 12, y  = -0.09),
             curvature = 0.2,
             arrow = arrow(length = unit(0.03, "npc")),
             size = 0.4)+
  annotate(geom="text", x=6, y=-0.09, label="Midnight-bus going anywhere",
           color="black")+
  theme(axis.ticks.y.left = element_blank(),
        axis.text.y = element_blank(),
        panel.grid = element_blank(),
        plot.caption = element_text(hjust = 0))








#################################################################################################
# graphics that didn´t made the cut to the report
#limiting analysis to direct connections: maybe here the bus can hold the train advantage on duration
#################################################################################################
#creating a dataset which though only includes direct bus connections
direct_busses <- dplyr::filter(primary_connections_intensity_leaving_big_4, 
                               is.na(shift_time)) # <-here we filter for only direct connections

summary_bus_direct <- direct_busses %>% 
  mutate(Price = as.double(Price)) %>% 
  group_by(From, Destination) %>% 
  summarise(
    mean_price_bus = mean(Price, na.rm = TRUE),
    mean_duration_bus = mean(duration, na.rm = TRUE))
price_duration_cross_comparison <- dplyr::left_join(summary_bus_direct, summary_train,
                                                    by = c("From" = "departure_city_id",
                                                           "Destination" = "arrival_city_id"))

price_duration_cross_comparison$comparison_level <- "direct bus connections only"

direct_busses_comparison <- price_duration_cross_comparison




#now creating again the dataset without any filtery
summary_bus <- primary_connections_intensity_leaving_big_4 %>% 
  mutate(Price = as.double(Price)) %>% 
  group_by(From, Destination) %>% 
  summarise(
    mean_price_bus = mean(Price, na.rm = TRUE),
    mean_duration_bus = mean(duration, na.rm = TRUE))
price_duration_cross_comparison <- dplyr::left_join(summary_bus, summary_train,
                                                    by = c("From" = "departure_city_id",
                                                           "Destination" = "arrival_city_id"))
price_duration_cross_comparison$comparison_level <- "all bus connections"




#now bind both datasets for comparison
direct_busses_comparison_data <- dplyr::bind_rows(direct_busses_comparison, price_duration_cross_comparison)



ggplot(data = direct_busses_comparison_data, aes(x = mean_duration_train, y = mean_duration_bus,
                                                 color = comparison_level))+
  geom_point(alpha = 0.5)+
  facet_wrap(From ~ .)+
  xlim(0,
       max(price_duration_cross_comparison$mean_duration_bus, na.rm = TRUE))+
  ylim(0,
       max(price_duration_cross_comparison$mean_duration_bus, na.rm = TRUE))+
  geom_abline(intercept = 0, slope = 1,
              colour = "red",
              linetype = "dashed")+
  labs(title = "Bus and train travel duration",
       subtitle = "Comparison of popular connections departing German \n transportation hubs",
       x = "Mean bus travel duration [h]",
       y = "Mean train travel duration [h]")+
  labs(colour="Included bus connections")


  
  

