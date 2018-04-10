require(highcharter)
library(tidyverse)


london_crime_by_lsoa <- tbl_df(london_crime_by_lsoa)

data <- london_crime_by_lsoa %>%
  group_by(borough, major_category) %>%
  summarise(sum = sum(value), count = n(), meanValue = mean(value))


data <- arrange(data, desc(meanValue))

hchart(data,'column', hcaes(x = borough, y = sum, size = meanValue, group = major_category)) %>% 
  hc_plotOptions(column = list(stacking = 'normal')) %>% 
  hc_add_theme(hc_theme_flat()) %>%
  hc_title(text = "Somme des crimes par arrondissement") %>%
  hc_xAxis(title = list(text = "Arrondissement")) %>%
  hc_yAxis(title = list(text = "Somme"))
  
