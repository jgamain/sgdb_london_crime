require(highcharter)
library(tidyverse)

crimeWestminsterMajorAvg <- arrange(crimeWestminsterMajorAvg, avgPerYear)
hchart(crimeWestminsterMajorAvg,'column', hcaes(x = major_category, y = avgPerYear, color = avgPerYear)) %>% 
  hc_add_theme(hc_theme_smpl()) %>%
  hc_xAxis(title = list(text = "Type de crime")) %>%
  hc_yAxis(title = list(text = "Moyenne par an"))

crimeWestminsterMinorAvg <- arrange(crimeWestminsterMinorAvg, avgPerYear)
hchart(crimeWestminsterMinorAvg,'column', hcaes(x = minor_category, y = avgPerYear, color = avgPerYear)) %>% 
  hc_add_theme(hc_theme_smpl()) %>%
  hc_xAxis(title = list(text = "Type de crime")) %>%
  hc_yAxis(title = list(text = "Moyenne par an"))

