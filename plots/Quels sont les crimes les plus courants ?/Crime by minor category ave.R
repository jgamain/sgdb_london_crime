require(highcharter)
library(tidyverse)


crime <- tbl_df(crimeByMinorCategoryAvg)
head(crime)
crime <- arrange(crime, averagePerYear)


hchart(crime, 'pie', hcaes(x = minor_category, y = crime$averagePerYear, color = crime$averagePerYear)) %>%
  hc_title(text = "Crime by minor category average")
