require(highcharter)
library(tidyverse)


crime <- tbl_df(crimeByMajorCategoryAvg)
head(crime)
crime <- arrange(crime, averagePerYear)


hchart(crime, 'pie', hcaes(x = major_category, y = averagePerYear, color = averagePerYear)) %>%
  hc_title(text = "Crime by major category average")

