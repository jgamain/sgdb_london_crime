require(highcharter)
library(tidyverse)


crime <- tbl_df(criminality)
View(crime)

crime <-
  crime %>% mutate(rate = avgCrimesPerYear / population) %>% arrange(population)
head(crime)
hchart(crime, 'column', hcaes(x = borough, y = rate, color = avgCrimesPerYear)) %>%
  hc_title(text = "Dans quels arrondissements la criminalité est-elle la plus élevée ?") %>%
  hc_xAxis(title = list(text = "Arrondissement ordonné par population")) %>%
  hc_yAxis(title = list(text = "Moyenne du nombre de crimes par an par habitant")) %>%
  hc_add_theme(hc_theme_flat()) %>%
  hc_exporting(enabled = TRUE)
