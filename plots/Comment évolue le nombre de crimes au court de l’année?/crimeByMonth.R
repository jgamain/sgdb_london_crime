library(tidyverse)
require(anytime)


crimeByMonth <- tbl_df(cbm) %>% arrange(year,month_id)
head(crimeByMonth)

qplot(data = crimeByMonth, x= factor(month_id), y=total, geom = "line", color = factor(year), group = factor(year)) + 
  geom_point() +
  labs(x = "Mois", y = "Nombre de crimes", color = "Somme") + scale_x_discrete(labels = crimeByMonth$month_name) +
  theme_bw()


crimeByMonthAvg <- tbl_df(cbma) %>% arrange(month_id)
head(crimeByMonthAvg)

qplot(data = crimeByMonthAvg, x=factor(month_id), y=average, geom = "line", group =1)+
  geom_point() +
  labs(x = "Mois", y = "Nombre de crimes moyen")  + scale_x_discrete(labels = crimeByMonth$month_name)+ 
  theme_bw()

