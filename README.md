# London Crimes Investigation

Pour executer notre code, voici les commandes que vous devriez lancer: 

mvn package clean

spark-submit --class fr.upmc_insta.stl.dar.LondonCrime --master local target/sgbd-london-crime-1.0-SNAPSHOT.jar [chemin_vers_le_london_crime_by_lsoa.csv]

Les fichiers d'input sont :
- london_crime_by_lsoa.csv (argument)
- lsoa-data_iadatasheet1.csv (local)
- months.csv (local)

Nos fichiers de résultat sont dans le dossier "results".

Le code pour obtenir les graphiques est dans le dossier "plots".


