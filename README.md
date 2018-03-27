# London Crimes Investigation

Pour executer notre code, voici les commandes que vous devriez lancer: 

mvn package clean

spark-submit --class fr.upmc_insta.stl.dar.LondonCrime --master local target/sgbd-london-crime-1.0-SNAPSHOT.jar

Les fichiers d'input sont :
- london_crime_by_lsoa.csv
- lsoa-data_iadatasheet1.csv
- months.csv

Nos fichiers de résultat sont dans le dossier "results".


