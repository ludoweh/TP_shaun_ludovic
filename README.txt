Toutes les commandes sont faîtes sur le namenode.
Après s'être placé dans un répertoire approprié, on doit créer notre fichier csv :
sudo nano datashaunludovic.csv

Ensuite on va pousser notre fichier sur notre instance de hdfs
hadoop fs -put datashaunludovic.csv /user/administrator/shaun-ludovic/

On arrête tous les datanodes et le namenode
stop-all.sh

On redémarre tous les services
start-all.sh

Si tout s'est bien passé on peut maintenant consulter notre fichier sur l'interface hdfs.


Concernant les scripts:

Tout fonctionnne grâce à des appels de méthodes directement via ligne de commande:
Nous avons implémenté deux services:

1) un service pour ne pas prendre en compte une ligne d'un CSV:
-on le lance en appelant la méthode main en mettant comme argument "delete x" avec x qui est l'id de la personne que l'on ne veut pas dans le rendu

2)un service pour hasher les données des personnes:
-on le lance en appelant la méthode main en mettant comme argument "hash"

dans le cas présent l'utilisation de scopt ne fonctionne pas, les 2 services sont donc lancés un par un sans demande d'arguments.
