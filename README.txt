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

Tout fonctionnne grâce à des appels de méthodes directement via ligne de commande grâce à scopt :
Nous avons implémenté deux services:

1) un service pour ne pas prendre en compte une ligne d'un CSV :
- on le lance en écrivant "-d x" en ligne de commande avec x qui est l'id de la personne que l'on veut supprimer
La méthode delete se lance et va supprimer la ligne correspondant à cet id s'il existe bien.
Si l'id n'existe pas, ou plus, on a un retour sur le terminal que cet id n'existe pas.

2)un service pour hasher les données des personnes:
- on le lance en écrivant "-h x" avec x l'id de la personne qu'on veut hasher.
La méthode hash se lance et va encrypter la ligne correspondant à cet id s'il existe bien.
Si l'id n'existe pas, ou plus, on a un retour sur le terminal que cet id n'existe pas.
