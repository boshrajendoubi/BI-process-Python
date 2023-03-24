import psycopg2
import pygrametl
from pygrametl.datasources import CSVSource, TransformingSource, FilteringSource

from pygrametl.tables import Dimension, FactTable

#connexion a la bd
connect = psycopg2.connect("dbname='projet' user='postgres' password='justinbieber12345'")
curseur = connect.cursor()
"""
curseur.execute("CREATE SCHEMA HébergementsTouristiques")
connect.commit()


#Création de la table Commune
curseur.execute("CREATE Table HébergementsTouristiques.Commune (idCommune varchar(20) PRIMARY KEY , nomC varchar(50) )")
connect.commit()


#creation de la table hotel
curseur.execute("CREATE Table HébergementsTouristiques.Hotel (idHotel varchar(20) PRIMARY KEY , rue varchar(50) , codePostal int , idCommune varchar(50) REFERENCES HébergementsTouristiques.Commune(idCommune), tel1 varchar(50) NOT NULL, tel2 varchar(50), mail varchar(50) NOT NULL, siteWeb1 varchar(50) NOT NULL, siteWeb2 varchar(50), nbEtoiles int NOT NULL, capacité int NOT NULL )")
connect.commit()


#Création de la table Offre
curseur.execute("CREATE Table HébergementsTouristiques.Offre (idOffre varchar(20) PRIMARY KEY , nom varchar(50) , idHotel varchar(20) REFERENCES HébergementsTouristiques.Hotel(idHotel) )")
connect.commit()

#creation de la table tarif logement
curseur.execute("CREATE Table HébergementsTouristiques.TarifLogement (idTarifLog varchar(20) PRIMARY KEY , type varchar(50) , prix int)")
connect.commit()

#Création de la table Tarif Service
curseur.execute("CREATE Table HébergementsTouristiques.TarifService (idTarifServ varchar(20) PRIMARY KEY , type varchar(50) , prix int )")
connect.commit()

#Création de la table Année 
curseur.execute("CREATE Table HébergementsTouristiques.Année (idAnnee int PRIMARY KEY)")
connect.commit()

#Création de la table Mois 
curseur.execute("CREATE Table HébergementsTouristiques.Mois (idMois int PRIMARY KEY ,idAnnee int REFERENCES HébergementsTouristiques.Année(idAnnee) )")
connect.commit()

#Création de la table Jour
curseur.execute("CREATE Table HébergementsTouristiques.Jour (idJour int PRIMARY KEY , idMois int REFERENCES HébergementsTouristiques.Mois(idMois) )")
connect.commit()

#Création de la table Date
curseur.execute("CREATE Table HébergementsTouristiques.Date (idDate int PRIMARY KEY , nom varchar(50) , idJour int REFERENCES HébergementsTouristiques.jour(idJour) )")
connect.commit()

#creation de la table hebergements touristiques 
curseur.execute("CREATE Table HébergementsTouristiques.HebergementsTouristique (idOffre  varchar(50) REFERENCES HébergementsTouristiques.Offre(idOffre) , idTarifLog  varchar(50) REFERENCES HébergementsTouristiques.TarifLogement(idTarifLog) ,idTarifServ  varchar(50) REFERENCES HébergementsTouristiques.tarifService(idTarifServ), idDate  int REFERENCES HébergementsTouristiques.date(idDate), nb_semaines int NOT NULL, charge int NOT NULL )")
connect.commit()
"""






