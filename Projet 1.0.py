import psycopg2
import pygrametl
from pygrametl.datasources import CSVSource, TransformingSource, FilteringSource
import json
from pygrametl.tables import Dimension, FactTable
import matplotlib.pyplot as plt


pgconn = psycopg2.connect(dbname='projet',user='postgres', password='justinbieber12345')


cu=pgconn.cursor()


connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to HébergementsTouristiques')

Hotel_dimension = Dimension(
    name='hébergementstouristiques.hotel',
    key='idHotel',
    attributes=['rue', 'codePostal', 'idCommune', 'tel1', 'tel2', 'mail', 'siteWeb1', 'siteWeb2', 'nbEtoiles', 'capacité'])

TarifLog_dimension = Dimension(
    name='HébergementsTouristiques.TarifLogement',
    key='idTarifLog',
    attributes=['type', 'prix'])

HebergementTouristique_Fact = FactTable(
    name='HébergementsTouristiques.HebergementsTouristique',
    keyrefs=['idOffre', 'idTarifLog', 'idTarifServ', 'iddate'],
    measures=['nb_semaines', 'charge'])

Offre_dimension = Dimension(
    name='HébergementsTouristiques.offre',
    key='idoffre',
    attributes=['nom','idhotel'])

Commune_dimension = Dimension(
    name='hébergementstouristiques.commune',
    key='idCommune',
    attributes=['nomC'])

TarifService_dimension = Dimension(
    name='HébergementsTouristiques.TarifService',
    key='idTarifServ',
    attributes=['type','prix'])

Année_dimension = Dimension(
    name='hébergementstouristiques.année',
    key='idannee',
    attributes=['annee'])

Mois_dimension = Dimension(
    name='HébergementsTouristiques.mois',
    key='idmois',
    attributes = ['idannee','mois'])

Jour_dimension = Dimension(
    name='hébergementsTouristiques.jour',
    key='idjour',
    attributes = ['idmois','jour'])

Date_dimension = Dimension(
    name='hébergementsTouristiques.date',
    attributes = ['idjour','date'],
    key='iddate')

source = open('E:\Sauvgarde Dell Bochra\Document\Isg L3\LPE-BI\Hébergements_touristiques.csv', 'r', 200)
data = CSVSource(source, delimiter=',')
print('lecture du fichier réussi')

def transform(row):
    
    info=row['CLASSEMENT']
    ch="Ã©"
    if info.__contains__(ch):
         info=info.replace(ch,"é")
    row['CLASSEMENT']=info
    
    info1=row['NOM_OFFRE']
    ch1="ï¿½"
    if info1.__contains__(ch1):
         info1=info1.replace(ch1,"é")
    row['NOM_OFFRE']=info1
    
    info2=row['INFOS_COMPLEMENTAIRES']
    ch2="Ã¯Â¿Â½"
    ch3="CÃ¯Â¿Â½ble"
    if info2.__contains__(ch3):
         info2=info2.replace(ch3,"cable")
    if info2.__contains__(ch2):
         info2=info2.replace(ch2,"é")
    row['INFOS_COMPLEMENTAIRES']=info2
    
    info3=row['RUE']
    ch4="Ã¯Â¿Â½"
    if info3.__contains__(ch4):
         info3=info3.replace(ch4,"é")
    row['RUE']=info3
    
    info4=row['NOM_OFFRE']
    if info4.__contains__(ch4):
         info4=info4.replace(ch4,"é")
    row['NOM_OFFRE']=info4
    TarifLog=row['Tarif_logement'].split('|')
    row['typeLog']=TarifLog[0]
    row['prixLog']=TarifLog[1]
    TarifServ=row['Tarif_SERVICES'][6:]
    TarifServ=TarifServ.split('|')
    row['typeServ']=TarifServ[0]
    row['prixServ']=TarifServ[1]
    row['Date']=row['Date'][0:10]
    row['Nombre_Semaine_Heberge']=round(int(row['Nombre_Semaine_Heberge'])/1200)
    row['CLASSEMENT']=row['CLASSEMENT'][0:1]
    row['charge']=int(row['Nombre_Semaine_Heberge'])*(int(row['prixLog'])+int(row['prixServ']))
    row['jour']=row['Date'][0:2]
    row['mois']=row['Date'][3:5]
    row['annee']=row['Date'][6:10]
    tel=row['TEL'].split('#')
    if len(tel)==1:
        row['tel1']=tel
        row['tel2']=" "
    else:
        row['tel1']=tel[0]
        row['tel2']=tel[1]
             
    site=row['SITE_WEB'].split('# ')
    if len(site)==1:
        row['site1']=site
        row['site2']=" "
    else:
        row['site1']=site[0]
        row['site2']=site[1]
        
    
    
    

    

                   
dataT = TransformingSource(data, transform)


"""
CompteurLigne=0

for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idCommune']=i['Id']
    dic['nomC']=i['COMMUNE']
    print(CompteurLigne)
    print(dic)
    Commune_dimension.insert(dic)

print("chargement dimension commune est terminé")"""
"""
CompteurLigne=0
for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idHotel']=i['Id']
    dic['rue']=i['RUE']
    dic['codePostal']=i['CODE_POSTAL']
    dic['idCommune']=i['Id']
    dic['tel1']=i['tel1']
    dic['tel2']=i['tel2']
    dic['mail']=i['MaiL']
    dic['siteWeb1']=i['site1']
    dic['siteWeb2']=i['site2']
    dic['nbEtoiles']=i['CLASSEMENT']
    dic['capacité']=i['CAPACITE_NBRE_PERS'] 
    print(CompteurLigne,"-------------------------------------")
    print(dic)
    Hotel_dimension.insert(dic)

print("chargement dimension Hotel est terminé")"""
"""
CompteurLigne=0

for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idTarifLog']=i['Id']
    dic['type']=i['typeLog']
    dic['prix']=i['prixLog']
    print(CompteurLigne)
    print(dic)
    TarifLog_dimension.insert(dic)

print("chargement dimension tarif logement est terminé")

CompteurLigne=0
for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idOffre']=i['Id']
    dic['idHotel']=i['Id']
    dic['nom']=i['NOM_OFFRE']

    print(CompteurLigne)
    print(dic)
    Offre_dimension.insert(dic)

print("chargement dimension offre est terminé")

CompteurLigne=0
for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idTarifServ']=i['Id']
    dic['type']=i['typeServ']
    dic['prix']=i['prixServ']

    print(CompteurLigne)
    print(dic)
    TarifService_dimension.insert(dic)

print("chargement dimension Tarif service  est terminé")

CompteurLigne=0
for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idannee']=i['Id']
    dic['annee']=i['annee']
    print(CompteurLigne)
    print(dic)
    Année_dimension.insert(dic)

print("chargement dimension annee  est terminé")

CompteurLigne=0
for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idmois']=i['Id']
    dic['mois']=i['mois']
    dic['idannee']=i['Id']
    print(CompteurLigne)
    print(dic)
    Mois_dimension.insert(dic)

print("chargement dimension mois  est terminé")

CompteurLigne=0
for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idmois']=i['Id']
    dic['jour']=i['jour']
    dic['idjour']=i['Id']
    print(CompteurLigne)
    print(dic)
    Jour_dimension.insert(dic)

print("chargement dimension jour est terminé")
CompteurLigne=0
for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idjour']=i['Id']
    dic['date']=i['Date']
    dic['iddate']=i['Id']
    print(CompteurLigne)
    print(dic)
    Date_dimension.insert(dic)
print("chargement dimension date est terminé")

CompteurLigne=0
for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idOffre']=i['Id']
    dic['idTarifLog']=i['Id']
    dic['iddate']=i['Id']
    dic['idTarifServ']=i['Id']
    dic['nb_semaines']=i['Nombre_Semaine_Heberge']
    dic['charge']=i['charge']
    
    print(CompteurLigne)
    print(dic)
    HebergementTouristique_Fact.insert(dic)
print("chargement dimension fact est terminé")


connection.commit()
connection.close()


pgconn.close()
 
CompteurLigne=0
for i in dataT:
    CompteurLigne+=1
    dic={}
    dic['idoffre']=i['Id']
    dic['nom']=i['NOM_OFFRE']
    if len(i['Id'])==17:
        dic['idhotel']=i[''][0:len(i['Id'])-1]Id
    else:
        dic['idhotel']=i['Id']

    print(CompteurLigne)
    print(dic)
    Offre_dimension.insert(dic)

print("chargement dimension offre est terminé")
connection.commit()

"""

#connexion a la bd
connect = psycopg2.connect("dbname='projet' user='postgres' password='justinbieber12345'")
curseur = connect.cursor()
curseur.execute("select * from hébergementstouristiques.année" )
connect.commit()
liste1=[]
for i in curseur.fetchall():
    liste=list(i)
    liste1.append(liste[1])

curseur.execute('SELECT * FROM hébergementstouristiques.hebergementstouristique INNER JOIN hébergementstouristiques.année ON hébergementstouristiques.année.idannee = hébergementstouristiques.hebergementstouristique.iddate')
connect.commit()

liste2=[]
somme16=0
somme17=0
somme18=0
for i in curseur.fetchall():
    liste=list(i)
    if liste[7]==2016:
        somme16=somme16+liste[4]
    if liste[7]==2017:
        somme17=somme17+liste[4]
    if liste[7]==2018:
        somme18=somme18+liste[4]
        
    #liste2.append(liste[0])
    

listesomme=[]
listesomme.append(somme16)
listesomme.append(somme17)
listesomme.append(somme18)
print(listesomme)
"""
curseur.execute("DELETE from hébergementstouristiques.hotel where idhotel LIKE 'VILAQU064FS000052' ")
connect.commit()
print("done")"""


"""
curseur.execute("select * from hébergementstouristiques.année" )
connect.commit()
liste1=[]
for i in curseur.fetchall():
    liste=list(i)
    liste1.append(liste[1])"""

curseur.execute('SELECT nbetoiles FROM hébergementstouristiques.hotel INNER JOIN hébergementstouristiques.offre ON hébergementstouristiques.hotel.idhotel = hébergementstouristiques.offre.idhotel')
connect.commit()
listenbetoiles=[]
for i in curseur.fetchall():
    print(i)
    liste4=list(i)
    listenbetoiles.append(liste4[0])

print(listenbetoiles)

curseur.execute('SELECT nb_semaines FROM hébergementstouristiques.hebergementstouristique INNER JOIN hébergementstouristiques.offre ON hébergementstouristiques.hebergementstouristique.idoffre = hébergementstouristiques.offre.idoffre')
connect.commit()
listenbsemaines=[]
for i in curseur.fetchall():
    print(i)
    liste5=list(i)
    listenbsemaines.append(liste5[0])

print(listenbsemaines)
"""
listenbetoiles=list(set(listenbetoiles))
print(listenbetoiles)
for i in listenbetoiles:
    i=str(i)
 """
 
dict_from_list = dict(zip(listenbsemaines, listenbetoiles)) 
values1=[] 
for i in range(5):
    values1.append(0)
print(values1)
for i,j in dict_from_list.items():
    if j==1:
        values1[0]=values1[0]+i
    if j==2:
        values1[1]=values1[1]+i
    if j==3:
        values1[2]=values1[2]+i
    if j==4:
        values1[3]=values1[3]+i
    if j==5:
        values1[4]=values1[4]+i

print(values1)
names1=['1','2','3','4','5']
#print(dict_from_list) 

curseur.execute('SELECT DISTINCT nomc, count(idhotel) FROM hébergementstouristiques.hotel, hébergementstouristiques.commune WHERE hébergementstouristiques.hotel.idcommune = hébergementstouristiques.commune.idcommune GROUP BY hébergementstouristiques.commune.nomc')
connect.commit()
listecommunes=[]
listenbhotels=[]
liste6=[]
for i in curseur.fetchall():
    liste6=list(i)
    listecommunes.append(liste6[0])
    listenbhotels.append(liste6[1])
    
print(liste6)
print(listecommunes)
print(listenbhotels)

curseur.execute('SELECT count(idhotel) from hébergementstouristiques.hotel GROUP BY nbetoiles ')
connection.commit()
liste7=[]
listeee=[]
for i in curseur.fetchall():
    liste7=list(i)
    listeee.append(liste7[0])

print(listeee)
names2=['1','2','3','4']
plt.figure()
names = ['2016', '2017', '2018']
values = listesomme
plt.subplot(131)
plt.plot(names, values)
plt.xlabel('Année')
plt.ylabel('Charge')
plt.suptitle('Charge par année ')

plt.subplot(132)
plt.bar(names1, values1)
plt.xlabel('nombre des étoiles')
plt.ylabel('nombre des semaines hébérgées')
plt.suptitle('nombre des étoiles')

plt.subplot(133)
plt.pie(listeee,labels=names2)
plt.xlabel('étoiles')
plt.ylabel('nombre des hotels')
plt.suptitle('Les indicateurs de performance')

plt.show()

            
            
        