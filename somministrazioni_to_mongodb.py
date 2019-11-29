# ==================
#  SOMMINISTRAZIONI
# ==================
# ----------------------------------------------------------------------
# Import date from .csv and convert into a nested .js format for MongoDB
# ----------------------------------------------------------------------

# Clear all
try:
    from IPython import get_ipython
    get_ipython().magic('clear')
    get_ipython().magic('reset -f')
except:
    pass

# Required packages
# -----------------
import pandas as pd
import numpy as np
import json
from json import dumps
import os
import pymongo 
import time
import datetime
import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action = "ignore", category = SettingWithCopyWarning)

# Import data (.csv format)
# -------------------------
# Attivazioni
start_time = time.time()
df_att = pd.read_csv('C:/Users/Nicola Caravaggio/OneDrive/Desktop/Ciu_Tos_Roma3/Somministrazioni/CO_II_TRIM_2019_CF_DKMU_Estratto.csv')
df_att['TIPOLOGIA'] = 'Attivazione'
e = int(time.time() - start_time)
elapsed_time = time.time() - start_time
print('\n Data import Attivazioni')
print('- required time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

# Missioni
start_time = time.time()
df_mis = pd.read_csv('C:/Users/Nicola Caravaggio/OneDrive/Desktop/Ciu_Tos_Roma3/Somministrazioni/MISSIONI_II_TRIM_2019_CF_DKMU_Estratto.csv')
df_mis['TIPOLOGIA'] = 'Missione'
e = int(time.time() - start_time)
elapsed_time = time.time() - start_time
print('\n Data import Missioni')
print('- required time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

# Somministrazioni
start_time = time.time()
df_som = pd.read_csv('C:/Users/Nicola Caravaggio/OneDrive/Desktop/Ciu_Tos_Roma3/Somministrazioni/SOMM_II_TRIM_2019_CF_DKMU_Estratto.csv')
df_som['TIPOLOGIA'] = 'Somministrazione'
e = int(time.time() - start_time)
elapsed_time = time.time() - start_time
print('\n Data import Somministrazioni')
print('- required time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

# Append
start_time = time.time()
df = df_att.append(df_mis, sort = False)
df = df.append(df_som, sort = False)
e = int(time.time() - start_time)
elapsed_time = time.time() - start_time
print('\n Append datasets')
print('- required time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

# Null values
# -----------
# Check the presence
df.isnull().values.any()
df.isnull().sum()
# Replace spaces as null values
df = df.replace(r'^\s*$', np.nan, regex = True)
df.isnull().values.any()
df.isnull().sum()
# NaN to null (if data has been already processed with another software, e.g. R, STATA)
df = df.where((pd.notnull(df)), None)
 
# Replace values
# --------------
# 'CODREGIONEDOMICILIO' with regions' names (from 'tracciato campione CICO')
df.CODREGIONEDOMICILIO.replace(1, 'Abruzzo', inplace = True)
df.CODREGIONEDOMICILIO.replace(2, 'Basilicata', inplace = True)
df.CODREGIONEDOMICILIO.replace(3, 'Bolzano', inplace = True)
df.CODREGIONEDOMICILIO.replace(4, 'Calabria', inplace = True)
df.CODREGIONEDOMICILIO.replace(5, 'Campania', inplace = True)
df.CODREGIONEDOMICILIO.replace(6, 'Emilia Romagna', inplace = True)
df.CODREGIONEDOMICILIO.replace(7, 'Friuli Venezia Giulia', inplace = True)
df.CODREGIONEDOMICILIO.replace(8, 'Lazio', inplace = True)
df.CODREGIONEDOMICILIO.replace(9, 'Liguria', inplace = True)
df.CODREGIONEDOMICILIO.replace(10, 'Lombardia', inplace = True)
df.CODREGIONEDOMICILIO.replace(11, 'Marche', inplace = True)
df.CODREGIONEDOMICILIO.replace(12, 'Molise', inplace = True)
df.CODREGIONEDOMICILIO.replace(13, 'Piemonte', inplace = True)
df.CODREGIONEDOMICILIO.replace(14, 'Puglia', inplace = True)
df.CODREGIONEDOMICILIO.replace(15, 'Sardegna', inplace = True)
df.CODREGIONEDOMICILIO.replace(16, 'Sicilia', inplace = True)
df.CODREGIONEDOMICILIO.replace(17, 'Toscana', inplace = True)
df.CODREGIONEDOMICILIO.replace(18, 'Trento', inplace = True)
df.CODREGIONEDOMICILIO.replace(19, 'Umbria', inplace = True)
df.CODREGIONEDOMICILIO.replace(20, 'Val d\'Aosta', inplace = True)
df.CODREGIONEDOMICILIO.replace(21, 'Veneto', inplace = True)
df.CODREGIONEDOMICILIO.replace(99, 'Estero', inplace = True)

# 'CODREGIONEDOMICILIO' with regions' names (from 'tracciato campione CICO')
df.CODREGIONELAVORO.replace(1, 'Abruzzo', inplace = True)
df.CODREGIONELAVORO.replace(2, 'Basilicata', inplace = True)
df.CODREGIONELAVORO.replace(3, 'Bolzano', inplace = True)
df.CODREGIONELAVORO.replace(4, 'Calabria', inplace = True)
df.CODREGIONELAVORO.replace(5, 'Campania', inplace = True)
df.CODREGIONELAVORO.replace(6, 'Emilia Romagna', inplace = True)
df.CODREGIONELAVORO.replace(7, 'Friuli Venezia Giulia', inplace = True)
df.CODREGIONELAVORO.replace(8, 'Lazio', inplace = True)
df.CODREGIONELAVORO.replace(9, 'Liguria', inplace = True)
df.CODREGIONELAVORO.replace(10, 'Lombardia', inplace = True)
df.CODREGIONELAVORO.replace(11, 'Marche', inplace = True)
df.CODREGIONELAVORO.replace(12, 'Molise', inplace = True)
df.CODREGIONELAVORO.replace(13, 'Piemonte', inplace = True)
df.CODREGIONELAVORO.replace(14, 'Puglia', inplace = True)
df.CODREGIONELAVORO.replace(15, 'Sardegna', inplace = True)
df.CODREGIONELAVORO.replace(16, 'Sicilia', inplace = True)
df.CODREGIONELAVORO.replace(17, 'Toscana', inplace = True)
df.CODREGIONELAVORO.replace(18, 'Trento', inplace = True)
df.CODREGIONELAVORO.replace(19, 'Umbria', inplace = True)
df.CODREGIONELAVORO.replace(20, 'Val d\'Aosta', inplace = True)
df.CODREGIONELAVORO.replace(21, 'Veneto', inplace = True)
df.CODREGIONELAVORO.replace(99, 'Estero', inplace = True)

# Variables' types
# ----------------
# Convert data into dateISO format
df['DATANASCITALAVORATORE'] =  pd.to_datetime(df['DATANASCITALAVORATORE'], format = '%m/%d/%Y')
df['DATANASCITALAVORATORE'] = df['DATANASCITALAVORATORE'].dt.strftime('%Y-%m-%d')
df['RAPPORTO_DATAINIZIO'] =  pd.to_datetime(df['RAPPORTO_DATAINIZIO'], format = '%m/%d/%Y')
df['RAPPORTO_DATAINIZIO'] = df['RAPPORTO_DATAINIZIO'].dt.strftime('%Y-%m-%d')
df['DTCESSAZIONEEFFETTIVA'] =  pd.to_datetime(df['DTCESSAZIONEEFFETTIVA'], format = '%m/%d/%Y')
df['DTCESSAZIONEEFFETTIVA'] = df['DTCESSAZIONEEFFETTIVA'].dt.strftime('%Y-%m-%d')
df['DTFINEPREVISTA'] =  pd.to_datetime(df['DTFINEPREVISTA'], format = '%m/%d/%Y')
df['DTFINEPREVISTA'] = df['DTFINEPREVISTA'].dt.strftime('%Y-%m-%d')
df['DTFINEPREVISTA_ULTIMA_PROROGA'] =  pd.to_datetime(df['DTFINEPREVISTA_ULTIMA_PROROGA'], format = '%m/%d/%Y')
df['DTFINEPREVISTA_ULTIMA_PROROGA'] = df['DTFINEPREVISTA_ULTIMA_PROROGA'].dt.strftime('%Y-%m-%d')
df['DTTRASFORMAZIONE'] =  pd.to_datetime(df['DTTRASFORMAZIONE'], format = '%m/%d/%Y')
df['DTTRASFORMAZIONE'] = df['DTTRASFORMAZIONE'].dt.strftime('%Y-%m-%d')
# Convert string to date (if in the STATA format, e.g. 10jan2019)
#df['RAPPORTO_DATAINIZIO'] =  pd.to_datetime(df['RAPPORTO_DATAINIZIO'], format = '%d%b%Y')
#df['RAPPORTO_DATAINIZIO'] = df['RAPPORTO_DATAINIZIO'].dt.strftime('%Y-%m-%d')

# Check columns' types
#df.info(verbose = True)

# Convert int64 (not supported in MongoDB) into float64
df["ID_DATORELAVORO"] = df["ID_DATORELAVORO"].astype(np.float64)
df["ID_LAVORATORE"] = df["ID_LAVORATORE"].astype(np.float64)
df["CODCITTADINANZA"] = df["CODCITTADINANZA"].astype(np.float64)
df["CODTITOLOSTUDIO"] = df["CODTITOLOSTUDIO"].astype(np.float64)
df["CODPROVINCIADOMICILIO"] = df["CODPROVINCIADOMICILIO"].astype(np.float64)
df["CODPROVINCIALAVORO"] = df["CODPROVINCIALAVORO"].astype(np.float64)
df["N_PROROGHE"] = df["N_PROROGHE"].astype(np.float64)
df["FONTE"] = df["FONTE"].astype(np.float64)

# Coefficients 
# ------------
# Indentify subjects born on lap years
df['COEF'] = 0
mask = (df['DATANASCITALAVORATORE'] > '2020-01-01') & (df['DATANASCITALAVORATORE'] <= '2020-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '2016-01-01') & (df['DATANASCITALAVORATORE'] <= '2016-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '2012-01-01') & (df['DATANASCITALAVORATORE'] <= '2012-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '2008-01-01') & (df['DATANASCITALAVORATORE'] <= '2008-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '2004-01-01') & (df['DATANASCITALAVORATORE'] <= '2004-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '2000-01-01') & (df['DATANASCITALAVORATORE'] <= '2000-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1996-01-01') & (df['DATANASCITALAVORATORE'] <= '1996-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1992-01-01') & (df['DATANASCITALAVORATORE'] <= '1992-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1988-01-01') & (df['DATANASCITALAVORATORE'] <= '1988-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1984-01-01') & (df['DATANASCITALAVORATORE'] <= '1984-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1980-01-01') & (df['DATANASCITALAVORATORE'] <= '1980-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1976-01-01') & (df['DATANASCITALAVORATORE'] <= '1976-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1972-01-01') & (df['DATANASCITALAVORATORE'] <= '1972-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1968-01-01') & (df['DATANASCITALAVORATORE'] <= '1968-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1964-01-01') & (df['DATANASCITALAVORATORE'] <= '1964-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1960-01-01') & (df['DATANASCITALAVORATORE'] <= '1960-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1956-01-01') & (df['DATANASCITALAVORATORE'] <= '1956-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1952-01-01') & (df['DATANASCITALAVORATORE'] <= '1952-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1948-01-01') & (df['DATANASCITALAVORATORE'] <= '1948-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1944-01-01') & (df['DATANASCITALAVORATORE'] <= '1944-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1940-01-01') & (df['DATANASCITALAVORATORE'] <= '1940-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1936-01-01') & (df['DATANASCITALAVORATORE'] <= '1936-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1932-01-01') & (df['DATANASCITALAVORATORE'] <= '1932-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1928-01-01') & (df['DATANASCITALAVORATORE'] <= '1928-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1924-01-01') & (df['DATANASCITALAVORATORE'] <= '1924-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1920-01-01') & (df['DATANASCITALAVORATORE'] <= '1920-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1916-01-01') & (df['DATANASCITALAVORATORE'] <= '1916-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1912-01-01') & (df['DATANASCITALAVORATORE'] <= '1912-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1908-01-01') & (df['DATANASCITALAVORATORE'] <= '1908-31-12')
df.loc[mask,'COEF'] = 1
mask = (df['DATANASCITALAVORATORE'] > '1904-01-01') & (df['DATANASCITALAVORATORE'] <= '1904-31-12')
df.loc[mask,'COEF'] = 1

# Assign values to coefficients
df.COEF.replace(1, 366/12, inplace = True)
df.COEF.replace(0, 365/12, inplace = True)

# Descriptive statistics
# ----------------------
df.describe()
# Show first 5 rows of the dataframe
df.head()

# Subsets
# -------
start_time = time.time()

# Generate a subset of DataFrames clustered based on the initial values of 'ID_LAVORATORE'
df['ID'] = df['ID_LAVORATORE'].astype(np.str)
df_0 = df[df['ID'].str.match('0')]
df_1 = df[df['ID'].str.match('1')]
df_2 = df[df['ID'].str.match('2')]
df_3 = df[df['ID'].str.match('3')]
df_4 = df[df['ID'].str.match('4')]
df_5 = df[df['ID'].str.match('5')]
df_6 = df[df['ID'].str.match('6')]
df_7 = df[df['ID'].str.match('7')]
df_8 = df[df['ID'].str.match('8')]
df_9 = df[df['ID'].str.match('9')]

# Eliminate empty - or useless - DataFrames  
del df, df_att, df_mis, df_som
df_names = ['df_0','df_1','df_2','df_3','df_4','df_5','df_6','df_7','df_8','df_9']
for df_ in df_names:
    if locals()[df_].empty:
        del locals()[df_]
  
# List of the remaining DataFrames    
print('\n Remaining sub-DataFrames:')
alldfs = [var for var in dir() if isinstance(eval(var), pd.core.frame.DataFrame)]
for i in alldfs:
    if i[:1] != '_':
        print (i)

# Required time
e = int(time.time() - start_time)
elapsed_time = time.time() - start_time
print('\n Subsets')
print('- required time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
        
# Connect to MongoDB and clean existing collections
# -------------------------------------------------
client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["assolavoro"]

# Eliminate if the collection of interest already exists
'somministrazioni' in db.list_collection_names()     
collection = db['somministrazioni']
collection.estimated_document_count() == 0
collection.drop()  

col = db['somministrazioni']
print('\n Connected to MongoDB')  
        
# Import data into MongoDB
# ------------------------
# Select the directory from which derive the script
os.chdir("C:/Users/Nicola Caravaggio/OneDrive/Desktop/Ciu_Tos_Roma3/Somministrazioni/MongoDB") 
start_time = time.time()

print('\n DataFrame df_0:')
try: 
    df = df_0
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

print('\n DataFrame df_1:')
try: 
    df = df_1
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

print('\n DataFrame df_2:')
try: 
    df = df_2
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

print('\n DataFrame df_3:')
try: 
    df = df_3
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

print('\n DataFrame df_4:')
try: 
    df = df_4
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

print('\n DataFrame df_5:')
try: 
    df = df_5
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

print('\n DataFrame df_6:')
try: 
    df = df_6
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

print('\n DataFrame df_7:')
try: 
    df = df_7
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

print('\n DataFrame df_8:')
try: 
    df = df_8
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

print('\n DataFrame df_9:')
try: 
    df = df_9
    exec(open("import_somministrazioni_into_mongodb.py").read());
except NameError: print("No var")

# Required time
e = int(time.time() - start_time)
elapsed_time = time.time() - start_time
print('\n Import data into MongoDB')
print('- required time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

# Adjustments of the MongoDB collection
# -------------------------------------
start_time = time.time()

# Convert 'DATANASCITALAVORATORE' into ISODate format
client = pymongo.MongoClient('localhost', 27017)
db = client['assolavoro']
somministrazioni = db['somministrazioni']
for item in db.somministrazioni.find():
    if 'DATANASCITALAVORATORE' in item:
        item['DATANASCITALAVORATORE'] = datetime.datetime.strptime(item['DATANASCITALAVORATORE'], '%Y-%m-%d')
        db.somministrazioni.replace_one({'_id': item['_id']}, item)
client.close()
#print("Query correctly read")        

# Unwind 'TIPOLOGIA_LAVORO'
client = pymongo.MongoClient('localhost', 27017)
db = client['assolavoro']
somministrazioni = db['somministrazioni']
pipe = [{ '$unwind' : "$TIPOLOGIA_LAVORO" }, { '$out': 'somministrazioni' }]
TestOutput = somministrazioni.aggregate(pipeline = pipe)
client.close()
#print(list(TestOutput))
#print("Query correctly read")

# Create the element 'ATTIVAZIONI' with ISODate      
client = pymongo.MongoClient('localhost', 27017)
db = client['assolavoro']
somministrazioni = db['somministrazioni']
pipe = [{
        '$addFields': {
                'ATTIVAZIONI': {
                        '$map': {
                                'input': '$TIPOLOGIA_LAVORO.Attivazione',
                                'as': 'att',
                                'in': {
                        'ID_DATORELAVORO': '$$att.ID_DATORELAVORO',  
                        'CODCOMUNEDOMICILIO': '$$att.CODCOMUNEDOMICILIO', 
                        'CODREGIONEDOMICILIO': '$$att.CODREGIONEDOMICILIO',
                        'CODCOMUNELAVORO': '$$att.CODCOMUNELAVORO', 
                        'CODPROVINCIALAVORO': '$$att.CODPROVINCIALAVORO', 
                        'CODREGIONELAVORO': '$$att.CODREGIONELAVORO', 
                        
                        'RAPPORTO_DATAINIZIO': 
                            { '$toDate': { '$substr': [ '$$att.RAPPORTO_DATAINIZIO', 0, { '$subtract': [ {
                                '$strLenCP': '$$att.RAPPORTO_DATAINIZIO' }, 0 ] } ] } },

                        'DTCESSAZIONEEFFETTIVA': 
                            { '$cond': [ { '$eq': [ "$$att.DTCESSAZIONEEFFETTIVA", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTCESSAZIONEEFFETTIVA', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTCESSAZIONEEFFETTIVA' }, 0 ] } ] } }
                                 ] },
    
                        'CODMOTIVOCESSAZIONECO': '$$att.CODMOTIVOCESSAZIONECO', 
                        
                        'DTFINEPREVISTA': 
                            { '$cond': [ { '$eq': [ "$$att.DTFINEPREVISTA", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTFINEPREVISTA', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTFINEPREVISTA' }, 0 ] } ] } }
                                 ] },

                        'DTFINEPREVISTA_ULTIMA_PROROGA': 
                            { '$cond': [ { '$eq': [ "$$att.DTFINEPREVISTA_ULTIMA_PROROGA", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTFINEPREVISTA_ULTIMA_PROROGA', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTFINEPREVISTA_ULTIMA_PROROGA' }, 0 ] } ] } }
                                 ] },

                        'DTTRASFORMAZIONE': 
                            { '$cond': [ { '$eq': [ "$$att.DTTRASFORMAZIONE", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTTRASFORMAZIONE', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTTRASFORMAZIONE' }, 0 ] } ] } }
                                 ] },
 
                        'N_PROROGHE': '$$att.N_PROROGHE', 
                        'CODSETTORE': '$$att.CODSETTORE',  
                        'CODTIPOCONTRATTO': '$$att.CODTIPOCONTRATTO',
                        'CODTIPOORARIO': '$$att.CODTIPOORARIO',
                        'CODQUALIFICAPROFESSIONALE': '$$att.CODQUALIFICAPROFESSIONALE',
                        'CODCCNL': '$$att.CODCCNL',
                        'CODTIPOTRASFORMAZIONE': '$$att.CODTIPOTRASFORMAZIONE',
                        'CLASSIFICAZIONEPROFESSIONE': '$$att.CLASSIFICAZIONEPROFESSIONE',
                        'FONTE': '$$att.FONTE',
                        'TIPOLOGIA': '$$att.TIPOLOGIA'                               
    } } } } }, {
        '$out': 'somministrazioni'
    }]
TestOutput = somministrazioni.aggregate(pipeline = pipe)
client.close()
#print(list(TestOutput))
#print("Query correctly read")

# Create the element 'MISSIONI' with ISODate      
client = pymongo.MongoClient('localhost', 27017)
db = client['assolavoro']
somministrazioni = db['somministrazioni']
pipe = [{
        '$addFields': {
                'MISSIONI': {
                        '$map': {
                                'input': '$TIPOLOGIA_LAVORO.Missione',
                                'as': 'att',
                                'in': {
                        'ID_DATORELAVORO': '$$att.ID_DATORELAVORO',  
                        'CODCOMUNEDOMICILIO': '$$att.CODCOMUNEDOMICILIO', 
                        'CODREGIONEDOMICILIO': '$$att.CODREGIONEDOMICILIO',
                        'CODCOMUNELAVORO': '$$att.CODCOMUNELAVORO', 
                        'CODPROVINCIALAVORO': '$$att.CODPROVINCIALAVORO', 
                        'CODREGIONELAVORO': '$$att.CODREGIONELAVORO', 
                        
                        'RAPPORTO_DATAINIZIO': 
                            { '$toDate': { '$substr': [ '$$att.RAPPORTO_DATAINIZIO', 0, { '$subtract': [ {
                                '$strLenCP': '$$att.RAPPORTO_DATAINIZIO' }, 0 ] } ] } },

                        'DTCESSAZIONEEFFETTIVA': 
                            { '$cond': [ { '$eq': [ "$$att.DTCESSAZIONEEFFETTIVA", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTCESSAZIONEEFFETTIVA', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTCESSAZIONEEFFETTIVA' }, 0 ] } ] } }
                                 ] },
    
                        'CODMOTIVOCESSAZIONECO': '$$att.CODMOTIVOCESSAZIONECO', 
                        
                        'DTFINEPREVISTA': 
                            { '$cond': [ { '$eq': [ "$$att.DTFINEPREVISTA", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTFINEPREVISTA', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTFINEPREVISTA' }, 0 ] } ] } }
                                 ] },

                        'DTFINEPREVISTA_ULTIMA_PROROGA': 
                            { '$cond': [ { '$eq': [ "$$att.DTFINEPREVISTA_ULTIMA_PROROGA", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTFINEPREVISTA_ULTIMA_PROROGA', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTFINEPREVISTA_ULTIMA_PROROGA' }, 0 ] } ] } }
                                 ] },

                        'DTTRASFORMAZIONE': 
                            { '$cond': [ { '$eq': [ "$$att.DTTRASFORMAZIONE", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTTRASFORMAZIONE', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTTRASFORMAZIONE' }, 0 ] } ] } }
                                 ] },
 
                        'N_PROROGHE': '$$att.N_PROROGHE', 
                        'CODSETTORE': '$$att.CODSETTORE',  
                        'CODTIPOCONTRATTO': '$$att.CODTIPOCONTRATTO',
                        'CODTIPOORARIO': '$$att.CODTIPOORARIO',
                        'CODQUALIFICAPROFESSIONALE': '$$att.CODQUALIFICAPROFESSIONALE',
                        'CODCCNL': '$$att.CODCCNL',
                        'CODTIPOTRASFORMAZIONE': '$$att.CODTIPOTRASFORMAZIONE',
                        'CLASSIFICAZIONEPROFESSIONE': '$$att.CLASSIFICAZIONEPROFESSIONE',
                        'FONTE': '$$att.FONTE',
                        'TIPOLOGIA': '$$att.TIPOLOGIA'                               
    } } } } }, {
        '$out': 'somministrazioni'
    }]
TestOutput = somministrazioni.aggregate(pipeline = pipe)
client.close()
#print(list(TestOutput))
#print("Query correctly read")

# Create the element 'SOMMINISTRAZIONI' with ISODate      
client = pymongo.MongoClient('localhost', 27017)
db = client['assolavoro']
somministrazioni = db['somministrazioni']
pipe = [{
        '$addFields': {
                'SOMMINISTRAZIONI': {
                        '$map': {
                                'input': '$TIPOLOGIA_LAVORO.Somministrazione',
                                'as': 'att',
                                'in': {
                        'ID_DATORELAVORO': '$$att.ID_DATORELAVORO',  
                        'CODCOMUNEDOMICILIO': '$$att.CODCOMUNEDOMICILIO', 
                        'CODREGIONEDOMICILIO': '$$att.CODREGIONEDOMICILIO',
                        'CODCOMUNELAVORO': '$$att.CODCOMUNELAVORO', 
                        'CODPROVINCIALAVORO': '$$att.CODPROVINCIALAVORO', 
                        'CODREGIONELAVORO': '$$att.CODREGIONELAVORO', 
                        
                        'RAPPORTO_DATAINIZIO': 
                            { '$toDate': { '$substr': [ '$$att.RAPPORTO_DATAINIZIO', 0, { '$subtract': [ {
                                '$strLenCP': '$$att.RAPPORTO_DATAINIZIO' }, 0 ] } ] } },

                        'DTCESSAZIONEEFFETTIVA': 
                            { '$cond': [ { '$eq': [ "$$att.DTCESSAZIONEEFFETTIVA", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTCESSAZIONEEFFETTIVA', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTCESSAZIONEEFFETTIVA' }, 0 ] } ] } }
                                 ] },
    
                        'CODMOTIVOCESSAZIONECO': '$$att.CODMOTIVOCESSAZIONECO', 
                        
                        'DTFINEPREVISTA': 
                            { '$cond': [ { '$eq': [ "$$att.DTFINEPREVISTA", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTFINEPREVISTA', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTFINEPREVISTA' }, 0 ] } ] } }
                                 ] },

                        'DTFINEPREVISTA_ULTIMA_PROROGA': 
                            { '$cond': [ { '$eq': [ "$$att.DTFINEPREVISTA_ULTIMA_PROROGA", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTFINEPREVISTA_ULTIMA_PROROGA', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTFINEPREVISTA_ULTIMA_PROROGA' }, 0 ] } ] } }
                                 ] },

                        'DTTRASFORMAZIONE': 
                            { '$cond': [ { '$eq': [ "$$att.DTTRASFORMAZIONE", "NaT" ] }, "NaT", 
                                { '$toDate': { '$substr': [ '$$att.DTTRASFORMAZIONE', 0, { '$subtract': [ {
                                    '$strLenCP': '$$att.DTTRASFORMAZIONE' }, 0 ] } ] } }
                                 ] },
 
                        'N_PROROGHE': '$$att.N_PROROGHE', 
                        'CODSETTORE': '$$att.CODSETTORE',  
                        'CODTIPOCONTRATTO': '$$att.CODTIPOCONTRATTO',
                        'CODTIPOORARIO': '$$att.CODTIPOORARIO',
                        'CODQUALIFICAPROFESSIONALE': '$$att.CODQUALIFICAPROFESSIONALE',
                        'CODCCNL': '$$att.CODCCNL',
                        'CODTIPOTRASFORMAZIONE': '$$att.CODTIPOTRASFORMAZIONE',
                        'CLASSIFICAZIONEPROFESSIONE': '$$att.CLASSIFICAZIONEPROFESSIONE',
                        'FONTE': '$$att.FONTE',
                        'TIPOLOGIA': '$$att.TIPOLOGIA'                               
    } } } } }, {
        '$out': 'somministrazioni'
    }]
TestOutput = somministrazioni.aggregate(pipeline = pipe)
client.close()
#print(list(TestOutput))
#print("Query correctly read")

# Create the element 'CONTRATTI' as aggregation of Attivazioni and Missioni
client = pymongo.MongoClient('localhost', 27017)
db = client['assolavoro']
somministrazioni = db['somministrazioni']
pipe = [{
        '$addFields': {
                'CONTRATTI': {
                        'ATTIVAZIONI': '$ATTIVAZIONI', 
                        'MISSIONI': '$MISSIONI',
                        'SOMMINISTRAZIONI': '$SOMMINISTRAZIONI'
                } } }, {
        '$out': 'somministrazioni'
    }]
TestOutput = somministrazioni.aggregate(pipeline = pipe)
client.close()
#print(list(TestOutput))
#print("Query correctly read")

# Unwind 'CONTRATTI'
client = pymongo.MongoClient('localhost', 27017)
db = client['assolavoro']
somministrazioni = db['somministrazioni']
pipe = [{ '$unwind' : "$CONTRATTI" }, {'$out': 'somministrazioni' }]
TestOutput = somministrazioni.aggregate(pipeline = pipe)
client.close()
#print(list(TestOutput))
#print("Query correctly read")

# Eliminate useless fields
client = pymongo.MongoClient('localhost', 27017)
db = client['assolavoro']
somministrazioni = db['somministrazioni']
somministrazioni.update_many({}, {'$unset': {
        'TIPOLOGIA_LAVORO':1, 
        'ATTIVAZIONI':1, 
        'MISSIONI':1,
        'SOMMINISTRAZIONI':1}})
client.close()
#print(list(TestOutput))
#print("Query correctly read")

# Required time
e = int(time.time() - start_time)
elapsed_time = time.time() - start_time
print('\n Adjustments of the MongoDB collection')
print('- required time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))