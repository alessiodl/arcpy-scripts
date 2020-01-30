import arcpy
import pandas as pd
import os
import json
import time

# OVERWRITE OPTION
arcpy.env.overwriteOutput = True
# DATI DI INPUT
inputFolder = os.path.join(os.getcwd(), 'input')
# CONNESSIONE CON DB
with open('config.json') as config_file:
	# Dati di accesso al DB da file di configurazione
    connection_data = json.load(config_file)
	instance = connection_data['VETINFO_PROD']['instance']
	username = connection_data['VETINFO_PROD']['username']
	password = connection_data['VETINFO_PROD']['password']
	# Crea file di connessione
	arcpy.CreateDatabaseConnection_management(arcpy.env.scratchFolder,'conn.sde', "ORACLE", instance, "DATABASE_AUTH", username, password)
	connection = os.path.join(arcpy.env.scratchFolder,'conn.sde')
# TABELLA STRUTTURE
strutture = os.path.join(connection,'VETGIS.SDO_STRUTTURE')
# CONTEGGIO NUMERO DI STRUTTURE
count = 0
with arcpy.da.SearchCursor(strutture,"*") as cursor:
	for row in cursor:
		count += 1
print count
	    
# SELEZIONE POLIGONALE
# Crea uno shapefile a partire da un geojson
with open(os.path.join(inputFolder, 'polygon.json')) as file:
	selection = json.load(file)

for g in selection['features']:
	geom = arcpy.AsShape(g['geometry'])
	arcpy.CopyFeatures_management(geom,os.path.join(arcpy.env.scratchFolder,'selezione.shp'))

# LETTURA FOCOLAI BRUCELLOSI DA FILE EXCEL A PANDAS
inTable = os.path.join(inputFolder,'FOCOLAI_BRC_2019.xls')
df = pd.read_excel(inTable)
aziende = df['AZI_COD_AZIENDA'].values.tolist()
# Splitta la lista in sottoliste di 500 elementi
chunks = [aziende[x:x+500] for x in xrange(0, len(aziende), 500)]
# Crea uno shapefile per ogni chunk di codici aziendali
for chunk in chunks:
	name = str(chunks.index(chunk)+1)
	elenco_aziende = "','".join(chunk)
	where_clause = "COD_AZIENDA IN ('"+elenco_aziende+"')"
	arcpy.Select_analysis(strutture, os.path.join(arcpy.env.scratchFolder,'foc_part_'+name), where_clause)
# UNIONE DEGLI SHAPEFILE PARZIALI IN UN UNICO SHAPEFILE
focolai_partials = []
for root, dirs, files in os.walk(arcpy.env.scratchFolder):
	for filename in files:
		if filename.endswith('.shp') and filename.startswith('foc_'):
			focolai_partials.append(os.path.join(arcpy.env.scratchFolder,filename))

arcpy.Merge_management(focolai_partials,os.path.join(arcpy.env.scratchFolder,'focolai_brucellosi_19'))
# SELEZIONE FOCOLAI AOI
focolaiAOI_selection = arcpy.SelectLayerByLocation_management(os.path.join(arcpy.env.scratchFolder,'focolai_brucellosi_19'),"INTERSECT","selezione")
arcpy.CopyFeatures_management(focolaiAOI_selection,os.path.join(arcpy.env.scratchFolder,'focolai_brucellosi_selection'))
# PULIZIA CARTELLA DAI DATI PARZIALI
for file in focolai_partials:
	arcpy.Delete_management(file)