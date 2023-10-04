###########################################################
#Autor: Violeta Gracia
#Fecha: 09/2023
############################################################
#Extrae información de las fronteras dr los municipios de España y los procesa
#Fuente de información: https://public.opendatasoft.com/explore/dataset/georef-spain-municipio/table/?disjunctive.acom_code&disjunctive.acom_name&disjunctive.prov_code&disjunctive.prov_name&disjunctive.mun_code&disjunctive.mun_name'
############################################################

import requests
import os
import csv
import pandas as pd
import json

URL_MUNICIPIOS="https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-spain-municipio/exports/csv?lang=en&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"

def descargar_municipios_df(nombre_archivo_local,url =URL_MUNICIPIOS ):
# Descargar las coordenadas de cada municipio y término municipal. Información en la url indicada
# Obtenemos para todos los municipios peninsulares, sus coordenadas y las fronteras de sus términos municipales
    # Nombre local del archivo es el lugar donde se guarda el csv
    if not os.path.exists(nombre_archivo_local):
        # Realiza la solicitud GET para descargar el archivo
        response = requests.get(url)
        # Verifica si la solicitud fue exitosa (código de estado 200)
        if response.status_code == 200:
            # Abre el archivo local en modo binario y escribe los datos descargados
            with open(nombre_archivo_local, 'wb') as archivo_local:
                archivo_local.write(response.content)
            print(f'Archivo descargado como "{nombre_archivo_local}"')
        else:
            print('La descarga falló. Código de estado:', response.status_code)
            return None
        
    maxsyze=2*131072
    csv.field_size_limit(maxsyze)
    df_fronteras=pd.read_csv(nombre_archivo_local,sep=';',engine='python',encoding='utf-8', on_bad_lines='warn')

    #quitamos las islas y ceuta y melilla
    df_fronteras = df_fronteras[df_fronteras['Official Code Autonomous Community']!=4] #Baleares
    df_fronteras = df_fronteras[df_fronteras['Official Code Autonomous Community']!=5] #Canarias
    df_fronteras = df_fronteras[df_fronteras['Official Code Autonomous Community']!=18] #Ceuta
    df_fronteras = df_fronteras[df_fronteras['Official Code Autonomous Community']!=19] #Melilla
    df_fronteras = df_fronteras[df_fronteras['Official Code Autonomous Community']!=20] #Otros
    df_fronteras = df_fronteras.reset_index(drop=True)
    
    for index, row in df_fronteras.iterrows():
        #pasamos el campo string de Geo Shape a un diccionario
        res=json.loads(row['Geo Shape'])
        #los exclaves en los municipios se tratan con distintos tipos
        #de list en los datos, con esto tratamos de enmendar el entuerto
        if len(res['coordinates'])==1:
            df_fronteras.at[index,'Geo Shape']=res['coordinates']
        else:
            coordinates=[]
            longitud_maxima = max(len(exclave) for exclave in res['coordinates'])
            if longitud_maxima<30:
                for i in res['coordinates']:                
                    coordinates.append(i[0])
                df_fronteras.at[index,'Geo Shape']=coordinates
            else:
                df_fronteras.at[index,'Geo Shape']=res['coordinates']
    return df_fronteras