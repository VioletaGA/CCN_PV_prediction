
###########################################################
#Autor: Violeta Gracia
#Fecha: 09/2023
############################################################
#Filtrar información descargada de producción solar de Red Eléctrica localiza en 
#/Datos/extraccion_produccion.csv
#Generar nuevo archivo en 'Datos/produccion_por_provincia_y_fecha.json'
# WARNING: Ejecutar .py en Codigo/Produccion Red Electrica/Tratamiento produccion
############################################################

import requests
import json
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
import numpy as np
import random
import sys
import csv
import requests
import os
import time
from tqdm import tqdm
import pytz
import ephem
import datetime
os.chdir("../../../")

# Carga datos de la producción solar de Red eléctrica extraidos en el código extraccion_produccion.py (extraccion_produccion.csv)
# Creamos un nuevo archivo  con la información filtrada por las horas de día
input_file='Datos/extraccion_produccion.csv'#'Produccion Red Electrica/Extraccion produccion/extraccion_produccion.csv'
data_df = pd.read_csv(input_file, sep=";", header=0)

data_df['datetime']=pd.to_datetime(data_df['datetime'])
data_df=data_df[data_df['datetime']<pd.to_datetime('2021-01-01 00:00:00+01:00')]
data_df=data_df.sort_values(by=['datetime','geoid'], ascending=True)

def franja_dia(fecha):

    """
    Determina si una fecha dada se encuentra dentro de la franja de luz solar en Cap de Creus y Cabo do Touriñan.

    :param fecha: La fecha y hora para comprobar si está dentro de la franja de luz solar.
    :return: True si la fecha está dentro de la franja de luz solar, False en caso contrario.
    """

    # Configura la zona horaria de Madrid
    madrid_tz = pytz.timezone('Europe/Madrid')

    cap_de_Creus=['42.3253', '3.2889']
    cabo_do_Toruñan=['42.8829', '-9.2894']
    fecha = fecha.astimezone(madrid_tz)

    # Crea un objeto Observer para Madrid
    observer_este = ephem.Observer()
    observer_este.lat = cap_de_Creus[0]
    observer_este.long = cap_de_Creus[1]
    observer_este.elev = 0  # Altitud en metros (aquí se asume nivel del mar)

    # Calcula el amanecer y el atardecer para la fecha actual
    sunrise = observer_este.previous_rising(ephem.Sun(),start=fecha)

    # Crea un objeto Observer para Madrid
    observer_oeste = ephem.Observer()
    observer_oeste.lat = cabo_do_Toruñan[0]
    observer_oeste.long = cabo_do_Toruñan[1]
    observer_oeste.elev = 0  # Altitud en metros (aquí se asume nivel del mar)
    sunset =  observer_oeste.next_setting(ephem.Sun(), start=fecha)

    sunrise = madrid_tz.localize(ephem.localtime(sunrise))
    sunset = madrid_tz.localize(ephem.localtime(sunset))

    return  (sunrise.day ==  fecha.day and  sunset.day ==  fecha.day)

try:
  data_df=data_df[data_df['datetime'].apply(franja_dia)]
except Exception as e:
  print(data_df['datetime'] , 'Error: ' , str(e))

data_filtrado_df=data_df.loc[:,['id',	'geoid',	'geoname',	'value',	'datetime']]


# Crea un json con un valor de producción por hora en formato diccionario con 47 valores, uno para cada provincia
def obtener_dataset_produccion(fechas, output_file):
  """
  Crea un dataset con un registro de producción por fecha con 47 valores y lo guarda en un archivo JSON.

  :param fechas: Las fechas para las cuales se obtendrán los datos de producción.
  :param output_file: El nombre del archivo de salida donde se guardará el dataset en formato JSON.
  :return: Un DataFrame con el dataset de producción generado.
  """

  array_append=[]

  geoname_distintos = data_filtrado_df['geoname'].unique()

  with tqdm(total=len(fechas), desc=str('Creación archivo'),  ncols=100) as pbar:
    df_provincias= pd.DataFrame(geoname_distintos, columns=['geoname'])
    for fecha in fechas:
      try:
        #df con los datos de esa fecha concreta
        df_fecha=data_filtrado_df[data_filtrado_df['datetime']==fecha] #fecha en formato string

        cantidad_provincias=len(df_fecha) #cantidad de provincias con datos
        df_fecha = pd.merge(df_provincias,df_fecha,on=['geoname'], how='outer').fillna(-999).sort_values(by=['geoname'], ascending=True) #cross join de los datos con todas las provincias, -999 cuando no hay dato
        provincias=df_fecha['geoname'].T.to_list()
        produccion=df_fecha['value'].T.to_list()

        #generamos un df_auxiliar con los datos tratados para esta fecha
        df_aux= pd.DataFrame()
        df_aux['fecha_produccion'] = [fecha]
        df_aux['cantidad_provincias']=[cantidad_provincias]
        df_aux['provincias']= [provincias]
        df_aux['produccion']= [produccion]
        array_append.append(df_aux)

      except Exception as e:
          print("Error en fecha: ", fecha, 'Error: ', str(e))
      pbar.update(1)

  try:
    df_final=pd.concat(array_append).reset_index(drop=True).sort_values(by=['fecha_produccion'], ascending=True)
    df_final.to_json(output_file)
  except Exception as e:
    print("Error generando df último y guardado de json. Error: ", str(e))

  return df_final

fechas=data_filtrado_df['datetime'].unique()
output_file='Datos/produccion_por_provincia_y_fecha.json'
obtener_dataset_produccion(fechas, output_file)
