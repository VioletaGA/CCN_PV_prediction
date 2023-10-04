
###########################################################
#Autor: Violeta Gracia
#Fecha: 09/2023
############################################################
#Extraer información de datos meteorológicos
#Fuente de información: https://re.jrc.ec.europa.eu/pvg_tools/en/#TMY
#Información de la API utilizada: https://re.jrc.ec.europa.eu/api/v5_2/seriescalc,
############################################################

import requests
import pandas as pd
import os
import time
from tqdm import tqdm


def descargar_json_municipios(df_fronteras,output_path,step=100,url='https://re.jrc.ec.europa.eu/api/v5_2/seriescalc'):
    # toma los datos del dataframe df fronteras y descarga los datos de 
    # irradiancia, temperatura, velocidad del viento y altura del sol para cada Geo Point
    # de 2015 a 2020. Los datos son guardados en archivos .json con hasta step municipios
    # cuyo nombre es automáticamente generado como  'irradianciaMunicipio_indice+primero-indice+ultimo+1.json'
    # devuelve una lista de los archivos generados
    inicio=0
    length=len(df_fronteras)
    tiempo_de_espera_minimo = 2
    df_irrad=df_fronteras[['Geo Point','Official Name Municipality','Official Code Municipality']]
    #Realizamos compartimentos de 100 municipios, en un total de 8066, crea 81 compartimentos
    compartimentos=range(inicio, length, step)
    archivos_de_salida=[]
    if not os.path.isdir(output_path):
        os.makedirs(output_path)
        
    for i in compartimentos:
        #si compartimento no es el ultimo (8000)
        if i != compartimentos[-1]:
            final=i+step #ultimo municipio que coges: 0+100, 100+100, 200+100, ..., 7900+100
            df_irrad_truc=df_irrad.iloc[i:final] #el 100 no lo coge porque es [100]=99
        #si compartimento es el ultimo = 8000, entonces hay que coger de 8000 al final, 8066
        else:
            final=length+1
            df_irrad_truc=df_irrad.iloc[i:]

        #df_irrad_truc --> dataframe con los 100 municipios
        df_irrad_truc=df_irrad_truc.reset_index(drop=True)
        archivo_salida=os.path.join(output_path,'irradianciaMunicipio'+str(i)+'-'+str(final-1)+'.json')
        for j in tqdm(range(len(df_irrad_truc)),  desc='Descargando datos de Municipios'):
            tiempo_inicio = time.time()

            #i=0: 0+1, , 0+100
            #i=100 100+1, 100+1
            #...
            #i=8000 8000+1, 8000+66
            coordenadas=df_fronteras.at[i+j,'Geo Point']
            lat,lon=coordenadas.split(', ')
            params={
                'lat':lat,
                'lon':lon,
                'startyear':2015,
                'showtemperatures':1,
                'outputformat':'json'
            }
            response=requests.get(url,params=params)
            if response.status_code == 200:
                # Obtener los datos JSON de la respuesta
                data = response.json()
                # Ahora puedes trabajar con los datos contenidos en 'data'
                df_data=pd.DataFrame(data['outputs']['hourly'])
                df_irrad_truc.loc[j,'Irradiancia']=df_data.to_json(orient='records')
            else:
                print('La solicitud no se completó con éxito. Código de respuesta:', response.status_code)

            tiempo_transcurrido = time.time() - tiempo_inicio
            tiempo_de_espera_restante = max(0, tiempo_de_espera_minimo - tiempo_transcurrido)

            # Agrega una pausa de tiempo_de_espera_restante segundos
            time.sleep(tiempo_de_espera_restante)
        df_irrad_truc.to_json(archivo_salida, orient='records')
        archivos_de_salida.append(archivo_salida)
        print('Datos de '+str(i)+'-'+str(final-1)+' obtenidos.')
    return archivos_de_salida
