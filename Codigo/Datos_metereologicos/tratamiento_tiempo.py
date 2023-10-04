
###########################################################
#Autor: Violeta Gracia
#Fecha: 09/2023
###########################################################
#Tratamiento información de datos meteorológicos extraidos en el código \Codigo\Datos metereologicos\Extraccion tiempoextraccion_tiempo.py
#Localizados en \Codigo\Datos metereologicos\Extraccion tiempo\Salida extraccion tiempo
#Creación de nuevos archivos en \Codigo\Datos metereologicos\Tratamiento tiempo\Salida tratamiento tiempo

#Faltaría la parte de código que toma este primer tratamiento de \Codigo\Datos metereologicos\Tratamiento tiempo\Salida tratamiento tiempo
#Y agrupa y ordena por mes guardándolo en \Codigo\Datos metereologicos\Tratamiento tiempo\Salida tratamiento tiempo ordenado
###########################################################

from tqdm import tqdm
import pandas as pd
import csv
import os
import pytz
import ephem
from io import StringIO



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




def obtener_nombre_archivo(directorio_csv, fecha):
    """
    Genera el nombre de un archivo CSV con la ruta del directorio a partir de una fecha dada y un directorio especificado.
    :param directorio_csv: La ruta al directorio donde se ubicará el archivo CSV.
    :param fecha: La fecha para la cual se generará el nombre del archivo.
    :return: El nombre completo del archivo CSV generado, incluyendo la ruta al directorio.
    """
    mes = fecha.month
    año = fecha.year
    return os.path.join(directorio_csv, f'datos_{año}_{mes}.csv')

# Función para procesar un archivo
def procesar_archivo(archivo,output_path):

    """
    Procesa un archivo JSON, extrae datos relevantes y los guarda en archivos CSV.
    :param archivo: El nombre del archivo JSON a procesar.
    :param directorio: La ruta al directorio que contiene el archivo JSON.
    :param output_path: La ruta al directorio donde se guardarán los archivos CSV procesados.
    """

    df_datos = pd.read_json(archivo)
    

    # Asegúrate de que el directorio exista; si no, créalo
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Usemos tqdm para la barra de progreso
    with tqdm(total=len(df_datos), desc='Procesando archivos json') as pbar:
        for i, row in df_datos.iterrows():
            # Guardamos cada municipio
            dict_datos = {}
            try:
                # Cogemos los datos que nos interesan
                json_string = row['Irradiancia']
                df_aux = pd.read_json(StringIO(json_string))
                df_aux['time'] = pd.to_datetime(df_aux['time'], format="%Y%m%d:%H%M", utc=True).dt.tz_convert('Europe/Madrid')
                df_aux = df_aux.loc[df_aux['time'].apply(franja_dia)]
                df_aux['id'] = row['Official Code Municipality']
                # Itera sobre las filas del DataFrame y guarda los datos en archivos CSV
                for indice, fila in df_aux.iterrows():
                    fecha = fila['time']
                    nombre_archivo = obtener_nombre_archivo(output_path,fecha)

                    if nombre_archivo not in dict_datos:
                        dict_datos[nombre_archivo] = []
                    dict_datos[nombre_archivo].append([fecha, fila['G(i)'], fila['H_sun'], fila['T2m'], fila['WS10m'], fila['id']])

                # Escribimos los datos en el csv correspondiente
                for nombre_archivo, datos in dict_datos.items():
                    encabezado= not os.path.exists(nombre_archivo)
                    with open(nombre_archivo, mode='a', newline='') as archivo_csv:
                        escritor_csv = csv.writer(archivo_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        if encabezado:
                            escritor_csv.writerow(['fecha', 'G(i)', 'H_sun', 'T2m', 'WS10m', 'id'])  # Encabezado
                        escritor_csv.writerows(datos)

            except Exception as e:
                print(f'Error en municipio {row["Official Name Municipality"]} {str(e)}',flush=True)
            pbar.update(1)
    print(str(archivo)+' Procesado',flush=True)



def ordenar_directorio_por_fecha(directorio):
    for mes in os.listdir(directorio):
        if mes.endswith('.csv'):
            archivo_a_tratar=os.path.join(directorio,mes)
            datos_desordenados=pd.read_csv(archivo_a_tratar, sep=',')
            datos_ordenados=datos_desordenados.sort_values('fecha')
            datos_ordenados.to_csv(archivo_a_tratar, sep=',',index=False)