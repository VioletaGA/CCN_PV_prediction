
############################################################
#Autor: Violeta Gracia 
#Fecha: 09/2023 
############################################################
#Toma los datos meteorológicos tratados y ordenados del directorio \Codigo\Datos metereologicos\Tratamiento tiempo\Salida tratamiento tiempo ordenado
#Crea las imágenes y las guarda en el directorio \Codigo\Datos metereologicos\Imagenes\salida_imagenes_256
############################################################

import pandas as pd
import matplotlib.pyplot as plt
import os
from tqdm import tqdm





# Obtenidos previamente los valores max y min de temperatura, irradiancia y elevación del sol
min_Gi_final=0
max_Gi_final=1150

min_H_final=0
max_H_final=78

min_T2_final=-21
max_T2_final=46

min_WS_final=0
max_WS_final=38




def map_color(valor1, valor2, valor3):
    """
    Mapea valores numéricos a un color RGB en función de tres variables.

    :param valor1: El primer valor numérico.
    :param valor2: El segundo valor numérico.
    :param valor3: El tercer valor numérico.
    :return: Una tupla que representa un color RGB calculado a partir de los valores proporcionados.
    """
    r=(valor3-min_T2_final)/(max_T2_final-min_T2_final)
    g=(valor1-min_Gi_final)/(max_Gi_final-min_Gi_final)
    b=(valor2-min_H_final)/(max_H_final-min_H_final)
    return (r,g,b)


def pintar_vacio(df_fronteras, width, height):
    """
    Genera una imagen en blanco con límites geográficos y dibuja fronteras en ella.

    :param df_fronteras: Un DataFrame que contiene información sobre las fronteras geográficas.
    :param width: Ancho de la imagen en píxeles.
    :param height: Alto de la imagen en píxeles.
    :return: Una figura de Matplotlib que representa la imagen en blanco y un diccionario de municipios con sus fronteras.
    """
    #no mostrar ventanas para paralelizar
    #plt.switch_backend('Agg')

    # Definir los límites geográficos
    min_lat, max_lat = 35.9, 43.8 #Punta de Tarifa-0.1 Estaca de Bares+0.1
    min_long, max_long = -9.4, 3.4 #Cabo de Touriñan-0.1 Cabo de Creus+0.1

    # Crear una figura en blanco con Matplotlib
    fig, ax = plt.subplots(figsize=(width / 100, height / 100), dpi=100)
    fig.set_facecolor('black')
    ax.set_xlim(0, width)
    ax.set_ylim(0, height)
    ax.axis('off')
    municipios={}
    errores=[]
    with tqdm(total=len(df_fronteras), desc="Municipios pintados")  as pbar:
        for i, row in df_fronteras.iterrows():
            coordenadas = row['Geo Shape']
            try:
                # Dibujar la frontera en la imagen utilizando las coordenadas del DataFrame
                exclaves=[]
                for longest_front in coordenadas:
                    df = pd.DataFrame(longest_front, columns=['longitud', 'latitud'])
                    # Transformar las coordenadas geográficas a píxeles en la imagen
                    points = []
                    for index, fila in df.iterrows():
                        lat = (fila['latitud'] - min_lat) / (max_lat - min_lat) * height
                        long = (fila['longitud'] - min_long) / (max_long - min_long) * width
                        points.append((long, lat))

                    # Dibujar el polígono en la imagen
                    polygon = plt.Polygon(points, closed=True, facecolor='grey')
                    exclaves.append(polygon)
                    ax.add_patch(polygon)
                municipios[row['Official Code Municipality']]=exclaves
            except Exception as e:
                errores.append(row['Official Code Municipality'])
            pbar.update(1)
        pbar.close()
    plt.tight_layout()
    return fig, municipios


def hora_en_punto(fecha):
    """
    Convierte una fecha y hora dada en una fecha y hora con los minutos, segundos y microsegundos establecidos a cero.

    :param fecha: Una fecha y hora en formato de objeto datetime.
    :return: Una nueva fecha y hora con los minutos, segundos y microsegundos establecidos a cero.
    """
    return(pd.to_datetime(fecha.replace(minute=0, second=0, microsecond=0)))


def pintar_irradiancia(municipios, df_pintar):
    """
    Actualiza los colores de los municipios en un mapa en función de los datos proporcionados en un DataFrame.

    :param municipios: Un diccionario que mapea identificadores de municipios a listas de polígonos que representan sus fronteras.
    :param df_pintar: Un DataFrame que contiene los datos para pintar los municipios.
    :return: Un diccionario que contiene identificadores de municipios con errores si los hubiera durante el proceso.
    """
    municipios_cambiados=municipios.copy()
    municipios_error={}
    for i,row in df_pintar.iterrows():
        try:
            exclaves=municipios_cambiados.pop(row['id'])
            #fill_color=map_valor_a_colormap(row['G(i)'])
            fill_color=map_color(row['G(i)'],row['H_sun'],row['T2m'])
            #fill_color=(1,1,1)
            for polygon in exclaves:
                polygon.set_facecolor(fill_color)
        except Exception as e:
            municipios_error[row['id']]=str(e)

    for id,exclaves in municipios_cambiados.items():
        for polygon in exclaves:
            polygon.set_facecolor('black')
    return municipios_error



def pintar_horas_archivo(path_archivo, output_path,municipios,fig):
    """
    Procesa un archivo de datos, crea imágenes por hora y pinta valores de municipios en esas imágenes.

    :param path_archivo: La ruta al archivo de datos a procesar.
    :param output_path: La ruta de salida donde se guardarán las imágenes generadas.
    :param municipios: Un diccionario que mapea identificadores de municipios a listas de polígonos que representan sus fronteras.
    :param fig: La figura de Matplotlib donde se pintará
    """

    try:
        df_datos=pd.read_csv(path_archivo,sep=',')

        df_horas=df_datos['fecha'].drop_duplicates()
        imagenes=len(df_horas)/2
        hora_inferior=hora_en_punto(pd.to_datetime(df_datos.at[0,'fecha']))
        hora_superior=hora_inferior+pd.DateOffset(hours=1)
        longitud=len(df_datos)
    except Exception as e:
        print("Error en el tratamiento del archivo: " + str(e))

    aux_append=[]

    try:
        with tqdm(total=imagenes, desc='Creando imágenes de '+str(path_archivo),  ncols=80) as pbar:
            for i,row in df_datos.iterrows():
                hora_iteracion=pd.to_datetime(row['fecha']) #revisar q la funcion en puto acepto de entrada to_datetime o sting

                if(hora_iteracion<hora_superior):
                    aux_append.append(row)
                else:
                    #actualizamos el df con la informacion de la hora
                    df_aux=pd.DataFrame(aux_append, columns=df_datos.columns)
                    #pintamos sobre la figura los valores de los municipios y guardamos la imagen con la fecha_inferior
                    pintar_irradiancia(municipios, df_aux)
                    carpeta= hora_inferior.strftime("%Y_%m")
                    path_carpeta=os.path.join(output_path, carpeta)
                    if not os.path.isdir(path_carpeta):
                        os.makedirs(path_carpeta)
                    output_file = hora_inferior.strftime("%Y-%m-%d %H%M%S") + '.png'
                    fig.savefig(os.path.join(path_carpeta, output_file))
                    pbar.update(1)

                    #redefinimos
                    hora_inferior=hora_en_punto(hora_iteracion)
                    hora_superior=hora_inferior+pd.DateOffset(hours=1)
                    #print("Ha saltado el else: ")
                    aux_append=[row]

            #actualizamos el df con la informacion de la hora
            df_aux=pd.DataFrame(aux_append, columns=df_datos.columns)
            #pintamos sobre la figura los valores de los municipios y guardamos la imagen con la fecha_inferior
            pintar_irradiancia(municipios, df_aux)
            carpeta= hora_inferior.strftime("%Y_%m")
            path_carpeta=os.path.join(output_path, carpeta)
            if not os.path.isdir(path_carpeta):
                os.mkdir(path_carpeta)
            output_file = hora_inferior.strftime("%Y-%m-%d %H%M%S") + '.png'
            fig.savefig(os.path.join(path_carpeta, output_file))
            pbar.update(1)

    except Exception as e:
        print("Error generating images: " + str(e))

    


def buscar_datos_por_hora(hora_inicial,df_tratado):
    df_tratado['fecha']=pd.to_datetime(df_tratado['fecha'])
    hora_inicial=pd.to_datetime(hora_inicial)
    hora_inicial=hora_en_punto(hora_inicial)
    print('Se cogen los datos desde las '+ str(hora_inicial)+' hasta las '+ str(hora_inicial+pd.DateOffset(hours=1))  )
    #Obtener los datos a cambiar
    df_datos_a_cambiar= df_tratado.loc[(df_tratado['fecha']>= hora_inicial) & (df_tratado['fecha']< hora_inicial+pd.DateOffset(hours=1))]
    return df_datos_a_cambiar

#Habría que recorrer todos los archivos del input_path e iterar la función pintar_horas_archivo(path_archivo, output_path,municipios,fig)
#Parte de código para ejecutarlo en batch
'''
def pintar_horas_path(file, fig, municipios):
    fig.set_facecolor('black')
    try:
        #para el BASH script
        #pintar_horas_archivo(input_path,file,output_path,municipios,fig)
        pintar_horas_archivo(file,output_path,municipios,fig)
    except Exception as e:
        print("Error pintando el archivo: " + file + " . "+ str(e))

import sys
# Los argumentos se encuentran en sys.argv
# sys.argv[0] es el nombre del script
# sys.argv[1] es el primer argumento
# sys.argv[2] es el segundo argumento, y así sucesivamente

if len(sys.argv) >= 2:
    file =sys.argv[1]
else:
    print("Debes proporcionar los archivos a procesar.")

fig, municipios=pintar_vacio(df_fronteras,256,256)
pintar_horas_path(file,fig, municipios)
'''