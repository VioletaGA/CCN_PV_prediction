
############################################################
#Autor: Violeta Gracia
#Fecha: 09/2023
############################################################
#Cargamos los datos de produccion solar, los directorios de todas las imágenes generadas y la potencia solar instalada en un único dataset
#produccion solar: \Codigo\Produccion Red Electrica\Tratamiento produccion\tratamiento_produccion_json.json
#imagenes: \Codigo\Datos metereologicos\Imagenes\salida_imagenes_256
#potencia instalada: \Codigo\Potencia instalada Red Electrica
#guardamos el nuevo dataset en el directorio: \Codigo\Modelo CNN
############################################################


#!pip install ephem

import pandas as pd
import os




#Cargar datos de produccion solar
input_produccion='Datos\produccion_por_provincia_y_fecha.json'
df_produccion = pd.read_json(input_produccion)
df_produccion['fecha_produccion_formateada'] = pd.to_datetime(df_produccion['fecha_produccion']).apply(lambda x: x.strftime('%Y-%m-%d %H%M%S'))
df_produccion['fecha_produccion_formateada_mes'] = pd.to_datetime(df_produccion['fecha_produccion']).apply(lambda x: x.strftime('%m/%Y'))
primera_columna= df_produccion.pop('fecha_produccion_formateada')
segunda_columna= df_produccion.pop('fecha_produccion_formateada_mes')
df_produccion.insert(1, 'fecha_produccion_formateada', primera_columna)
df_produccion.insert(2, 'fecha_produccion_formateada_mes', segunda_columna)


#Cargar imagenes 
input_imagenes='Datos\Imagenes_generadas'
carpetas = sorted(os.listdir(input_imagenes))

imagenes_path=[]
imagenes_fecha=[]
for carpeta in carpetas:
  carpetas_path=input_imagenes+'/'+carpeta
  if not carpeta.endswith('.ini'):
    imagenes=os.listdir(carpetas_path)
    for imagen in imagenes:
      if imagen.endswith('.png'):
        imagenes_path.append(carpetas_path +'/'+imagen)
        imagenes_fecha.append(imagen.split('.')[0])

df_imagenes_path=pd.DataFrame({'imagenes_path': imagenes_path, 'imagenes_fecha': imagenes_fecha})


#Cargar potencia instalada
input_potencia='Codigo\Potencia instalada Red Electrica'
df = pd.read_csv(input_potencia, sep=";")
df_potencia = df.T.reset_index()
df_potencia.columns = df_potencia.iloc[0]
df_potencia = df_potencia.iloc[1:]
df_potencia.head()


#Unir los tres dataframes
df_data = pd.merge(df_produccion,df_imagenes_path, left_on = ['fecha_produccion_formateada'], right_on=['imagenes_fecha'], how = 'inner',suffixes=('','_COLUMNS_MERGE'))
#Obtenemos los casos en los que tenemos info para las 47 provincias
df_data=df_data[df_data['cantidad_provincias']==47]
df_data_final = pd.merge(df_data,df_potencia, left_on = ['fecha_produccion_formateada_mes'], right_on=['potencia_fecha'], how = 'left',suffixes=('','_COLUMNS_MERGE'))

output_file='Codigo\Modelo CNN\dataset_cnn.json'
df_data_final.to_json(output_file)



