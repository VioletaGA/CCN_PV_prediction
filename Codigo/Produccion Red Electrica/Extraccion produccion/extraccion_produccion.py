############################################################
#Autor: Violeta Gracia
#Fecha: 09/2023
############################################################
#Exctracción información de producción solar fotovoltaica de Red Eléctrica ejecutando el archivo .py
#en linea de comandos
#Fuente de información:
#https://www.esios.ree.es/es/analisis/1161?vis=1&start_date=01-06-2023T00%3A00&end_date=13-07-2023T23%3A55&compare_start_date=31-05-2023T00%3A00&groupby=hour&geoids=
#Descarga de csv generado:  'Datos/extraccion_produccion.csv'
# WARNING: Ejecutar .py en Codigo/Produccion Red Electrica/Extraccion produccion
############################################################

#!pip install beautifulsoup4 pandas selenium unidecode

from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from unidecode import unidecode
os.chdir("../../../")

def get_page_content(url, download_directory):
    """
    Descarga una página web y obtiene su contenido.
    :param url: La URL de la página web a descargar.
    :param download_directory: El directorio donde se guardará el archivo descargado.
    :return: El contenido de la página web en formato BeautifulSoup.
    """
    # Configura las opciones de Chrome para la descarga y navegación
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": os.path.abspath(download_directory),  # Ruta donde se guardará el archivo descargado
        "download.prompt_for_download": False,  # Desactiva el diálogo de descarga
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    # Inicia una instancia de Chrome con las opciones configuradas
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)

    # Espera a que la página se cargue completamente
    wait = WebDriverWait(driver, 25)
    wait.until(EC.presence_of_element_located((By.ID, 'analysis-form-geo-id')))

    # Espera implícitamente para asegurarse de que todos los elementos se carguen correctamente
    driver.implicitly_wait(25)

    # Localiza el botón de selección y lo hace clic mediante JavaScript
    selector_button = driver.find_element(By.XPATH, '//*[@id="export_multiple"]')
    driver.execute_script("arguments[0].click();", selector_button)

    # Localiza el botón de descarga y lo hace clic mediante JavaScript
    download_button = driver.find_element(By.XPATH, '/html/body/main/div[3]/div/div/div/div[6]/div[1]/div[8]/div/div/div/div[1]')
    driver.execute_script("arguments[0].click();", download_button)

    # Espera un tiempo suficiente para que se complete la descarga
    time.sleep(5)

    # Devuelve el código de la página
    soup=driver.page_source
    # Cierra el navegador
    driver.quit()

    # Devuelve el contenido de la página
    return BeautifulSoup(soup, 'html.parser')

# Cargamos de un csv7 el nombre de las provincias con el correspondiente goid que asocia Red Eléctrica a las provincias
geoid = 'Codigo/Produccion Red Electrica/Extraccion produccion/goid_provincias.csv'
provincias = pd.read_csv(geoid,header=None)
prov = provincias.to_numpy()

def scrapping_red_electrica(startDate,endDate,year,download_path):
    """
    Realiza web scraping de datos relacionados con la producción eléctrica.
    De la url: https://www.esios.ree.es/es/analisis/1161?vis=1&start_date=

    :param startDate: La fecha de inicio para la extracción de datos (en formato YYYY-MM-DD).
    :param endDate: La fecha de fin para la extracción de datos (en formato YYYY-MM-DD).
    :param year: El año correspondiente a la extracción de datos.
    :param download_path: La ruta al directorio donde se guardarán los archivos descargados.
    """
    nombre_carpeta = year
    download_directory = os.path.join(download_path, nombre_carpeta)
    
    for id in prov:
        try:
            geoid = str(id[0])
            url = 'https://www.esios.ree.es/es/analisis/1161?vis=1&start_date='
            url=url+ startDate +'T00%3A00&end_date='+ endDate + 'T23%3A55'
            url=url+'&geoids=' + str(geoid) + '&groupby=hour#'
            download_fold=os.path.join(download_directory, unidecode(str(id[1]))+'-'+ startDate+'-'+ endDate)
            # Obtiene el contenido de la página web y guarda archivos en el directorio especificado
            page_content = get_page_content(url,download_fold)

                # Imprime información o realiza otras acciones con los datos procesados
            print(year + '-' + str(id[1]))
        except Exception as e:
            print ("Error: " + str(e))


# Parámetros sobre los que se va a iterar la función scrapping_red_electrica(startDate,endDate,year,download_path)
fechas=[['01-01-2015','31-12-2015'], ['01-01-2016','31-12-2016'],
        ['01-01-2017','31-12-2017'], ['01-01-2018','31-12-2018'],
        ['01-01-2019','31-12-2019'], ['01-01-2020','31-12-2020'],
        ['01-01-2021','31-12-2021'], ['01-01-2022','31-12-2022']]
years=['2015','2016','2017','2018','2019','2020','2021','2022']
download_path = 'Datos/Extraccion produccion/scrapping'
if not os.path.isdir(download_path):
    os.makedirs(download_path)

for i in range(len(fechas)):
    startDate=fechas[i][0]
    endDate=fechas[i][1]
    year=years[i]

    try:
        scrapping_red_electrica(startDate,endDate,year,download_path)
    except Exception as e:
        print (F"Error: " + str(e))

# Unimos los diversos resultados en un único archivo
input_path = download_path
output_path = 'Datos/extraccion_produccion.csv'

import os
import pandas as pd
years = os.listdir(input_path)

resultado = []
for year in years:
    print("Año: " + year)
    input_year_path = os.path.join(input_path, year)
    provincias = os.listdir(input_year_path)
    for provincia in provincias:
        input_provincia_path = os.path.join(input_year_path, provincia)
        archivo = os.listdir(input_provincia_path)[0]
        archivo_path = os.path.join(input_provincia_path, archivo)
        try:
            join_df = pd.read_csv(archivo_path, sep=";")
        except Exception as e:
            print('Error: ' + str(e))
        resultado.append(join_df)

resultado_df = pd.concat(resultado)

# Output file:
try:
    resultado_df.to_csv(output_path, sep=";", header=True, index=False)
except Exception as e:
    print("Error generando csv . Error: " + str(e))
