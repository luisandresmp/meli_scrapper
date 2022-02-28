import requests
from bs4 import BeautifulSoup
from pandas import DataFrame
from datetime import datetime
import pandas as pd
import os
from time import sleep
import random
from progress.bar import ChargingBar
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import sqlalchemy as sa
from sqlalchemy import create_engine, text as sa_text
import yaml

# FUNCIONES DE CONEXION

# INICIAR SESION GOOGLE DRIVE
def login(directorio_credenciales):
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(directorio_credenciales)

    if gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile(directorio_credenciales)
    else:
        gauth.Authorize()

    return GoogleDrive(gauth)

def dataConfig(file_config='settings.yaml'):

    with open(file_config, 'r') as config:
        try:
            data_config = yaml.safe_load(config)   
        except yaml.YAMLError as exc:
            print(exc)
    
    return data_config

#sube archivo a GOOGLE DRIVE
def sube_archivo_a_drive (ruta_archivo, id_folder):
    credenciales = login()
    archivo = credenciales.CreateFile({'parents': [{"kind": "drive#fileLink",\
                                                    "id": id_folder}]})
    archivo['title'] = ruta_archivo.split("/")[-1]
    archivo.SetContentFile(ruta_archivo) # error
    archivo.Upload()
    print("Archivo subido a drive")

# FUNCIONES PARA GUARDAR INFORMACION

def load_data(name, data_clean):

    #CONSULTO LA HORA DEL SISTEMA, NOMBRO EL ARCHIVO FINAL Y CREO LA RUTA DEL ARCHIVO POR USUARIO    
    date = datetime.now().strftime('%Y_%m_%d')
    file_name = f'\{name}_{date}.csv'

# os.path.expanduser('~')
    path_desktop = os.path.join(os.path.join(os.environ['HOME']), 'Documents/projects/meli_pipeline')
    path_file = path_desktop + file_name

    if os.path.isdir(path_desktop): 
        data_clean.to_csv(path_file, mode='a', header=False, index=False)
    else: 
        os.mkdir(path_desktop)
        data_clean.to_csv(path_file, encoding='utf-8', index=False)

    return path_file    

def conectionPostgres(file_credential):
    # Configura la conexión a la bbdd datawarehouse de brubank. Devuelve la ruta de conexión como 'engine' 
    DIALECT = 'postgresql'
    SQL_DRIVER = 'psycopg2'
    USERNAME = file_credential['postgresql']['username']
    PASSWORD = file_credential['postgresql']['password']
    HOST = file_credential['postgresql']['host']
    PORT = file_credential['postgresql']['puerto']
    DBNAME = file_credential['postgresql']['dbname']
    ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) +"/"+DBNAME 
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    
    return engine

# FUNCIONES DE EXTRACCION DE INFORMACION

def scraper_product(prod_pag):
    
    list_prod = []
    
    for n, i in enumerate(prod_pag):
        dict_prod = {}
            
        ## LINK DEL PRODUCTO
        try:
            dict_prod['url'] = prod_pag[n].a.get('href')
        except AttributeError:
            dict_prod['url'] = None

        ## DESCUENTO DEL PRODUCTO
        try:
            dict_prod['discount_por'] = prod_pag[n].find('span', attrs={'class':'promotion-item__discount'}).get_text()
        except AttributeError:
            dict_prod['discount_por'] = None

        ## VENDEDOR
        try:
            dict_prod['seller'] = prod_pag[n].find('span', attrs={'class':'promotion-item__seller'}).get_text()
        except AttributeError:
            dict_prod['seller'] = None

        ## NOMBRE DEL PRODUCTO
        try:
            dict_prod['title'] = prod_pag[n].find('p', attrs={'class':'promotion-item__title'}).get_text()
        except AttributeError:
            dict_prod['title'] = None

        ## ENVIO
        try:
            dict_prod['shipping'] = prod_pag[n].find('span', attrs={'class':'promotion-item__shipping'}).get_text()
        except AttributeError:
            dict_prod['shipping'] = None

        ## CUOTAS
        try:
            dict_prod['dues'] = prod_pag[n].find('span', attrs={'class':'promotion-item__installments'}).get_text()
        except AttributeError:
            dict_prod['dues'] = None

        ## PRECIO CON DESCUENTO
        try:
            dict_prod['price_discount'] = prod_pag[n].find('span', attrs={'class':'promotion-item__price'}).span.get_text()
        except AttributeError:
            dict_prod['price_discount'] = None

        ## PRECIO NORMAL
        try:
            dict_prod['price'] = prod_pag[n].find('span', attrs={'class':'promotion-item__oldprice'}).get_text()
        except AttributeError:
            dict_prod['price'] = None

        ## categoria oferta del dia
        try:
            dict_prod['sale_day'] = prod_pag[n].find('span', attrs={'class':'promotion-item__today-offer-text'}).get_text()
        except AttributeError:
            dict_prod['sale_day'] = None

        ## imagen principal del producto
        try:
            if n<=5:
                dict_prod['picture'] = prod_pag[n].img.get('src')
            else:
                dict_prod['picture'] = prod_pag[n].img.get('data-src')
        except AttributeError:
            dict_prod['picture'] = None

        ## envio full
        try:
            dict_prod['type_shipping'] = prod_pag[n].svg.get('class')[0]
        except AttributeError:
            dict_prod['type_shipping'] = None
            
        dict_prod['download_date'] = datetime.now()   
        
        list_prod.append(dict_prod)
    
    return list_prod

def pagesTotal(url, cantidad):
    
    list = []
    for i in range(1,cantidad+1):
        list.append(url + str(i))
        
    return list

def getReview(list_products):

    q = len(list_products)
    print(f'\n Start search review for {q} products\n')
    list = []

    bar = ChargingBar('Search review:', max=q)

    for i in list_products:
        sleep(random.randint(1,5))
        dict = {}
        url_review = f'https://www.mercadolibre.com.ar/noindex/catalog/reviews/MLA{i}'
        response = requests.get(url_review)
        soup_response = BeautifulSoup(response.text, 'lxml')

        ## PRODUCT SCORE
        try:
            score = soup_response.find('div', attrs={'class':'big-score'})
            dict['review_score'] = float(score.h1.get_text())
        except AttributeError:
            dict['review_score'] = None

        try:
            dict['review_count']  = soup_response.find('div', attrs={'class':'total-reviews'}).span.get_text()
        except AttributeError:
            dict['review_count'] = None

        try:
            dict['review_url'] = url_review
        except AttributeError:
            dict['review_url'] = None

        try:
            dict['product_id'] = i
        except AttributeError:
            dict['product_id'] = None
        
        dict['review_download'] = datetime.now() 
        
        list.append(dict) 
        load_data('review_meli', DataFrame(list))
        bar.next()

# FUNCIONES DE TRANSFORMACION DE DATOS

def deletePor(x):  
    try:
        result = x.replace('por ','')
    except AttributeError:
        result = None
    
    return result

def replaceIconFull(x):  
    try:
        result = x.replace('full-icon','full')
    except AttributeError:
        result = None
    
    return result

def transformProducts(df_enter):

    #Filtro las filas nulas
    df = df_enter[df_enter.url.notnull()]

    #QUITO LA PRIMERA PARTE DEL URL
    df['product_id'] = df['url'].apply(lambda x: x.replace('https://articulo.mercadolibre.com.ar/',''))

    #EXTRAIGO EL ID DEL PRODUCTO
    df['product_id'] = df['product_id'].apply(lambda x: x.split('-')[1])
    df['product_id'] = df['product_id'].apply(lambda x: deleteStr(x))

    #REMPLAZO EL ICON-FULL POR FULL
    df['type_shipping'] = df['type_shipping'].apply(lambda x: replaceIconFull(x))

    #ELIMINO 'Por ' DE LOS VENDEDORES
    df['seller'] = df['seller'].apply(lambda x: deletePor(x))

    #CREO UN CAMPO SIN '% OFF' DE LOS DESCUENTOS
    df['discount_number'] = df['discount_por'].apply(lambda x: replaceOFF(x))

    #CREO CAMPOS SIN '$' Y '.' DE LOS PRECIOS
    df['price_number'] = df['price'].apply(lambda x: replaceUnit(x))
    df['price_discount_number'] = df['price_discount'].apply(lambda x: replaceUnit(x))   

    return df

def replaceOFF(x):  
    try:
        result = int(x.replace('% OFF',''))/100
    except AttributeError:
        result = None
    
    return result

def replaceUnit(x):  
    try:
        stag = x.replace('$','')
        result = int(stag.replace('.',''))
        
    except AttributeError:
        result = None
    except ValueError:
        result = None
    
    return result

def deleteStr(x):
    
    try:
        r = int(x)
    except ValueError:
        r = None
    
    return r

# PROGRAMA

def run():

    # Inicia el scrapper
    print('\n ############### MELI_SCRAPPER ############### \n')
    start = datetime.now()

    print(f'\n Start of the process: {start}\n')

    # levanto las credenciales secretas
    secrets = dataConfig()

    # Consulto el home y calculo las iteraciones segun la cantidad
    # de productos encontrados
    response = requests.get(secrets['meli']['url'])
    soup_response = BeautifulSoup(response.text, 'lxml')
    q_pag = soup_response.find_all('a', attrs={'class':'andes-pagination__link'})
    q = int(q_pag[-2].get_text())
    pages = pagesTotal(secrets['meli']['url'], q)

    print(f'\n Start scrapper of {q} pages with 48 products (total: {q*48})\n')

    bar = ChargingBar('Scrapeando pagina:', max=q)

    #Creo el df molde vacio
    df_end = pd.DataFrame(columns=['url', 'discount_por', 'seller', 'title', 'shipping', 'dues',
        'price_discount', 'price', 'sale_day', 'picture', 'type_shipping',
        'download_date', 'product_id', 'discount_number', 'price_number', 'price_discount_number'], index=[0])

    for pag in pages:
        sleep(random.randint(5,20))
        response = requests.get(pag)
        soup_response = BeautifulSoup(response.text, 'lxml')
        test_list = soup_response.find_all('li', attrs={'class':'promotion-item'})
        result = scraper_product(test_list) # transforma una lista en un dict con los datos de interes
        df_temp = pd.DataFrame(result)
        df_end = pd.concat([df_end, df_temp], axis=0)
        break
        bar.next()

    bar.finish()

    # data clean de la info descargada
    df = transformProducts(DataFrame(df_end))

    # creo la conexion
    engine = conectionPostgres(secrets)

    #carga incremental de la data obtenida
    df.to_sql(secrets['db']['table'], engine, schema=secrets['db']['schema'], if_exists='append',  index=False, chunksize=2000)

    end = datetime.now()
    print(f'\n\nEnd of the process: {end}')
    print(f'\nDuration of the process: {end - start}\n')   
        
if __name__=='__main__':
    run()






