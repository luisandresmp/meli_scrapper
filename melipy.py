import requests
from bs4 import BeautifulSoup
from pandas import DataFrame
from datetime import datetime
# import pandas as pd
import os
from time import sleep
import random
from progress.bar import ChargingBar

# FUNCIONES PARA GUARDAR INFORMACION

def load_data(name, data_clean):

    #CONSULTO LA HORA DEL SISTEMA, NOMBRO EL ARCHIVO FINAL Y CREO LA RUTA DEL ARCHIVO POR USUARIO    
    date = datetime.now().strftime('%Y_%m_%d')
    file_name = f'\{name}_{date}.csv'
    path_desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop/meli_scrapper/result')
    path_file = path_desktop + file_name

    if os.path.isdir(path_desktop): 
        data_clean.to_csv(path_file, mode='a', header=False, index=False)
    else: 
        os.mkdir(path_desktop)
        data_clean.to_csv(path_file, encoding='utf-8', index=False)

    return path_file    

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

def transformProducts(df):

    #QUITO LA PRIMERA PARTE DEL URL
    df['product_id'] = df['url'].apply(lambda x: x.replace('https://articulo.mercadolibre.com.ar/',''))

    #EXTRAIGO EL ID DEL PRODUCTO
    df['product_id'] = df['product_id'].apply(lambda x: x.split('-')[1])

    #REMPLAZO EL ICON-FULL POR FULL
    df['type_shipping'] = df['type_shipping'].apply(lambda x: replaceIconFull(x))

    #ELIMINO 'Por ' DE LOS VENDEDORES
    df['seller'] = df['seller'].apply(lambda x: deletePor(x))

    #CREO UN CAMPO SIN '% OFF' DE LOS DESCUENTOS
    df['discount'] = df['discount_por'].apply(lambda x: replaceOFF(x))

    #CREO CAMPOS SIN '$' Y '.' DE LOS PRECIOS
    df['price_clean'] = df['price'].apply(lambda x: replaceUnit(x))
    df['price_discount_clean'] = df['price_discount'].apply(lambda x: replaceUnit(x))   

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
    
# PROGRAMA

def run():

    #INICIA EL BOT
    print('\n ############### MELI_SCRAPPER ############### \n')
    start = datetime.now()

    print(f'\n Start of the process: {start}\n')
    
    url= 'https://www.mercadolibre.com.ar/ofertas?page='   

    response = requests.get(url)
    soup_response = BeautifulSoup(response.text, 'lxml')
    q_pag = soup_response.find_all('a', attrs={'class':'andes-pagination__link'})
    q = int(q_pag[-2].get_text())

    pages = pagesTotal(url, q)

    print(f'\n Start scrapper of {q} pages with 48 products (total: {q*48}): {start}\n')
    
    bar = ChargingBar('Scrapeando pagina:', max=q)

    for pag in pages:
        sleep(random.randint(5,20))
        response = requests.get(pag)
        soup_response = BeautifulSoup(response.text, 'lxml')
        test_list = soup_response.find_all('li', attrs={'class':'promotion-item'})
        result = scraper_product(test_list)
        df = transformProducts(DataFrame(result))
        load_data('Ofertas_meli', df) 
        bar.next()
    
    end = datetime.now()
    print(f'\nEnd of the process: {end}')
    print(f'\nDuration of the process: {end - start}\n')   
    
    bar.finish() 
        
if __name__=='__main__':
    run()
