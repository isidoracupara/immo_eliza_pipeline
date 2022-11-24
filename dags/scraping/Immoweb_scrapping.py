import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import concurrent.futures
from bs4 import BeautifulSoup
import requests
import json
import csv

with open('scraping/used_urls.csv', 'r') as f:
    reader = csv.reader(f)
    used_urls = set([item for sublist in reader for item in sublist])


def get_houses_url():
    """
    Function visiting announcement pages and get urls for each house.
    """
    for i in range(1, 6):
        num_pages = str(i)+'&orderBy=relevance'
        url = 'https://www.immoweb.be/en/search/house/for-sale?countries=BE&isNewlyBuilt=false&isALifeAnnuitySale=false&page='+num_pages

        driver.get(url)

        page_houses = driver.find_elements(
            By.XPATH, "//a[@class='card__title-link']")

        for link in page_houses:
            url = link.get_attribute('href')
            houses_url.append(url)


def get_apartments_url():
    """
    Function visiting announcement pages and get urls for each apartments.
    """
    for i in range(1, 6):
        num_pages = str(i)+'&orderBy=relevance'
        url = 'https://www.immoweb.be/en/search/apartment/for-sale?countries=BE&isNewlyBuilt=false&isALifeAnnuitySale=false&page='+num_pages

        driver.get(url)

        page_apartments = driver.find_elements(
            By.XPATH, "//a[@class='card__title-link']")

        for link in page_apartments:
            url = link.get_attribute('href')
            apartments_url.append(url)


def get_info(url):
    """
    Function get all the details for each house and apartment with BeautifulSoup.

    Args:
        url : URL of each propriety from list_url, called by ThreadPoolExecutor.
    """
    dict_temp = {}

    content = session.get(url)
    if content.status_code == 200:
        soup = BeautifulSoup(content.text, 'html.parser')

        script_tag = soup.find_all('script', attrs={"type": "text/javascript"})

        # Details of property stored in a dictionary inside the script wich has window.classified as keyword
        for i in script_tag:
            if 'window.classified' in str(i):
                info_final_prop = i

        # Removing white spaces/unecessary fields and get the dictionary from script.
        info_final_prop = info_final_prop.text.strip()
        info_final = re.findall('{.*}', info_final_prop)
        info_dict = json.loads(info_final[0])

        # Requesting relevant informations from the dictionary by specifying the key names.
        post_code = info_dict['property']['location']['postalCode']
        region = info_dict['property']['location']['region']
        province = info_dict['property']['location']['province']
        locality = info_dict['property']['location']['locality']
        type_of_property = info_dict['property']['type']
        if info_dict['flags']['isPublicSale'] == True:
            type_of_sale = 'Public Sale'
        if info_dict['flags']['isNotarySale'] != None:
            type_of_sale = 'Notary Sale'
        if info_dict['flags']['isPublicSale'] == False and info_dict['flags']['isNotarySale'] == None:
            type_of_sale = 'Regular'
        price = info_dict['transaction']['sale']['price']
        # if info_dict['property']['building'] != None:
        #construction_year = info_dict['property']['building']['constructionYear']
        # else:
        #construction_year = None
        bedrooms_count = info_dict['property']['bedroomCount']
        habitable_surface = info_dict['property']['netHabitableSurface']
        if info_dict['property']['kitchen'] != None:
            kitchen_type = info_dict['property']['kitchen']['type']
        else:
            kitchen_type = None
        furnished = info_dict['transaction']['sale']['isFurnished']
        fireplace = info_dict['property']['fireplaceExists']
        terrace_area = info_dict['property']['terraceSurface']
        garden_area = info_dict['property']['gardenSurface']
        if info_dict['property']['land'] != None:
            land_area = info_dict['property']['land']['surface']
        else:
            land_area = None
        if info_dict['property']['building'] != None:
            num_facade = info_dict['property']['building']['facadeCount']
        else:
            num_facade = None
        swimmingpool = info_dict['property']['hasSwimmingPool']
        if info_dict['property']['building'] != None:
            state = info_dict['property']['building']['condition']
        else:
            state = None
        subtype = info_dict['property']['subtype']
        indoor_parking = info_dict['property']['parkingCountIndoor']
        outdoor_parking = info_dict['property']['parkingCountOutdoor']

        keys = ["Postal_code", "Region", "Province", "Locality", "Type_of_property", "Subtype_of_property", "Price", "State_of_the_building", "Type_of_sale", "Number_of_bedrooms",
                "Surface", "Kitchen_type", "Furnished", "Open_fire", "Terrace_surface", "Garden_surface", "Land_surface", "Number_of_facades",
                "Swimming_pool", "Indoor_parking", "Outdoor_parking"]
        values = [post_code, region, province, locality, type_of_property, subtype, price, state, type_of_sale, bedrooms_count,
                  habitable_surface, kitchen_type, furnished, fireplace, terrace_area, garden_area, land_area, num_facade,
                  swimmingpool, indoor_parking, outdoor_parking]

        dict_temp = {key: value for key, value in zip(keys, values)}
        return dict_temp


def create_csv_file():
    csv_columns = ["Postal_code", "Region", "Province", "Locality", "Type_of_property", "Subtype_of_property", "Price", "State_of_the_building", "Type_of_sale", "Number_of_bedrooms",
                   "Surface", "Kitchen_type", "Furnished", "Open_fire", "Terrace_surface", "Garden_surface", "Land_surface", "Number_of_facades",
                   "Swimming_pool", "Indoor_parking", "Outdoor_parking"]

    csv_file = "all_entriess.csv"
    with open(csv_file, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in properties:
            if data is not None:
                writer.writerow(data)
    return csv_file


if __name__ == '__main__':
    houses_url = []
    apartments_url = []
    driver = webdriver.Chrome()
    get_houses_url()
    get_apartments_url()
    driver.close()
    list_url = houses_url + apartments_url
    set_url = set(list_url)
    new_urls = set_url.difference(used_urls)
    session = requests.Session()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        properties = list(executor.map(get_info, new_urls))
    session.close()
    create_csv_file()
    with open('used_urls.csv', 'a+', newline='') as file_url:
        writer = csv.writer(file_url)
        for item in new_urls:
            writer.writerow(item)
