import numpy as np
import pandas as pd
from scipy import stats
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from sklearn.preprocessing import PolynomialFeatures
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder
import pickle
import statistics

main_df = pd.read_csv(
    '/Users/ahmetsamilcicek/Desktop/becode/pipeline-immoweb-airflow/assets/Property_structured_data .csv')
new_df = pd.read_csv(
    '/Users/ahmetsamilcicek/Desktop/becode/pipeline-immoweb-airflow/assets/cleaned_entriess.csv')

main_df.drop('URL', axis=1, inplace=True)
main_df.dropna(how='all', inplace=True)
main_df.replace(to_replace=['Null'], value=np.nan, inplace=True)
main_df['Price'].dropna(inplace=True)
# Turn object type column to int
to_int = ['Number_of_bedrooms', 'Number_of_facades', 'Garden_surface', 'Postal_code', 'Land_surface',
          'Terrace_surface', 'Surface']
for column in to_int:
    main_df[column] = main_df[column].astype('Int64')

new_df['Swimming_pool'] = new_df['Swimming_pool'].map({True: 1, False: 0})
new_df['Open_fire'] = new_df['Open_fire'].map({True: 1, False: 0})
new_df['Furnished'] = new_df['Furnished'].map({True: 1, False: 0})
new_df['Garden'] = 0
new_df.loc[(new_df["Garden_surface"] > 0), 'Garden'] = 1

df = pd.concat([main_df, new_df]).fillna(-1)

df['Price'] = df['Price'].astype(float)

df = df.fillna(-1)
z_scores_price = stats.zscore(df['Price'])
abs_z_scores_price = np.abs(z_scores_price)
filtered_entries_price = (abs_z_scores_price < 1)
df = df[filtered_entries_price]
#Price (float)
df = df.loc[df['Price'] != -1]
#Surface (float)
df = df.loc[df['Surface'] <= 800]
df = df.loc[df['Surface'] >= 35]
df = df.loc[df['Surface'] != -1]
# Sub type of property (str)
df = df.loc[df['Subtype_of_property'] != 'APARTMENT_BLOCK']
df = df.loc[df['Subtype_of_property'] != 'MIXED_USE_BUILDING']
others = ["CHALET", "MANOR_HOUSE", "OTHER_PROPERTY", "CASTLE", "PAVILION"]
df.loc[df["Subtype_of_property"].isin(others), "Type_of_property"] = "OTHER"
# Number of bedrooms (int)
df = df.loc[df['Number_of_bedrooms'] < 20]
df['Type_of_property'] = np.where(
    (df['Number_of_bedrooms'] > 20), "OTHER", df['Type_of_property'])
df['Number_of_bedrooms'] = df['Number_of_bedrooms'].replace(-1, 0)
# Zip Code (category)
df['zip_code_xx'] = df['Postal_code'].apply(lambda x: 'be_zip_'+str(x)[:2])
# Land surface (float)
df['Land_surface'] = [land_surface if land_surface != -1 else garden_surface if garden_surface >
                      0 else land_surface for garden_surface, land_surface in zip(df['Garden_surface'], df['Land_surface'])]
df['Land_surface'] = df['Land_surface'].replace(-1, 0)
#Garden (0,1)
df['Garden'] = df['Garden'].replace(-1, 0)
# Garden surface (float)
df['Garden_surface'] = df['Garden_surface'].replace(-1, 0)
df['Garden_surface'] = df['Garden_surface'].replace(1, 0)
df.loc[(df["Garden_surface"] > 2000) & (df['Type_of_property']
                                        == 'APARTMENT'), 'Type_of_property'] = "OTHER"
# Fully equiped kitchen (int) change later, calculate each value y/n
df["Fully_equipped_kitchen"] = df["Fully_equipped_kitchen"].map({-1: 0.74,
                                                                 1: 1,
                                                                 "1": 1,
                                                                 "INSTALLED": 0.75,
                                                                 "SEMI_EQUIPPED": 0.60,
                                                                 "NOT_INSTALLED": 0.57,
                                                                 "USA_INSTALLED": 0.85,
                                                                 "USA_SEMI_EQUIPPED": 0.80,
                                                                 "USA_UNINSTALLED": 0.75,
                                                                 'HYPER_EQUIPPED': 1,
                                                                 'USA_HYPER_EQUIPPED': 1,
                                                                 })
# Swiming pool (0,1)
df['Swimming_pool'] = df['Swimming_pool'].replace(-1, 0)
#Furnished (0,1)
df['Furnished'] = df['Furnished'].replace(-1, 0)
# Open fire (0,1)
df['Open_fire'] = df['Open_fire'].replace(-1, 0)
#Terrace (0,1)
df['Terrace'] = df['Terrace'].replace(-1, 0)
# Terrace surface (float)
df = df.loc[df['Terrace_surface'] < 500]
df['Terrace_surface'] = df['Terrace_surface'].replace(-1, 0)
#Facades (int)
df = df.loc[df['Number_of_facades'] < 9]
df['Number_of_facades'] = np.where((df['Number_of_facades'] == -1) & (
    df["Type_of_property"] == "APARTMENT"), 1, df['Number_of_facades'])
df['Number_of_facades'] = np.where((df['Number_of_facades'] == -1) & (
    df["Type_of_property"] == "HOUSE"), 2, df['Number_of_facades'])
df = df.loc[df['Number_of_facades'] != -1]
# State of the building (int)
df['State_of_the_building'] = df['State_of_the_building'].map({
    -1: 0.87252,
    "NO_INFO": 0.87252,  # "TO_RENOVATE"
    "TO_BE_DONE_UP": 0.65376,  # "JUST_RENOVATED"
    "TO_RENOVATE": 0.56664,  # "TO_RENOVATE"
    "TO_RESTORE": 0.46920,  # "TO_REBUILD"
    "JUST_RENOVATED": 0.93115,  # "JUST_RENOVATED"
    "GOOD": 0.79285,  # "GOOD"
    "AS_NEW": 1.0  # "NEW"
})
# Type of property (category)¶
df = df.loc[df["Type_of_property"] != "OTHER"]
ohe = OneHotEncoder()
transformed_df = ohe.fit_transform(df[['Type_of_property']])
df[ohe.categories_[0]] = transformed_df.toarray()
# price/m² calculate(float)
df['Price_m2'] = round(df['Price']/df['Surface'], 2)
# zipcode ratio calculate(float)
df_zip_list = ['Price_m2', 'zip_code_xx']
df_zips = df[df_zip_list]
xxx_zip = df_zips.groupby('zip_code_xx')
xxx_zip_list = []  # stores the name of each zipcode from the data base
for key, values in xxx_zip:
    xxx_zip_list.append(key)
df_zips_mean = round(df_zips.groupby('zip_code_xx').mean(), 5)
df_zips_mean_values = df_zips_mean.values  # calculates mean for each zipxx
zip_mean = []  # stores the values as a list of mean for each zipxx
for x in df_zips_mean_values:
    for i in x:
        zip_mean.append(i)
global_mean = statistics.median(zip_mean)  # calculate a global mean
xxx = []  # list of the ponderated means
for y, i in enumerate(zip_mean):
    # calculates the relation of mean/zip code and the global mean
    xxx.append(round(i/global_mean, 2))
dic_zip_value = dict()  # creates a dictionay for zipcodes and values
for i, x in enumerate(xxx_zip_list):
    dic_zip_value[x] = xxx[i]
df['zip_code_ratio'] = df['zip_code_xx']
df['zip_code_ratio'] = df['zip_code_ratio'].map(dic_zip_value)
# Parking place
df['Indoor_parking'].fillna(0, inplace=True)
df['Outdoor_parking'].fillna(0, inplace=True)

with open('/Users/ahmetsamilcicek/Desktop/becode/pipeline-immoweb-airflow/assets/dataset_for_model.csv', 'w') as file_model:
    df.to_csv(file_model, index=False)
