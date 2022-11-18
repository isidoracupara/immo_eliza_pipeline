import pandas as pd
import numpy as np

df = pd.read_csv(
    '/Users/ahmetsamilcicek/Desktop/becode/pipeline-immoweb-airflow/assets/all_entriess.csv')

df.drop_duplicates(inplace=True)

df.dropna(subset=['Price'], inplace=True)

df.replace(to_replace=['Null'], value=np.nan, inplace=True)

df['Furnished'].fillna(False, inplace=True)
df['Swimming_pool'].fillna(False, inplace=True)
df['Open_fire'].fillna(False, inplace=True)


to_int = ['Number_of_bedrooms', 'Number_of_facades', 'Garden_surface', 'Postal_code', 'Land_surface',
          'Terrace_surface', 'Surface', 'Price', 'Indoor_parking', 'Outdoor_parking']
for column in to_int:
    df[column] = df[column].astype('Int64')

with open('/Users/ahmetsamilcicek/Desktop/becode/pipeline-immoweb-airflow/assets/cleaned_entriess.csv', 'w') as file:
    df.to_csv(file, index=False)
