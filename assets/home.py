import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import json
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(page_title="References", layout="wide")

#Content of the page

st.markdown("# Immoweb dashboard")
st.markdown("## Property cost average by municipality in Euro")

# load geojson file as a dict
with open("map/BELGIUM_-_Municipalities.geojson") as geo_file:
    geo_data = json.load(geo_file)


df_postal_codes = pd.read_csv("map/postal_code_data.csv",dtype={"municipality": str})

df = pd.read_csv("Property_structured_data .csv", dtype={"Locality": str})


df = df.merge(df_postal_codes,on='Postal_code')
# geo_data_postal_code = geo_data["features"][0]["properties"]["CODE_INS"]
# geo_data = geo_data.merge(df_postal_codes,left_on=geo_data_postal_code, right_on="CODE_INS")

# df.to_csv("merge_test.csv")

# print(df.head)
min_price = df['Price'].min()
max_price = 1000000

## ALTERNATE MAP
# fig = go.Figure(
#     go.Choropleth(geojson=geo_data, locations="CODE_INS",
#         color="Price",
#         basemap_visible=False,
#         color_continuous_scale="Viridis",
#         #set this range_color to min and max price
#         range_color=(min_price, 1000000),
#         featureidkey="properties.CODE_INS",
#         labels={'CODE_INS':'INS location code', "Price": "Avg price per municipality"}
#     )
# )

# for col in df.columns:
#     df[col] = df[col].astype(str)

# # LIMITING PROPERTY PRICE
df.loc[df["Price"]== "Null", "Price"]= 0
df['Price'] = df['Price'].astype(int)
df["Postal_code"] = df['Postal_code'].astype(int)
df = df.loc[df['Price'] <= 1000000]

print(df.dtypes)

df['text'] = df['municipality'] + '<br>' + \
    'Postcode: ' +  df['Postal_code'].astype(str) + '<br>' + \
    'Price: ' + df['Price'].astype(str)

fig = go.Figure(data=go.Choropleth(
    geojson=geo_data,
    locations=df['CODE_INS'],
    z=df['Price'],
    colorscale='Reds',
    autocolorscale=False,
    text=df['text'], # hover text
    marker_line_color='grey',
    marker_line_width=0.1,
    colorbar_title="Avg Price",
    featureidkey='properties.CODE_INS',
))

fig.update_geos(fitbounds="locations",)
fig.update_layout(height=1000, margin={"r":0,"t":0,"l":0,"b":0})

st.plotly_chart(fig, use_container_width=True)
