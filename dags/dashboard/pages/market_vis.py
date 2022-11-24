import pandas as pd
import numpy as np
import plotly.express as px
import os
import csv
import plotly.graph_objects as go
import streamlit as st

df = pd.read_csv(
    'scraping/Property_structured_data .csv'
)

df.drop('URL', axis=1, inplace=True)
df.dropna(how='all', inplace=True)
df['Locality'] = df['Locality'].str.upper()
df = df[df['Price'] != 'Null']
df['Price'] = df['Price'].astype(int)
df.replace(to_replace=['Null'], value=np.nan, inplace=True)

to_int = [
    'Number_of_bedrooms', 'Number_of_facades', 'Garden_surface', 'Postal_code',
    'Land_surface', 'Terrace_surface', 'Surface'
]
for column in to_int:
    df[column] = df[column].astype('Int64')

st.set_page_config(layout="centered",
                   page_title="Database BeCode excercices",
                   initial_sidebar_state='auto',
                   page_icon="chart_with_upwards_trend")

st.title("**Market Analysis**")

st.subheader('Avg Price Per m2 vs Province')

province_avg_price = df.groupby('Province', as_index=False).agg({
    'Price':
    'sum',
    'Locality':
    'count',
    'Surface':
    'sum'
})
province_avg_price.rename(columns={'Locality': 'Number of Property'},
                          inplace=True)
province_avg_price = province_avg_price[
    province_avg_price['Number of Property'] > 3]
province_avg_price['Price per m2'] = (province_avg_price['Price'] /
                                      province_avg_price['Surface']).round()
province_avg_price_sorted = province_avg_price.sort_values('Price per m2',
                                                           ascending=False)

fig1 = px.bar(data_frame=province_avg_price_sorted,
              x='Province',
              y='Price per m2',
              color='Province',
              text_auto='.3s')
fig1.update(layout_coloraxis_showscale=False)
fig1.update(layout_showlegend=False)
fig1.add_hline(y=2786,
               line_dash='dash',
               annotation_text='Belgium Avg Price',
               annotation_position="bottom right")

st.plotly_chart(fig1)

st.subheader('Number of Properties in Regions')

property_number_per_region = df.groupby(['Region', 'Type_of_property'],
                                        as_index=False)['Locality'].count()
property_number_per_region.rename(columns={'Locality': 'Number_Of_Property'},
                                  inplace=True)
property_number_per_region_sorted = property_number_per_region.sort_values(
    by=['Number_Of_Property'], ascending=False)
fig2 = px.bar(data_frame=property_number_per_region_sorted,
              x='Region',
              y='Number_Of_Property',
              color='Type_of_property',
              barmode='group',
              text_auto='.2s')

st.plotly_chart(fig2)

st.subheader('Percentage of Properties Build After 2000s in Belgium')

# df2 = pd.read_csv('all_entriess.csv')
# print(df2.head())

# df2_recent_properties = df2[df2['construction_year'] > 2000]
# recent_property_per_region = df2_recent_properties.groupby(
#     'Region', as_index=False)['Locality'].count()
# recent_property_per_region.rename(columns={'Locality': 'Number_Of_Property'},
#                                   inplace=True)
# recent_property_per_region[
#     'Percentage'] = recent_property_per_region['Number_Of_Property'] / (
#         recent_property_per_region['Number_Of_Property'].sum())
# recent_property_per_region_sorted = recent_property_per_region.sort_values(
#     by=['Percentage'], ascending=False)
# fig3 = px.bar(data_frame=recent_property_per_region_sorted,
#               x='Region',
#               y='Percentage',
#               color='Region',
#               title='Percentage of Properties Build After 2000s in Belgium')
# fig3.update_traces(width=0.5)

# st.plotly_chart(fig3)
