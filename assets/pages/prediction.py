import pandas as pd
import streamlit as st
import seaborn as sns

import pickle
import numpy as np

import locale

locale.setlocale(locale.LC_ALL, '')

#VIEW--------------------------------------------
st.set_page_config(
    page_title="SQL Dashboard",  #Web page title
    page_icon="📊",  #Web page icon
    layout="wide")  #Web page layout


#MAIN PAGE--------------------------------------
st.markdown("# IMMO Eliza")
st.markdown('## Make prediction')

#PREDICTION-------------------------------------

#Import model-----------------------------------
with open(
        "model/immo_scaler.pkl",
        "rb") as scalefile:
    scaler = pickle.load(scalefile)

with open(
        "model/immo_poly_features.pkl",
        "rb") as polyfeaturesfile:
    poly_features = pickle.load(polyfeaturesfile)

with open(
        "model/immo_model.pkl",
        "rb") as modelfile:
    poly_model = pickle.load(modelfile)


#Prediction function---------------------------
def predict(preprocess_item):
    """
    Function that takes immo_eliza preprocessed data as an input and return a price as output.
    :input
    :output
    """
    array_input = np.array([preprocess_item])
    X_scaled_imput = scaler.transform(array_input)
    price_prediction = poly_model.predict(
        poly_features.fit_transform(X_scaled_imput))
    return float(price_prediction)


with st.form('Prediction', clear_on_submit=True):
    #Input--------------------------------------
    Property_type = st.selectbox(
        'Property type',
        ['APARTMENT', 'HOUSE'])  #: Literal["APARTMENT", "HOUSE", "OTHERS"]
    State_of_the_building = st.selectbox('State of building', [
        "NO_INFO", "TO_BE_DONE_UP", "TO_RENOVATE", "TO_RESTORE",
        "JUST_RENOVATED", "GOOD", "AS_NEW"
    ])
    Number_of_facades = st.number_input('Numbers of facades',
                                        step=1,
                                        min_value=1,
                                        max_value=8)
    Zip_code = st.number_input('Zipcode',
                               step=1,
                               min_value=1000,
                               max_value=9999)  #: int
    #full_address= st.text_input('Adress')#: str = None #Optional[str], never used

    #land_area= st.number_input('Area of the land')#: int = None #Optional[int]
    #Garden= st.selectbox('Has a garden',['No','Yes'])#: bool = None #Optional[bool],
    Garden_area = st.number_input('Garden area (m²)', step=1,
                                  min_value=0)  #: int = None #Optional
    Swimming_pool = st.selectbox(
        'Has swiming pool', ['No', 'Yes'])  #: bool = None #Optional[bool],

    Surface = st.number_input('Living Area (m²)', step=1,
                              min_value=1)  #: int[int],
    Number_of_bedrooms = st.number_input('Number of bedrooms',
                                         step=1,
                                         min_value=0)  #: int[int],
    furnished = st.selectbox('Is furnished',
                             ['No', 'Yes'])  #: bool = None #Optional[bool],
    Fully_equipped_kitchen = st.selectbox(
        'Type of kitchen',
        ['NOT_INSTALLED', 'EQUIPED', 'SEMI_EQUIPED', 'FULL_EQUIPED'
         ])  #: bool = None #Optional[bool],
    Open_fire = st.selectbox('Has open fire',
                             ['No', 'Yes'])  #: bool = None #Optional[bool],

    #terrace= st.selectbox('Has terrace',['No','Yes'])#: bool = None #Optional[bool],
    Terrace_surface = st.number_input(
        'Terrace Area (m²)', step=1,
        min_value=0)  #: int = None #Optional[int],

    button = st.form_submit_button('Make prediction')

    #Dictionary---------------------------------

    kitchen_type_dict = {
        'NOT_INSTALLED': 0,
        'EQUIPED': 0.5,
        'SEMI_EQUIPED': 0.75,
        'FULL_EQUIPED': 1
    }
    state_of_the_building_dict = {
        "NO_INFO": 0.87252,
        "TO_BE_DONE_UP": 0.65376,
        "TO_RENOVATE": 0.56664,
        "TO_RESTORE": 0.46920,
        "JUST_RENOVATED": 0.93115,
        "GOOD": 0.79285,
        "AS_NEW": 1.0
    }
    zip_code_dict_xx = {
        'be_zip_10': 1.53,
        'be_zip_11': 1.68,
        'be_zip_12': 1.66,
        'be_zip_13': 1.29,
        'be_zip_14': 1.18,
        'be_zip_15': 1.24,
        'be_zip_16': 1.31,
        'be_zip_17': 1.23,
        'be_zip_18': 1.22,
        'be_zip_19': 1.5,
        'be_zip_20': 1.53,
        'be_zip_21': 1.17,
        'be_zip_22': 1.13,
        'be_zip_23': 1.12,
        'be_zip_24': 1.03,
        'be_zip_25': 1.24,
        'be_zip_26': 1.27,
        'be_zip_27': 1.11,
        'be_zip_28': 1.22,
        'be_zip_29': 1.3,
        'be_zip_30': 1.58,
        'be_zip_31': 1.18,
        'be_zip_32': 1.1,
        'be_zip_33': 1.07,
        'be_zip_34': 0.87,
        'be_zip_35': 1.13,
        'be_zip_36': 1.0,
        'be_zip_37': 0.9,
        'be_zip_38': 0.94,
        'be_zip_39': 1.0,
        'be_zip_40': 0.93,
        'be_zip_41': 0.85,
        'be_zip_42': 0.86,
        'be_zip_43': 0.87,
        'be_zip_44': 0.81,
        'be_zip_45': 0.76,
        'be_zip_46': 0.95,
        'be_zip_47': 0.98,
        'be_zip_48': 0.85,
        'be_zip_49': 0.94,
        'be_zip_50': 0.97,
        'be_zip_51': 1.0,
        'be_zip_52': 0.77,
        'be_zip_53': 0.87,
        'be_zip_54': 0.77,
        'be_zip_55': 0.76,
        'be_zip_56': 0.67,
        'be_zip_57': 0.77,
        'be_zip_58': 0.77,
        'be_zip_59': 0.77,
        'be_zip_60': 0.64,
        'be_zip_61': 0.74,
        'be_zip_62': 0.78,
        'be_zip_63': 0.69,
        'be_zip_64': 0.66,
        'be_zip_65': 0.67,
        'be_zip_66': 0.91,
        'be_zip_67': 0.97,
        'be_zip_68': 0.84,
        'be_zip_69': 0.83,
        'be_zip_70': 0.8,
        'be_zip_71': 0.69,
        'be_zip_72': 0.67,
        'be_zip_73': 0.58,
        'be_zip_75': 0.86,
        'be_zip_76': 0.66,
        'be_zip_77': 0.79,
        'be_zip_78': 0.91,
        'be_zip_79': 0.66,
        'be_zip_80': 1.34,
        'be_zip_81': 1.25,
        'be_zip_82': 1.32,
        'be_zip_83': 2.12,
        'be_zip_84': 1.43,
        'be_zip_85': 1.06,
        'be_zip_86': 1.61,
        'be_zip_87': 1.16,
        'be_zip_88': 0.98,
        'be_zip_89': 0.95,
        'be_zip_90': 1.46,
        'be_zip_91': 1.13,
        'be_zip_92': 1.11,
        'be_zip_93': 1.03,
        'be_zip_94': 1.0,
        'be_zip_95': 0.96,
        'be_zip_96': 0.94,
        'be_zip_97': 1.11,
        'be_zip_98': 1.27,
        'be_zip_99': 1.16
    }

    #Preprocess input---------------------------

    transformed_elment = {
        'Number_of_bedrooms': Number_of_bedrooms,
        'Surface': Surface,
        'Fully_equipped_kitchen': kitchen_type_dict[Fully_equipped_kitchen],
        'Open_fire': 1 if Open_fire == 'Yes' else 0,
        'Terrace_surface': Terrace_surface,
        'Garden': 0 if Garden_area == 0 else 1,
        'Number_of_facades': Number_of_facades,
        'Swimming_pool': 1 if Swimming_pool == 'Yes' else 0,
        'State_of_the_building':
        state_of_the_building_dict[State_of_the_building],
        'zip_code_ratio': zip_code_dict_xx['be_zip_' + str(Zip_code)[:2]],
        'HOUSE': 1 if Property_type == 'HOUSE' else 0,
        'APARTMENT': 1 if Property_type == 'APARTMENT' else 0
    }

    preprocess_item = list(transformed_elment.values())

    #Make prediction----------------------------

    prediction_item = predict(preprocess_item)

#Show prediction----------------------------
if button:
    st.markdown('#### Prediction result')
    if prediction_item < 0:
        st.write('Wrong input !, please fill up the form properly')
    else:
        st.write(f'Price pediction= {"{:,d}".format(int(prediction_item))}€')
        st.write('This prediction has an average 70% of accuracy)')
# else: st.write('Please fill up the form')
