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

df = pd.read_csv(
    'cleaning/dataset_for_model.csv')

filtered_atributes = [
    'Price',
    # 'ID',
    # 'Locality',
    # 'Postal_code',
    # 'Region',
    # 'Province',
    # 'Type_of_property',
    # 'Subtype_of_property',
    # 'Type_of_sale',
    'Number_of_bedrooms',
    'Surface',
    # 'Kitchen_type',
    'Fully_equipped_kitchen',
    # 'Furnished',
    'Open_fire',
    # 'Terrace',
    'Terrace_surface',
    'Garden',
    # 'Garden_surface',
    # 'Land_surface',
    'Number_of_facades',
    'Swimming_pool',
    'State_of_the_building',
    # URL,
    # 'zip_code_xx',
    # 'Price_m2',
    'zip_code_ratio',
    'HOUSE',
    'APARTMENT'
]

# filter the atributes that we need
df = df[filtered_atributes]

# split the data
X = df.iloc[:, 1:].values  # features
Y = df.iloc[:, 0].values  # target : price
X_train, X_test, Y_train, Y_test = train_test_split(
    X, Y, test_size=0.1, random_state=3)
print('Data splited')

# scale the data
scaler = StandardScaler().fit(X_train)
X_train = scaler.transform(X_train)
X_test = scaler.transform(X_test)

with open("model/immo_scaler.pkl", "wb") as scalefile:
    pickle.dump(scaler, scalefile)

# train the model
poly_features = PolynomialFeatures(degree=3)
# transforms the existing features to higher degree features.
X_train_poly = poly_features.fit_transform(X_train)
# fit the transformed features to Linear Regression
poly_model = LinearRegression()
poly_model.fit(X_train_poly, Y_train)

with open("model/immo_poly_features.pkl", "wb") as polyfeaturesfile:
    pickle.dump(poly_features, polyfeaturesfile)


# predicting on training data-set
y_train_predicted = poly_model.predict(X_train_poly)
# predicting on test data-set
y_test_predict = poly_model.predict(poly_features.fit_transform(X_test))

# evaluating the model on training dataset
rmse_train = np.sqrt(mean_squared_error(Y_train, y_train_predicted))
r2_train = r2_score(Y_train, y_train_predicted)
# evaluating the model on test dataset
rmse_test = np.sqrt(mean_squared_error(Y_test, y_test_predict))
r2_test = r2_score(Y_test, y_test_predict)

with open("model/immo_model.pkl", "wb") as modelfile:
    pickle.dump(poly_model, modelfile)
