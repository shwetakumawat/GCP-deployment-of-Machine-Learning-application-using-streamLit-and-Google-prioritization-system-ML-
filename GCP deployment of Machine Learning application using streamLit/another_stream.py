# importing useful libraries
import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

st.write("""
# Explore different classifier and datasets
Which one is the best?
""")

# create select boxes for algorithms and datasets in sidebar
dataset_name = st.sidebar.selectbox(
    'Select Dataset',
    ('Iris', 'Breast Cancer', 'Wine')
)

st.write(f"## {dataset_name} Dataset")

classifier_name = st.sidebar.selectbox(
    'Select classifier',
    ('KNN', 'SVM', 'Random Forest')
)

# fuction to load selected dataset 
def get_dataset(name):
    data = None
    if name == 'Iris':
        data = datasets.load_iris()
    elif name == 'Wine':
        data = datasets.load_wine()
    else:
        data = datasets.load_breast_cancer()
    X = data.data
    y = data.target

    return X, y

# Display the shape and classes of selected dataset 
X, y= get_dataset(dataset_name)
st.write('Shape of dataset:', X.shape)
st.write('number of classes:', len(np.unique(y)), set(y))

df1 = pd.DataFrame(X).head(3)
df2 = pd.DataFrame(y).head(3)
df3 = pd.DataFrame(X).tail(3)
df4 = pd.DataFrame(y).tail(3)

# Return head and tail of the selected dataset
st.write("Head of the dataset")
st.write(pd.concat([df1,df2],axis=1))

st.write("Tail of the dataset")
st.write(pd.concat([df3,df4],axis=1))

# Adding the hyperparameters
def add_parameter_ui(clf_name):
    params = dict()
    if clf_name == 'SVM':
        C = st.sidebar.slider('C', 0.01, 10.0)
        params['C'] = C
    elif clf_name == 'KNN':
        K = st.sidebar.slider('K', 1, 15)
        params['K'] = K
    else:
        max_depth = st.sidebar.slider('max_depth', 2, 15)
        params['max_depth'] = max_depth
        n_estimators = st.sidebar.slider('n_estimators', 1, 100)
        params['n_estimators'] = n_estimators
    return params

params = add_parameter_ui(classifier_name)

# Assign hyperparameters according to algorithms 
def get_classifier(clf_name, params):
    clf = None
    if clf_name == 'SVM':
        clf = SVC(C=params['C'])
    elif clf_name == 'KNN':
        clf = KNeighborsClassifier(n_neighbors=params['K'])
    else:
        clf = RandomForestClassifier(n_estimators=params['n_estimators'],
            max_depth=params['max_depth'], random_state=1234)
    return clf

clf = get_classifier(classifier_name, params)

#### CLASSIFICATION ####

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1234)

clf.fit(X_train, y_train)
y_pred = clf.predict(X_test)

acc = accuracy_score(y_test, y_pred)

st.write(f'Classifier = {classifier_name}')
st.write(f'Accuracy =', acc)
