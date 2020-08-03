#import cif
from cif import cif
import pandas as pd

#def create_DataFrame_from_OECD(country = 'CZE', subject = [], measure = [], frequency = 'M',  startDate = None, endDate = None):
data, subjects, measures = cif.createDataFrameFromOECD(countries = ['USA'], dsname = 'QNA', frequency = 'Q', startDate = '2020-01')
print(data)
print(subjects)
print(measures)

path = 'C:/Users/lawre/Desktop/'
data.to_csv(path+'data.csv')
subjects.to_csv(path+'subjects.csv')
measures.to_csv(path+'measures.csv')