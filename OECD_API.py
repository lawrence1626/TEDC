#import cif
from cif_new import createDataFrameFromOECD
import pandas as pd

#def create_DataFrame_from_OECD(country = 'CZE', subject = [], measure = [], frequency = 'M',  startDate = None, endDate = None):
data, subjects, measures = createDataFrameFromOECD(countries = ['GBR'], dsname = 'MEI_BTS_COS', subject = [], measure = [], frequency = 'Q')
print(data)
print(subjects)
print(measures)
"""
out_path = "./output/"
data.to_csv(out_path+'data.csv')
subjects.to_csv(out_path+'subjects.csv')
measures.to_csv(out_path+'measures.csv')
"""