import re

import numpy
import pandas

# warnings.filterwarnings('ignore')
pandas.set_option('display.max_rows', 900)
pandas.set_option('display.max_colwidth', 100)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)
sourceQA14 = 'E:/Rate/Q1-Q4/SampleQnA.csv'
sourceQA5 = 'E:/Rate/Q5/SampleQ5.csv'
sourceQA6 = 'E:/Rate/Q6/SampleQ6.csv'

old14 = pandas.read_csv(sourceQA14)
old14['article'] = old14['Q'].str.extract(r'(?i)(pasal\s+\d+[a-z]*\s*(?:ayat\s+\d[a-z]*)?)')
validated14 = pandas.read_csv(re.sub(r'\.csv', 'validated.csv', sourceQA14))
validated14['article'] = validated14['Q'].str.extract(r'(?i)(pasal\s+\d+[a-z]*\s*(?:ayat\s+\d[a-z]*)?)')
validated14.loc[validated14['article'].isnull(), 'article'] = ''
validated14.loc[validated14['article'].isnull(), 'article'] = ''
validated14 = validated14[['reg_id', 'type2', 'article', 'isValid', 'newAnswer']]
validated14 = validated14[~(validated14['newAnswer'].isnull())]
new14 = old14.merge(validated14, on=['reg_id', 'type2', 'article'], how='left')
new14.loc[~(new14['newAnswer'].isnull()), 'A'] = new14.loc[~(new14['newAnswer'].isnull()), 'newAnswer']
new14 = new14[['reg_id', 'type1', 'type2', 'Q', 'A', 'isValid_y']]
new14.rename(columns={'isValid_y':'isValid'}, inplace=True)
new14['isValid'] = True
new14.to_csv(re.sub(r'\.csv', 'Merged.csv', sourceQA14), index=False)
print(new14)
