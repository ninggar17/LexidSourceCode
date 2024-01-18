import re

import numpy
import pandas
sourceQA14 = 'E:/Rate/Q1-Q4/SampleQnA.csv'
sourceQA5 = 'E:/Rate/Q5/SampleQ5.csv'
sourceQA6 = 'E:/Rate/Q6/SampleQ6.csv'

resEval14 = pandas.read_csv(re.sub(r'\.csv', 'evaluated.csv', sourceQA14))
resEval5 = pandas.read_csv(re.sub(r'\.csv', 'evaluated.csv', sourceQA5))
resEval6 = pandas.read_csv(re.sub(r'\.csv', 'evaluated.csv', sourceQA6))
resEval6 = resEval6[~(resEval6['answer'].isnull())]
resEval6['type1'] = 'Q6'
resEval6['type2'] = 'Q6.1'
resEval14 = resEval14[['type1', 'type2', 'f1']]
resEval5 = resEval5[['type1', 'type2', 'f1']]
resEval6 = resEval6[['type1', 'type2', 'f1']]
resEval = pandas.concat([resEval14, resEval5, resEval6], ignore_index=True)
resEval['f1Type'] = resEval.groupby('type1')['f1'].transform(numpy.mean)
resEval['f1Type2'] = resEval.groupby('type2')['f1'].transform(numpy.mean)
resEval['f1All'] = sum(resEval['f1'])/len(resEval)
print(resEval[['type2', 'f1Type2', 'f1Type']].drop_duplicates())
resEval = resEval[['type1', 'f1Type', 'f1All']].drop_duplicates()
print(resEval14)
print(resEval5)
print(resEval6)
print(resEval)