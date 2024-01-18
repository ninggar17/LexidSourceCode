import re

import numpy
import pandas

pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)
pandas.set_option('display.max_colwidth', 300)
pandas.set_option('display.max_rows', 300)

sourceQA14 = 'E:/Rate/Q1-Q4/SampleQnAEvaluated.csv'
sourceQA5 = 'E:/Rate/Q5/SampleQ5Evaluated.csv'
sourceQA6 = 'E:/Rate/Q6/SampleQ6Evaluated.csv'

resEval14 = pandas.read_csv(sourceQA14)
resEval5 = pandas.read_csv(sourceQA5)
resEval6 = pandas.read_csv(sourceQA6)

resEval14 = resEval14[['type1', 'Q', 'A', 'f1']].rename(columns={'type1': 'type'})
resEval5 = resEval5[['type1', 'Q', 'A', 'f1']].rename(columns={'type1': 'type'})
resEval6 = resEval6[['type', 'Q', 'A', 'f1']]

resEval = pandas.concat([resEval14, resEval5, resEval6])
resEval['rand'] = numpy.random.uniform(0, 1, size=(len(resEval)))
resEval['rank'] = resEval.groupby('type')['rand'].transform('rank')
# for line in resEval14.iterrows():
#     print('\t', line[1]['type'], ' & ', line[1]['Q'], ' & ', re.sub(r'[\r\n]+', r' \\newline ', line[1]['A']), ' & ', round(line[1]['f1'],2), ' \\\\')
# print('\t \\midrule')
# for line in resEval5.iterrows():
#     print('\t', line[1]['type'], ' & ', line[1]['Q'], ' & ', re.sub(r'[\r\n]+', r' \\newline ', line[1]['A']), ' & ', round(line[1]['f1'],2), ' \\\\')
# print('\t \\midrule')
# for line in resEval6.iterrows():
#     print('\t', line[1]['type'], ' & ', line[1]['Q'], ' & ', re.sub(r'[\r\n]+', r' \\newline ', line[1]['A']), ' & ', round(line[1]['f1'],2), ' \\\\')

for i in resEval[resEval['rank'] <= 20]['Q']:
    print(i)
