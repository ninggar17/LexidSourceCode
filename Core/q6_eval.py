import re
import warnings
from datetime import date

import pandas
from nltk.tokenize import word_tokenize

warnings.filterwarnings('ignore')
pandas.set_option('display.max_rows', 200)
pandas.set_option('display.max_colwidth', 100)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)
sourceQA = 'E:/Rate/Q6/SampleQ6.csv'

QAPairs = pandas.read_csv(sourceQA)
QAPairs['hasAnswer'] = QAPairs['answer'].isna() | QAPairs['answer'].str.contains(r'^\W+$', regex=True)
sum_len_A = 0
sum_len_answer = 0
sum_common = 0
sum_f1 = 0
res = []
for row in QAPairs.iterrows():
    temp = dict(row[1])
    exact = re.sub(r'[^a-zA-Z]+', ' ', row[1]['A'].lower()).strip()
    data = re.sub('^nan$', '', re.sub(r'[^a-zA-Z]+', ' ', str(row[1]['answer']).lower()).strip())
    exact = set(word_tokenize(exact))
    data = set(word_tokenize(data))
    len_answer = len(data)
    len_A = len(exact)
    common = len(exact & data)
    temp['len_A'] = len_A
    temp['len_answer'] = len_answer
    temp['common'] = common
    if len_A == 0:
        recall = 0
    else:
        recall = common / len_A
    if len_answer == 0:
        precision = 0
    else:
        precision = common / len_answer
    if recall == 0 and precision == 0:
        f1 = 0
    else:
        f1 = 2 * (precision * recall) / (precision + recall)
    temp['f1'] = f1
    sum_len_A += len_A
    sum_len_answer += len_answer
    sum_common += common
    sum_f1 += f1
    res.append(temp)
res = pandas.DataFrame(res)
res.to_csv(re.sub(r'\.csv', 'evaluated.csv', sourceQA), index=False)
# res = QAPairs.merge(res, left_on=['reg_id', 'Column2'], right_on=['reg_id', 'Column2'])
# print(res)
# recall = sum_common / sum_len_A
# precision = sum_common / sum_len_answer
# f1 = 2 * (precision * recall) / (precision + recall)
# avg_f1 = sum_f1/len(QAPairs)
# print(sum_len_A, sum_len_answer, sum_common, f1, avg_f1)
# print('ihihf',bool(''))
print(res)
