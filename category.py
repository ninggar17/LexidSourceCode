import pandas
import warnings
warnings.filterwarnings('ignore')
pandas.set_option('display.max_rows', 400)
pandas.set_option('display.max_colwidth', 100)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)
x = pandas.read_csv('crawl\legal_datail.csv')
x['categories'] = x['categories'].replace(to_replace=r'[\[\]]', value='', regex=True)
x['categories'] = x['categories'].str.strip()
x['c'] = x['categories'].str.split(pat=',')
x['xx'] = x['c'].str.len()
x=x.sort_values(['xx'], ascending=[False])
print(x[['c','xx']])
x= x.explode('c')
print(x[['c','xx']])
d= pandas.DataFrame(x.groupby(['c'])['url'].nunique())
print(d.columns)
old_names = d.columns
new_names = ['a', 'b']
d.rename(columns=dict(zip(old_names, new_names)), inplace=True)
print(d.columns)
print(d)
print('dukhuifh')
print(len(x.loc[x['categories'].str.contains(r'.\w+.', regex=True)]['url'].unique()))
