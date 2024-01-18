import re
import warnings

import numpy
import pandas
from scipy.optimize import curve_fit
from scipy import stats
import powerlaw

warnings.filterwarnings('ignore')
pandas.set_option('display.max_rows', 400)
pandas.set_option('display.max_colwidth', 100)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)
# file_list = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')
# docs = 0
# triples = []
# while docs < 25:
#     text = file_list.readline()
#     if text != '':
#         filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/sentence_triple/' + (
#             re.search(r'(.{,100})', re.sub('_pdf', '', re.sub('(%20)|([\\._]+)', '_',
#                                                               re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
#                                                                      '', text)))).group(1).lower()) + '.ttl'
#         print(filesource)
#         print(text)
#         try:
#             triple = pandas.read_csv(filesource, delimiter=' ', header=None)
#             triples.append(triple)
#         except FileNotFoundError:
#             continue
#         except Exception:
#             Exception.with_traceback()
#             # continue
#         docs += 1
#         print(docs)
#         print(triple)
# triples = pandas.concat(triples)
# prefix = triples[triples[0] == '@prefix']
# triples = triples[~(triples[0] == '@prefix') & ~triples.isnull().any(axis=1)]
# triples = triples[[0, 1, 2]].rename(columns={0: 'source', 1: 'rel_name', 2: 'target'})
# triples = triples[triples['target'].str.contains('(rdfs|rdf|dbo|owl|peraturan|reg|rpart|rel|rtype|'
#                                                  'norm|act|concept):', regex=True)].drop_duplicates()
# dummy = pandas.concat([triples[['source', 'target']].rename(columns={'source': 'node', 'target': 'neighbor'}),
#                       triples[['source', 'target']].rename(columns={'target': 'node', 'source': 'neighbor'})]).drop_duplicates()
# xx = triples.merge(dummy, left_on='source', right_on='neighbor')
# xx = xx.merge(dummy.add_suffix('_1'), left_on=['target', 'node'], right_on=['neighbor_1', 'node_1'])
# xx = xx[['source', 'target', 'node']].drop_duplicates()
# xx['m_clustering'] = xx.groupby('node')['node'].transform('count')
# xx = xx[['node', 'm_clustering']].drop_duplicates()
# triples = triples.drop_duplicates()
# dummy['n_neighbor'] = dummy.groupby('node')['node'].transform('count')
# dummy = dummy[['node', 'n_neighbor']].drop_duplicates()
# dummy['all_possibility'] = dummy['n_neighbor']*(dummy['n_neighbor']-1)
# dummy = dummy[['node', 'all_possibility']]
# clustering = dummy.merge(xx, how='left', left_on='node', right_on='node')
# clustering.loc[clustering['m_clustering'].isnull(), 'm_clustering'] = 0
# clustering = clustering[~((clustering['m_clustering'] != 0) & (clustering['all_possibility'] == 0))]
# clustering['m_clustering'] = clustering['m_clustering']/clustering['all_possibility']
# clustering = clustering.dropna()
# # clustering['sum_clustering'] = clustering['m_clustering'].transform('sum')
# # clustering['card_s'] = clustering['node'].transform('count')
# print(clustering)
# print(1-sum(clustering['m_clustering'])/len(clustering))


def power_law(x, a, b):
    return a*numpy.power(x, b)


file_degree = 'E:/Ninggar/Mgstr/Penelitian/degree.csv'
file_degree_raw = 'E:/Ninggar/Mgstr/Penelitian/degree_raw.csv'
degree = pandas.read_csv(file_degree, delimiter=',')
degree_raw = pandas.read_csv(file_degree_raw)
cardV = sum(degree['nodes'])
degree['node_ratio'] = degree['nodes']/cardV
dummy_degS = list(range(0, cardV))
dummy = pandas.DataFrame(dummy_degS, columns=['degree'])
# dummy = pandas.DataFrame([{'dummy': 1}])
# a, b = curve_fit(f=power_law, xdata=degree['degS'], ydata=degree['nodes'], p0=[0, 0], bounds=(-numpy.inf, numpy.inf))
# a, b, c = stats.powerlaw.fit(data=degree_raw['degS'], loc=0, scale=1)
fit = powerlaw.Fit(data=degree_raw['degS'])
# x,y =
dummy['alpha'] = fit.power_law.alpha
dummy['probability'] = dummy['degree']**(-dummy['alpha'])
dummy.loc[dummy['degree'] == 0, 'probability'] = 0
C = 1/sum(dummy['probability'])
dummy['norm_prob'] = C*dummy['probability']
dummy = dummy.merge(degree, how='left', left_on='degree', right_on='degS')[['degree', 'norm_prob', 'node_ratio']]
dummy = dummy.fillna(0)
dummy['degree_diff'] = abs(dummy['norm_prob']-dummy['node_ratio'])
print(dummy)
print(sum(dummy['degree_diff']))
# print(fit.power_law.alpha)
# print(fit.power_law.sigma)
# print(fit.power_law.ccdf())
# print(a, b, c)
# stdevs = numpy.sqrt(numpy.diag(b))
# degree['power'] = a*(degree['degS']**(-b))
# print(degree)
