import json
import re
import warnings
from datetime import date

import pandas
import rdflib
import rdfextras
from rdflib import Namespace
from nltk.tokenize import word_tokenize

warnings.filterwarnings('ignore')
pandas.set_option('display.max_rows', 200)
pandas.set_option('display.max_colwidth', 100)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)
rdfextras.registerplugins()
g = rdflib.Graph()

lexid_s = Namespace('<https://w3id.org/lex-id/schema/>')
lexid = Namespace('<https://w3id.org/lex-id/data/>')
# sourceQA = 'E:/Rate/Q1-Q4/SampleQnA.csv'
sourceQA = 'E:/Rate/Q5/SampleQ5.csv'
# sourceQA = 'E:/Rate/Q6/SampleQ6.csv'
resQA = re.sub('\.csv', '_Res.csv', sourceQA)

QAPairs = pandas.read_csv(sourceQA)
print(QAPairs[['reg_id']].drop_duplicates())

turtle_list = pandas.read_csv('E:/Ninggar/Mgstr/Penelitian/Data/files/sentence_triple_eval1/turtle_map.csv')

turtle_list = list(turtle_list[['ttl_file']].drop_duplicates().reset_index()['ttl_file'])
for ttl in turtle_list:
    print(ttl)
    g.parse(ttl, format='turtle')

Q1 = '''
select distinct (lcase(group_concat(distinct ?value; separator = '\\n')) as ?answer)
where
{{
  {{
    select distinct	 ?legalDocument ?year ?number ?Article
  					 (lcase(concat(?articleLabel,' ', coalesce(?sectionLabel, ''))) as ?contentLabel) ?value
    {{
      ?legalDocument a lexid-s:LegalDocument ;
                     rdfs:label '{document}'^^xsd:string .
      {{
        {{
          ?legalDocument lexid-s:amendedBy ?amendmentDocument .
          ?amendmentDocument lexid-s:hasContent ?article_I ;
                             lexid-s:regulationYear ?year ;
                             lexid-s:regulationNumber ?number .
          ?article_I lexid-s:modifies ?modification .
          ?modification lexid-s:hasModificationTarget ?contentParent ;
                        lexid-s:hasModificationContent ?contentModification .
          ?contentModification lexid-s:hasPart* ?Article.
        }}
        UNION
        {{
          ?legalDocument lexid-s:regulationYear ?year ;
                         lexid-s:regulationNumber ?number .
          ?legalDocument (lexid-s:hasContent|lexid:hasPart)* ?contentParent .
          ?contentParent lexid-s:hasPart* ?Article .
        }}
      }}
      ?Article a lexid-s:Article ;
               rdfs:label ?articleLabel .
      {{
        {{
        ?Article lexid-s:hasPart ?Section .
        ?Section a lexid-s:Section ;
                      rdfs:label ?sectionLabel ;
                      dct:description ?value .
        }}
        UNION
        {{
          ?Article dct:description ?value .
        }}
      }}
    }}
  }}
  filter(
    regex(?contentLabel, '{question}')
  )
}}
group by ?year ?number ?Article
order by desc(?year) ?desc(?number)
LIMIT 1
'''
# Q2 ='''
# SELECT distinct (coalesce(?label, ?ans) as ?answer)
# WHERE {{
#     ?LegalDocument a lexid-s:LegalDocument ;
#                  lexid-s:hasLegalBasis|lexid-s:implement|lexid-s:amend|lexid-s:repeal ?ans ;
#                  rdfs:label  '{document}'^^xsd:string.
#     ?ans a lexid-s:LegalDocument .
#     OPTIONAL{{
#         ?ans rdfs:label ?label .
#     }}
# }}
# '''
Q2 = '''
select distinct	 (?ldeletedArticle as ?answer)
{{
  ?legalDocument a lexid-s:LegalDocument ;
                 lexid-s:amendedBy ?amendmentDocument ;
                 rdfs:label '{document}'^^xsd:string .
  ?amendmentDocument lexid-s:hasContent ?articleI .
  ?articleI a lexid-s:Article ;
            rdfs:label 'Pasal I'^^xsd:string ;
            lexid-s:deletes ?deletedArticle .
  ?deletedArticle a lexid-s:Article ;
                  rdfs:label ?ldeletedArticle .
}}
'''
Q3 = '''
select distinct	 (lcase(?articleLabel) as ?answer)
{{
  ?legalDocument a lexid-s:LegalDocument ;
                 lexid-s:amendedBy ?amendmentDocument;
                 rdfs:label '{document}'^^xsd:string .
  ?amendmentDocument lexid-s:hasContent ?articleI .
  ?articleI a lexid-s:Article ;
            rdfs:label 'Pasal I'^^xsd:string ;
            lexid-s:adds ?addition .
  ?addition lexid-s:hasAdditionContent ?additionContent .
  ?additionContent lexid-s:hasPart* ?articleAddition .
  ?articleAddition a lexid-s:Article ;
                   rdfs:label ?articleLabel .
  filter(
    NOT EXISTS{{
      ?articleAddition lexid-s:deletedBy ?otherArticle .
    }}
  )
}}
LIMIT 200
'''
Q4 = '''
ASK
{{
  ?legalDocument a lexid-s:LegalDocument ;
                   lexid-s:amendedBy ?amendmentDocument ;
                   rdfs:label '{document}'^^xsd:string .
}}
'''

Q5 = '''
ASK
{{
  ?legalDocument a lexid-s:LegalDocument ;
                   lexid-s:repealedBy ?amendmentDocument ;
                   rdfs:label '{document}'^^xsd:string .
}}
'''

q_dict = {
    'Q5.1': {
        'query': Q1,
        'args': {
            'question': '',
            'document': ''
        }
    },
    'Q5.2': {
        'query': Q2,
        'args': {
            'document': ''
        }
    },
    'Q5.3': {
        'query': Q3,
        'args': {
            'document': ''
        }
    },
    'Q5.4': {
        'query': Q4,
        'args': {
            'document': ''
        }
    },
    'Q5.5': {
        'query': Q5,
        'args': {
            'document': ''
        }
    },
}
pairNum = 0
res = []
for pair in QAPairs.iterrows():
    pairNum += 1
    result = dict(pair[1])
    type1 = pair[1]['type1']
    type2 = pair[1]['type2']
    question = pair[1]['Q']
    if re.match(r'\W+', pair[1]['A'].lower()):
        expected = set([])
    elif type2 == 'Q5.1':
        expected = re.sub(r'\W+', ' ', pair[1]['A'].lower())
        expected = set(word_tokenize(expected))
    else:
        expected = re.split('; ', re.sub(r'[^;A-Za-z\d]+', ' ', pair[1]['A'].lower()))
        expected[-1] = re.sub(r'\W+', ' ', expected[-1]).strip()
        expected = set(expected)

    LegalDocumentType = re.search(r'((Peraturan|Undang-Undang).*)(Nomor\s\d+\sTahun|Tahun\s\d+\sNomor)\s',
                                  pair[1]['Q']).group(1)
    LegalDocumentNum = re.search(r'Nomor\s(\d+)(\s|\?)', pair[1]['Q']).group(1)
    LegalDocumentYear = re.search(r'Tahun\s(\d+)(\s|\?)', pair[1]['Q']).group(1)
    q_dict[type2]['args']['document'] = re.sub(r'\s+', ' ',
                                               LegalDocumentType.title() + ' Nomor ' + LegalDocumentNum + ' Tahun ' + LegalDocumentYear)
    if type2 == 'Q5.1':
        q_dict[type2]['args']['question'] = re.search(r'(Pasal\s*\d+(\s*ayat\s+\d+)?)', pair[1]['Q']).group(1).lower()
    # print('gggg')
    # print(question)
    # print(json.dumps(q_dict[type2], indent=4))
    # print(q_dict[type2]['query'].format(**q_dict[type2]['args']))
    # print(expected)
    # print(q_dict[type2]['args']['document'])
    results = g.query(q_dict[type2]['query'].format(**q_dict[type2]['args']))
    # print('jjj')
    # print(type2)
    print(pairNum)
    data = []
    for qres in results:
        if data == '':
            continue
        if type2 in ['Q5.4', 'Q5.5']:
            if (qres and type2 == 'Q5.4') or (not qres and type2 == 'Q5.5'):
                data.append('ya')
            else:
                data.append('tidak')
        else:
            data.append(re.sub(r'\W+', ' ', qres.answer.lower()).strip())
    if type2 == 'Q5.1' and len(data) == 1 and len(data[0]) > 0:
        temp = data[0]
        data = set(word_tokenize(data[0]))
    else:
        data = set(data)
    len_exact = len(expected)
    len_data = len(data)
    len_common = len(expected.intersection(data))
    if len_exact > 0 and len_data > 0:
        precision = len_common / len_data
        recall = len_common / len_exact
        if recall == 0 and precision == 0:
            f1 = 0
        else:
            f1 = 2 * (precision * recall) / (precision + recall)
    elif len_exact == 0 and len_data == 0:
        precision = 1
        recall = 1
        f1 = 1
    else:
        precision = 0
        recall = 0
        f1 = 0
    # print(data)
    # print(len_data, len_exact, len_common)
    # print(precision, recall, f1)
    # print(result)
    if len(data) == 0:
        result['answer'] = '-'
    if len(data) == 1:
        result['answer'] = list(data)[0]
    if type2 == 'Q5.1':
        result['answer'] = temp
    else:
        result['answer'] = ';\n'.join(list(data))
    result['len_A'] = len_exact
    result['len_answer'] = len_data
    result['len_common'] = len_common
    result['f1'] = f1
    res.append(result)
    # if pairNum>=5:
    #     break
res = pandas.DataFrame(res)
res.to_csv(re.sub(r'\.csv', 'evaluated.csv', sourceQA), index=False)
print(res)