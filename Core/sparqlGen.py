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
sourceQA = 'E:/Rate/Q1-Q4/SampleQnA.csv'
# sourceQA = 'E:/Rate/Q5/SampleQ5.csv'
# sourceQA = 'E:/Rate/Q6/SampleQ6.csv'
resQA = re.sub('\.csv', '_Res.csv', sourceQA)

QAPairs = pandas.read_csv(re.sub(r'\.csv', 'Merged.csv', sourceQA))
print(QAPairs[['reg_id']].drop_duplicates())

month = {'januari': '1', 'februari': '2', 'maret': '3', 'april': '4', 'mei': '5', 'juni': '6',
         'juli': '7', 'agustus': '8', 'september': '9', 'oktober': '10', 'november': '11', 'desember': '12'}
turtle_list = pandas.read_csv('E:/Ninggar/Mgstr/Penelitian/Data/files/sentence_triple_eval/turtle_map.csv')

turtle_list = list(turtle_list[['ttl_file']].drop_duplicates().reset_index()['ttl_file'])
max = 15
iter = 0
for ttl in turtle_list:
    print(ttl)
    g.parse(ttl, format='turtle')
    # break
    # iter+=1
    # if iter>=max:
    #     break
#
print('hkhidhi')
LegalDocument = 'Peraturan Pemerintah Republik Indonesia Nomor 29 Tahun 1982'
q1 = """
SELECT distinct (coalesce(?label, ?ans) as ?answer)
WHERE {{
    ?LegalDocument a lexid-s:LegalDocument ;
                 lexid-s:{question} ?ans ;
                 rdfs:label '{document}'^^xsd:string.
    OPTIONAL{{
        ?ans rdfs:label ?label .
    }}
            
}}
"""

q2_1 = """
SELECT distinct (coalesce(?label, ?ans) as ?answer)
WHERE {{
    ?LegalDocument a lexid-s:LegalDocument ;
                 lexid-s:{question} ?ans ;
                 rdfs:label  '{document}'^^xsd:string.
    ?ans a lexid-s:LegalDocument .
    ?ans rdfs:label ?label .
}}
"""
q2_2 = """
SELECT distinct (coalesce(?label, ?ans) as ?answer)
WHERE {{
    ?LegalDocument a lexid-s:LegalDocument ;
                 lexid-s:hasLegalBasis|lexid-s:implements|lexid-s:amends|lexid-s:repeals ?ans ;
                 rdfs:label  '{document}'^^xsd:string.
    ?ans a lexid-s:LegalDocument .
    ?ans rdfs:label ?label .
}}
"""

q3 = """
SELECT distinct (concat(?contentLabel, ': ', ?contentName) as ?answer)
WHERE {{
    ?LegalDocument a lexid-s:LegalDocument ;
                 lexid-s:hasContent ?topLevelContent ;
                 rdfs:label  '{document}'^^xsd:string.
    ?topLevelContent lexid-s:hasPart* ?content .
    ?content a lexid-s:{question} ;
             rdfs:label ?contentLabel ;
             lexid-s:name ?contentName .
}}
limit 100
"""

q4_1 = """
SELECT distinct (concat(coalesce(?sectionName, ''), ' ', ?ans) as ?answer)
WHERE {{
        ?LegalDocument a lexid-s:LegalDocument ;
                     lexid-s:hasContent ?topLevelContent ;
                     rdfs:label  '{document}'^^xsd:string.
        ?topLevelContent lexid-s:hasPart* ?article .
        ?article a lexid-s:Article ;
                 rdfs:label ?label ;
                 rdfs:label '{question}'^^xsd:string .
        OPTIONAL{{
            ?article lexid-s:hasPart ?section .
            ?section a lexid-s:Section ;
                    lexid-s:name ?sectionName;
                    dct:description ?ans .
        }}
        OPTIONAL
        {{
            ?article dct:description ?ans .
        }}
}}
limit 100
"""

q4_2 = """
SELECT distinct ?answer
WHERE {{
        ?LegalDocument a lexid-s:LegalDocument ;
                     lexid-s:hasContent ?topLevelContent ;
                     rdfs:label  '{document}'^^xsd:string.
        ?topLevelContent lexid-s:hasPart* ?article .
        ?article a lexid-s:Article ;
                 rdfs:label ?label ;
                 rdfs:label '{question1}'^^xsd:string ;
                 lexid-s:hasPart ?section .
        ?section a lexid-s:Section ;
                 rdfs:label '{question2}'^^xsd:string ;
                 dct:description ?answer .
}}
limit 100
"""

q_dict = {'Q1.1': {
    'query': q1,
    'args': {
        'question': 'hasEnactionOfficial',
        'document': ''
    }
},
    'Q1.2': {
        'query': q1,
        'args': {
            'question': 'hasEnactionDate',
            'document': ''
        }
    },
    'Q1.3': {
        'query': q1,
        'args': {
            'question': 'hasPromulgationDate',
            'document': ''
        }
    },
    'Q1.4': {
        'query': q1,
        'args': {
            'question': 'consider',
            'document': ''
        }
    },
    'Q2.1': {
        'query': q2_1,
        'args': {
            'question': 'hasLegalBasis',
            'document': ''
        }
    },
    'Q2.2': {
        'query': q2_2,
        'args': {
            'document': ''
        }
    },
    'Q2.3': {
        'query': q2_1,
        'args': {
            'question': 'amends',
            'document': ''
        }
    },
    'Q2.4': {
        'query': q2_1,
        'args': {
            'question': 'repeals',
            'document': ''
        }
    },
    'Q3.1': {
        'query': q3,
        'args': {
            'question': 'Chapter',
            'document': ''
        }
    },
    'Q3.2': {
        'query': q3,
        'args': {
            'question': 'Article',
            'document': ''
        }
    },
    'Q4.1': {
        'query': q4_1,
        'args': {
            'question': '',
            'document': ''
        }
    },
    'Q4.2': {
        'query': q4_2,
        'args': {
            'question1': '',
            'question2': '',
            'document': ''
        }
    }
}
pairNum = 0
res = []
for pair in QAPairs.iterrows():
    print(pairNum)
    try:
        result = {}
        type1 = pair[1]['type1']
        type2 = pair[1]['type2']
        # if not type2 in ['Q2.1']:
        #     continue
        LegalDocumentType = re.search(r'((Peraturan|Undang-Undang).*)(Nomor\s\d+\sTahun|Tahun\s\d+\sNomor)\s',
                                      pair[1]['Q']).group(1)
        LegalDocumentType = re.sub('Peraturan.+(diubah|dicabut|berelasi).+(?=Peraturan)|Peraturan.+(diubah|dicabut|berelasi).+(?=Undang)', '', LegalDocumentType)
        LegalDocumentNum = re.search(r'Nomor\s(\d+)(\s|\?)', pair[1]['Q']).group(1)
        LegalDocumentYear = re.search(r'Tahun\s(\d+)(\s|\?)', pair[1]['Q']).group(1)
        labelDoc = re.sub(r'\s+', ' ',
                          LegalDocumentType.title() + ' Nomor ' + LegalDocumentNum + ' Tahun ' + LegalDocumentYear)
        q_dict[type2]['args']['document'] = labelDoc
        if type2 == 'Q4.1':
            q_dict[type2]['args']['question'] = re.search(r'(Pasal\s\d+)', pair[1]['Q']).group(1)
        if type2 == 'Q4.2':
            q_dict[type2]['args']['question1'] = re.search(r'(Pasal\s\d+)', pair[1]['Q']).group(1)
            q_dict[type2]['args']['question2'] = re.search(r'(ayat\s\d+)', pair[1]['Q']).group(1)
        results = g.query(q_dict[type2]['query'].format(**q_dict[type2]['args']))
        answer = ''
        lenQres = 0
        for qres in results:
            temp = (re.sub('^.+/data/', '', qres.answer)).strip()

            if type1 == 'Q2' and re.search('_', temp):
                continue
            if not type2 in ['Q1.2', 'Q1.3']:
                temp = (re.sub(r'(?i)[^a-z\d]+', ' ', temp.lower())).strip()
            if temp == '':
                continue
            answer += temp + ';\n'
            lenQres += 1
        if pair[1]['type2'] == 'Q3.2':
            answer = lenQres
        else:
            answer = answer[:-2]
        result['answer'] = answer
        result['id'] = '0'
        result['reg_id'] = pair[1]['reg_id']
        result['type1'] = type1
        result['type2'] = type2
        result['Q'] = pair[1]['Q']
        result['A'] = pair[1]['A']
        result['isValid'] = pair[1]['isValid']
        res.append(result)
    except KeyError:
        print(KeyError.with_traceback())
        continue
    pairNum += 1
    # break
res = pandas.DataFrame(res)
QAPairs = res[['reg_id', 'type1', 'type2', 'Q', 'A', 'isValid', 'answer']]
TFPN = []
for pair in QAPairs.iterrows():
    answer = {'reg_id': pair[1]['reg_id'], 'type2': pair[1]['type2']}
    if pair[1]['type2'] in ['Q1.1']:
        exact = re.sub(r'\W+', ' ', pair[1]['A']).lower().strip()
        data = re.sub(r'\W+', ' ', str(pair[1]['answer'])).lower().strip()
        if data == 'nan':
            common = 0
            len_answer = 0
            len_A = 1
        elif exact == data:
            common = 1
            len_answer = 1
            len_A = 1
        else:
            common = 0
            len_answer = 1
            len_A = 1
    elif pair[1]['type2'] in ['Q3.2']:
        if str(pair[1]['answer']) == 'nan':
            pair[1]['answer'] = 0
        exact = int(pair[1]['A'])
        data = int(pair[1]['answer'])
        common = min(data, exact)
        len_answer = data
        len_A = exact
    elif pair[1]['type2'] in ['Q1.2', 'Q1.3']:
        dd = re.search(r'^\d+', pair[1]['A']).group(0)
        mm = month[re.search(r'[a-z]+', pair[1]['A']).group(0)]
        yyyy = re.search(r'\d+$', pair[1]['A']).group(0)
        exact = date(year=int(yyyy), month=int(mm), day=int(dd))
        try:
            data = date.fromisoformat(pair[1]['answer'])
            if exact == data:
                common = 1
                len_answer = 1
                len_A = 1
            else:
                common = 0
                len_answer = 1
                len_A = 1
        except Exception:
            common = 0
            len_answer = 0
            len_A = 1

    elif pair[1]['type2'] in ['Q1.4', 'Q3.1']:
        if not re.match(r'(?i)[^a-z\d]+', pair[1]['A'].lower()):
            exact = set(re.split(r';[\s\n]', re.sub(r'[^;\w]+|\W+$', ' ', pair[1]['A'].lower()).strip()))
        else:
            exact = set([])
        data = str(pair[1]['answer']).lower()
        if data != 'nan' and data != '':
            data = set(re.split(r';[\s\n]+', re.sub(r'[^;\w]+|\W+$', ' ', data.lower()).strip()))
        else:
            data = set([])
        common = len(exact.intersection(data))
        len_answer = len(data)
        len_A = len(exact)
    elif pair[1]['type1'] in ['Q2']:
        if pair[1]['A'].lower() != '-':
            exact = re.split(r';\s+', pair[1]['A'])
        else:
            exact = []
        for idx in range(len(exact)):
            exact[idx] = re.sub(r'(?i)[^a-z\d]+', ' ', exact[idx].lower()).strip()
            rtype = re.search(r'^(([a-z]+\d*[a-z]*\s)+)(tahun|nomor)', exact[idx]).group(1).strip()
            year = re.search(r'(tahun \d+)', exact[idx]).group(1).strip()
            try:
                num = re.search(r'(nomor \d+)', exact[idx]).group(1).strip()
            except Exception:
                num = ''
            exact[idx] = rtype + ' ' + num + ' ' + year
        exact = set(exact)
        data = str(pair[1]['answer']).lower()
        if data != 'nan' and data != '':
            data = set(re.split(r';\n', re.sub(r'[^;\n\w]+', ' ', data.lower())))
        else:
            data = set([])
        common = len(exact & data)
        len_answer = len(data)
        len_A = len(exact)
        # if common != len_answer:
        #     print(pair[1]['type2'])
        #     print(pair[1]['reg_id'])
        #     print(exact)
        #     print(data)
        #     print(data - exact)
        #     print()
    elif pair[1]['type2'] in ['Q4.1', 'Q4.2']:
        data = re.sub(r'(?i)[^a-z\d]+', ' ', pair[1]['answer']).lower().strip()
        if data != 'nan':
            data = set(word_tokenize(data))
        else:
            data = set([])
        exact = re.sub(r'(?i)[^a-z\d]+', ' ', pair[1]['A']).lower().strip()
        exact = set(word_tokenize(exact))
        len_answer = len(data)
        len_A = len(exact)
        common = len(exact & data)
    else:
        common = 0
        len_answer = 1
        len_A = 1
    if len_A == 0 and len_answer == 0:
        precision = 1
        common = 1
        f1 = 1
    elif len_answer != 0:
        precision = common / len_answer
        recall = common / len_A
        if recall == 0 and precision == 0:
            f1 = 0
        else:
            f1 = 2 * (precision * recall) / (precision + recall)
    else:
        precision = 0
        recall = 0
        f1 = 0
    answer['reg_id'] = pair[1]['reg_id']
    answer['type1'] = pair[1]['type1']
    answer['type2'] = pair[1]['type2']
    answer['Q'] = pair[1]['Q']
    answer['A'] = pair[1]['A']
    answer['isValid'] = pair[1]['isValid']
    answer['answer'] = pair[1]['answer']
    answer['len_common'] = common
    answer['len_exact'] = len_A
    answer['len_data'] = len_answer
    answer['f1'] = f1
    TFPN.append(answer)
QAPairs = pandas.DataFrame(TFPN)
# QAPairs = QAPairs.merge(TFPN.add_suffix('xx'), left_on=['reg_id', 'type2'], right_on=['reg_idxx', 'type2xx'],
#                         how='left')
# QAPairs[['T', 'lenExact', 'lenData', 'F1']] = QAPairs[['Txx', 'lenExactxx', 'lenDataxx', 'F1xx']]
# QAPairs = QAPairs[['reg_id', 'type1', 'type2', 'Q', 'A', 'isValid', 'answer', 'T', 'lenExact', 'lenData', 'F1']]
QAPairs.to_csv(re.sub(r'\.csv', 'evaluated.csv', sourceQA), index=False)
# allCommon = sum(QAPairs['len_common'])
# lenExact = sum(QAPairs['len_exact'])
# lenData = sum(QAPairs['len_data'])
# precision = allCommon / lenExact
# recall = allCommon / lenData
# f1 = 2 * (precision * recall) / (precision + recall)
# avgF1 = sum(QAPairs['f1'])/len(QAPairs['f1'])
print(QAPairs)
# print(f1, avgF1)
