import json
import re
import time

import numpy
import pandas

pandas.set_option('display.max_rows', 300)
pandas.set_option('display.max_colwidth', 150)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)

xx = pandas.read_csv('E:/Ninggar/Mgstr/Penelitian/Data/files/sentence_triple/turtle_map.csv')[['regulatory']]
xx['regulatory'] = xx['regulatory'].replace(to_replace=r'-', value='_', regex=True)
xx['type'] = xx['regulatory']
xx['type'] = xx['type'].replace(to_replace=r'PERDA_DKI_JAKARTA', value='PERPROV_DKI_JAKARTA', regex=True)
xx['type'] = xx['type'].replace(to_replace=r'PERDA_JATIM', value='PERPROV_JATIM', regex=True)
xx['type'] = xx['type'].replace(to_replace=r'_([_A-Z\d]+)', value='', regex=True)
xx = xx[~xx['type'].str.contains('^(PERATURAN|PERUBAHAN|KEPUTUSAN|PBERSAMA|UUD)', regex=True)]
xx['detail'] = xx['regulatory'].replace(to_replace=r'^([A-Z]+_)', value='', regex=True)
xx['detail'] = xx['detail'].replace(to_replace=r'([\d_]+)$', value='', regex=True)
xx['year'] = xx['regulatory'].replace(to_replace=r'^([A-Z_]+_)', value='', regex=True)
xx['year'] = xx['year'].replace(to_replace=r'_\d+$', value='', regex=True)
xx['num'] = xx['regulatory'].replace(to_replace=r'^([_A-Z\d]+_)', value='', regex=True)
print(xx)

yy = pandas.read_csv('regulatory_type.csv')[['code', 'type1']].drop_duplicates()
yy = yy[~yy['code'].str.contains('^(PERATURAN|PERUBAHAN|KEPUTUSAN|PBERSAMA|UUD)', regex=True)]
yy['code'] = yy['code'].replace(to_replace='(_[_A-Z]+)', value='', regex=True)
yy.loc[yy['code'].str.contains('PERMENKO', regex=True), 'type1'] = yy.loc[
                                                                       yy['code'].str.contains('PERMENKO', regex=True),
                                                                       'type1'].values + '_Koordinator_Bidang'
yy.loc[yy['code'].str.contains('^KEP', regex=True), 'type1'] = 'Keputusan_' + yy.loc[
    yy['code'].str.contains('^KEP', regex=True), 'type1'].values
yy['type1'] = yy['type1'].replace(to_replace='^Keputusan_Peraturan', value='Keputusan', regex=True)
yy['type1'] = yy['type1'].replace(to_replace='_', value=' ', regex=True)
yy = yy.drop_duplicates()
print(yy.drop_duplicates())

zz = pandas.read_csv('abbrev.csv')
print(zz)

aa = xx.merge(yy, left_on='type', right_on='code', how='left')
aa = aa.merge(zz, left_on='detail', right_on='abbrev', how='left')
aa.loc[aa['sin'].isna(), 'sin'] = aa.loc[aa['sin'].isna(), 'detail'].str.title()
aa['sin'] = aa['sin'].replace(to_replace='_', value=' ', regex=True)
aa.loc[~aa['type1'].str.contains('Provinsi|Kabupaten|Kota|Gubernur|Bupati|Walikota', regex=True),
       'sin'] = aa.loc[~aa['type1'].str.contains('Provinsi|Kabupaten|Kota|Gubernur|Bupati|Walikota', regex=True),
                       'sin'].values + ' Republik Indonesia'
aa['text'] = aa['type1'] + ' ' + aa['sin'] + ' Nomor ' + aa['num'] + ' Tahun ' + aa['year']
aa['Q1'] = 'Siapa yang menetapkan ' + aa['text'] + '?'
# aa['Q2'] = 'Siapa yang membuat ' + aa['text'] + '?'
aa['Q2'] = 'Kapan ' + aa['text'] + ' diundangkan?'
aa['Q3'] = 'Kapan ' + aa['text'] + ' ditetapkan?'
aa['Q4'] = 'Apa pertimbangan dalam membuat ' + aa['text'] + '?'
aa['Q5'] = 'Apa saja dasar hukum dalam membuat ' + aa['text'] + '?'
aa['Q6'] = 'Apa saja peraturan yang berelasi dengan ' + aa['text'] + '?'
# aa['Q8'] = 'Apakah ' + aa['text'] + ' mengalami perubahan?'
# aa['Q9'] = 'Apakah ' + aa['text'] + ' masih berlaku?'
# aa['Q10'] = 'Peraturan manakah yang diubah oleh ' + aa['text'] + '?'
# aa['Q11'] = 'Peraturan manakah yang dicabut oleh ' + aa['text']+'?'
aa['Q7'] = 'Apa saja Bab yang dibahas dalam ' + aa['text'] + '?'
aa['Q8'] = 'Berapa jumlah Pasal yang diatur dalam ' + aa['text'] + '?'

print(aa[['regulatory', 'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8']])
q1 = aa[['regulatory', 'Q1']].rename(columns={'Q1': 'Q'})
q2 = aa[['regulatory', 'Q2']].rename(columns={'Q2': 'Q'})
q3 = aa[['regulatory', 'Q3']].rename(columns={'Q3': 'Q'})
q4 = aa[['regulatory', 'Q4']].rename(columns={'Q4': 'Q'})
q5 = aa[['regulatory', 'Q5']].rename(columns={'Q5': 'Q'})
q6 = aa[['regulatory', 'Q6']].rename(columns={'Q6': 'Q'})
q7 = aa[['regulatory', 'Q7']].rename(columns={'Q7': 'Q'})
q8 = aa[['regulatory', 'Q8']].rename(columns={'Q8': 'Q'})
q1['type1'] = 'Q1'
q1['type2'] = 'Q1.1'
q2['type1'] = 'Q1'
q2['type2'] = 'Q1.2'
q3['type1'] = 'Q1'
q3['type2'] = 'Q1.3'
q4['type1'] = 'Q1'
q4['type2'] = 'Q1.4'
q5['type1'] = 'Q2'
q5['type2'] = 'Q2.1'
q6['type1'] = 'Q2'
q6['type2'] = 'Q2.2'
q7['type1'] = 'Q3'
q7['type2'] = 'Q3.1'
q8['type1'] = 'Q3'
q8['type2'] = 'Q3.2'
q = pandas.concat([q1, q2, q3, q4, q5, q6, q7, q8], ignore_index=True)
print(q)
# aa.loc[aa['Qtype'] == 1, 'Qrand'] = numpy.random.randint(0, 6, size=len(aa[aa['Qtype'] == 1]))
# aa.loc[aa['Qtype'] == 2, 'Qrand'] = numpy.random.randint(0, 2, size=len(aa[aa['Qtype'] == 2]))
# aa['Q'] = aa['Q1']
# aa.loc[(aa['Qtype'] == 0) & (aa['Qrand'] == 1), 'Q'] = aa.loc[(aa['Qtype'] == 0) & (aa['Qrand'] == 1), 'Q2']
# aa.loc[(aa['Qtype'] == 0) & (aa['Qrand'] == 2), 'Q'] = aa.loc[(aa['Qtype'] == 0) & (aa['Qrand'] == 2), 'Q3']
# aa.loc[(aa['Qtype'] == 0) & (aa['Qrand'] == 3), 'Q'] = aa.loc[(aa['Qtype'] == 0) & (aa['Qrand'] == 3), 'Q4']
# aa.loc[(aa['Qtype'] == 0) & (aa['Qrand'] == 4), 'Q'] = aa.loc[(aa['Qtype'] == 0) & (aa['Qrand'] == 4), 'Q5']
# aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 0), 'Q'] = aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 0), 'Q6']
# aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 1), 'Q'] = aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 1), 'Q7']
# aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 2), 'Q'] = aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 2), 'Q8']
# aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 3), 'Q'] = aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 3), 'Q9']
# aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 4), 'Q'] = aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 4), 'Q10']
# aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 5), 'Q'] = aa.loc[(aa['Qtype'] == 1) & (aa['Qrand'] == 5), 'Q11']
# aa.loc[(aa['Qtype'] == 2) & (aa['Qrand'] == 0), 'Q'] = aa.loc[(aa['Qtype'] == 2) & (aa['Qrand'] == 0), 'Q12']
# aa.loc[(aa['Qtype'] == 2) & (aa['Qrand'] == 1), 'Q'] = aa.loc[(aa['Qtype'] == 2) & (aa['Qrand'] == 1), 'Q13']

# aa['Qtype'] = 'Q' + (aa['Qtype'] + 1).astype(str)
# print(aa[['regulatory', 'Qtype', 'Q']].head(100))

file_list = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')
global reg_types, prefixs
reg_types = pandas.read_csv('regulatory_type.csv')
prefixs = pandas.read_csv('prefixs.csv').fillna('')
resMap = pandas.read_csv('E:/Ninggar/Mgstr/Penelitian/Data/files/sentence_triple/turtle_map.csv').fillna('')
state = True
wdCache = pandas.read_csv('C:/Users/ningg/PycharmProjects/crawling/crawl/wikidata_cache.csv').fillna('')
file_num = 0
all_node, all_edge, all_refer, all_alias, all_concept, all_abstract, all_potriple = [], [], [], [], [], [], []
all_optriple, all_regulatory = [], []
dumps = 0
start = time.time()
Qv2 = []
Ans = []
while state:
    file_num += 1
    text = file_list.readline()
    print(file_num, text)
    if text == '':
        state = False
    filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_parsed_files/' + (
        re.sub('_pdf', '.json', re.sub('(%20)|([\\._]+)', '_',
                                       re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '',
                                              text)))).lower().strip()
    #     break
    # if file_num <= 28328:
    # if file_num > 300:
    #     break
    # if file_num > 1839:
    #     break
    # if file_num < 471:
    #     continue
    # if file_num > 471:
    #     break
    try:
        # filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_1_text_files/perda/2019/perwal_no_5_tahun_2019.txt'
        file = json.load(open(filesource, 'r'))
    except Exception:
        continue
    if re.search('putusan', filesource):
        continue
    qv2 = {}
    if re.search('PERUBAHAN|PENCABUTAN|MENCABUT', file['title']['name']):
        print(file['title']['name'])
        continue
    try:
        filename = re.sub('json', 'pdf', re.search('[^/]+$', filesource).group(0))
        bab = ''
        nochap = False
        for key_chap in file['body'].keys():
            if re.search('^.+CHAPTER_', key_chap):
                bab += re.sub('^.+CHAPTER_', 'BAB ', key_chap) + ': ' + file['body'][key_chap]['name'] + ';\n'
            else:
                nochap = True
                break
        if nochap:
            bab = '_'
        ans = {'filename': filename,
               'reg_id': file['title']['reg_id'],
               'A1.1': file['closing_part']['enactment']['official_name'],
               'A1.2': file['closing_part']['enactment']['date'],
               'A1.3': file['closing_part']['promulgation']['date'],
               'A1.4': re.sub('\', \'', ';\n', re.sub(r'^\[\'|\']$', '', str(file['considerans']))).strip(),
               'A2.1': re.sub(', ', ';\n', re.sub(r'[\[\]\']', '', str(file['law_based']))).strip(),
               'A2.2': re.sub(', ', ';\n', re.sub(r'[\[\]\']', '', str(file['law_based']))).strip(),
               'A3.1': bab
               }

        # print('1.1', file['closing_part']['enactment']['official_name'])
        # print('1.2', file['closing_part']['enactment']['date'])
        # print('1.3', file['closing_part']['promulgation']['date'])
        # print('1.4', re.sub('\', \'', ';\n', re.sub(r'^\[\'|\']$', '', str(file['considerans']))).strip())
        # print('2.1', re.sub(', ', ';\n', re.sub(r'[\[\]\']', '', str(file['law_based']))).strip())
        # print('2.2', re.sub(', ', ';\n', re.sub(r'[\[\]\']', '', str(file['law_based']))).strip())
        # print('3.1', bab.strip())
    except KeyError:
        continue
    qv2['filename'] = filename
    qv2['reg_id'] = file['title']['reg_id']
    qv2['type1'] = 'Q4'
    qv2['type2'] = 'Q4.1'
    if len(file['values']) > 0:
        values = pandas.DataFrame(file['values'])
        values = values[values['id'].str.contains(r'^(ARTICLE|SECTION)') &
                        ~(values['id'].str.contains(r'V\d+$') |
                          values['id'].str.contains(r'_\d+[A-Z]'))][['id', 'value']]
        values['article'] = values['id'].replace(to_replace='^(ARTICLE|SECTION)_', value='', regex=True)
        values['article'] = values['article'].replace(to_replace=r'_\d+$', value='', regex=True)
        values['article'] = values['article'].astype(int)
        # print(values)
        ans['A3.2'] = str(max(values['article']))
        values['num'] = 'Pasal ' + values['id'].replace(to_replace=r'^[A-Z]+_', value='', regex=True)
        values['num'] = values['num'].replace(to_replace='_', value=' ayat ', regex=True)
        values = values.reset_index()
        # values = list(values['num'])
        if len(values) > 0:
            try:
                Vrand1 = numpy.random.randint(0, len(values))
                Vrand2 = numpy.random.randint(0, len(values))
                if Vrand1 == Vrand2:
                    Vrand2 += 1
                if re.search('ayat', (values.iloc[Vrand1])['num']):
                    qv2['type2'] = 'Q4.2'
                qv2['Q'] = (values.iloc[Vrand1])['num']
                qv2['A'] = (values.iloc[Vrand1])['value']
                Qv2.append(qv2.copy())
                # print(values)
                # print(Vrand1, Vrand2)
                if re.search('ayat', (values.iloc[Vrand2])['num']):
                    qv2['type2'] = 'Q4.2'
                else:
                    qv2['type2'] = 'Q4.1'
                qv2['Q'] = (values.iloc[Vrand2])['num']
                qv2['A'] = (values.iloc[Vrand2])['value']
                Qv2.append(qv2.copy())
            except Exception:
                continue
        Ans.append(ans)
    else:
        continue
    # print(ans)

Qv2 = pandas.DataFrame(Qv2)
Ans = pandas.DataFrame(Ans)
# print(Qv2)
# Qv2 = Qv2.merge(aa, left_on='reg_id', right_on='regulatory')
# Qv2 = Qv2[['regulatory', 'type1', 'type2', 'Q', 'A']]
# Qv2['Qtype'] = 'Q4'
# Qv2['Q'] = 'Bagaimanakah bunyi '+Qv2['value']+' dalam '+Qv2['text']+'?'
# Qv2.loc[~(Qv2['modified_by'] == ''), 'Q'] = 'perubahan ' + Qv2.loc[~(Qv2['modified_by'] == ''), 'Q']
# Qv2.loc[~(Qv2['modified_by'] == ''), 'Qtype'] = 'Q5'
# Qv2['Q'] = Qv2['Q'].replace(to_replace='^perubahan Bagaimanakah', value='Bagaimanakah perubahan', regex=True)
# Qmoddified = Qv2[~(Qv2['modified_by'] == '')]
# Qmoddified['Q1'] = 'Apa sajakah pasal baru yang ditambahkan dalam ' + Qmoddified['text'] + '?'
# Qmoddified['Q2'] = 'Apa sajakah pasal dalam ' + Qmoddified['text'] + ' yang dihapus?'
# Qmoddified['Qrand'] = numpy.random.randint(0, 2, size=len(Qmoddified))
# Qmoddified.loc[Qmoddified['Qrand'] == 0, 'Q'] = Qmoddified.loc[Qmoddified['Qrand'] == 0, 'Q1']
# Qmoddified.loc[Qmoddified['Qrand'] == 1, 'Q'] = Qmoddified.loc[Qmoddified['Qrand'] == 1, 'Q2']
# # print(Qmoddified[['regulatory', 'Qtype', 'Q']])
# Qv2 = Qv2[~(Qv2['value'] == '')]

# Qs = pandas.concat([aa[['Qtype', 'regulatory', 'Q']],
#                    Qv2[['Qtype', 'regulatory', 'Q']],
#                    Qmoddified[['Qtype', 'regulatory', 'Q']]], ignore_index=True)
# Qs = Qs.sort_values('Qtype')
# Qs.to_csv('Q1-Q5.csv', index=False)
print(Ans)
a1 = Ans[['filename', 'reg_id', 'A1.1']].rename(columns={'A1.1': 'A'})
a2 = Ans[['filename', 'reg_id', 'A1.2']].rename(columns={'A1.2': 'A'})
a3 = Ans[['filename', 'reg_id', 'A1.3']].rename(columns={'A1.3': 'A'})
a4 = Ans[['filename', 'reg_id', 'A1.4']].rename(columns={'A1.4': 'A'})
a5 = Ans[['filename', 'reg_id', 'A2.1']].rename(columns={'A2.1': 'A'})
a6 = Ans[['filename', 'reg_id', 'A2.2']].rename(columns={'A2.2': 'A'})
a7 = Ans[['filename', 'reg_id', 'A3.1']].rename(columns={'A3.1': 'A'})
a8 = Ans[['filename', 'reg_id', 'A3.2']].rename(columns={'A3.2': 'A'})
a1['type2'] = 'Q1.1'
a2['type2'] = 'Q1.2'
a3['type2'] = 'Q1.3'
a4['type2'] = 'Q1.4'
a5['type2'] = 'Q2.1'
a6['type2'] = 'Q2.2'
a7['type2'] = 'Q3.1'
a8['type2'] = 'Q3.2'
a = pandas.concat([a1, a2, a3, a4, a5, a6, a7, a8], ignore_index=True)
q = q.merge(a, left_on=['regulatory', 'type2'], right_on=['reg_id', 'type2'], how='inner')
q = q[['filename', 'regulatory', 'type1', 'type2', 'Q', 'A']]
Q14 = pandas.concat([q.rename(columns={'regulatory': 'reg_id'}), Qv2], ignore_index=True)
Q14['nQ'] = Q14.groupby(by='reg_id')['type2'].transform('count')
Q14.to_csv('Q1-Q4.csv', index=False)
