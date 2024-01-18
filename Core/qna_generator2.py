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

zz = pandas.read_csv('abbrev.csv')

aa = xx.merge(yy, left_on='type', right_on='code', how='left')
aa = aa.merge(zz, left_on='detail', right_on='abbrev', how='left')
aa.loc[aa['sin'].isna(), 'sin'] = aa.loc[aa['sin'].isna(), 'detail'].str.title()
aa['sin'] = aa['sin'].replace(to_replace='_', value=' ', regex=True)
aa.loc[~aa['type1'].str.contains('Provinsi|Kabupaten|Kota|Gubernur|Bupati|Walikota', regex=True),
       'sin'] = aa.loc[~aa['type1'].str.contains('Provinsi|Kabupaten|Kota|Gubernur|Bupati|Walikota', regex=True),
                       'sin'].values + ' Republik Indonesia'
aa['text'] = aa['type1'] + ' ' + aa['sin'] + ' Nomor ' + aa['num'] + ' Tahun ' + aa['year']
# aa['Q1'] = 'Siapa yang menetapkan ' + aa['text'] + '?'
# aa['Q2'] = 'Siapa yang membuat ' + aa['text'] + '?'
# aa['Q2'] = 'Kapan ' + aa['text'] + ' diundangkan?'
# aa['Q3'] = 'Kapan ' + aa['text'] + ' ditetapkan?'
# aa['Q4'] = 'Apa pertimbangan dalam membuat ' + aa['text'] + '?'
aa['Q1'] = 'Apa saja dasar hukum dalam membuat ' + aa['text'] + '?'
aa['Q2'] = 'Apa saja peraturan yang berelasi dengan ' + aa['text'] + '?'
# aa['Q8'] = 'Apakah ' + aa['text'] + ' mengalami perubahan?'
# aa['Q9'] = 'Apakah ' + aa['text'] + ' masih berlaku?'
aa['Q3'] = 'Peraturan manakah yang diubah oleh ' + aa['text'] + '?'
aa['Q4'] = 'Peraturan manakah yang dicabut oleh ' + aa['text']+'?'
# aa['Q7'] = 'Apa saja Bab yang dibahas dalam ' + aa['text'] + '?'
# aa['Q8'] = 'Berapa jumlah Pasal yang diatur dalam ' + aa['text'] + '?'

q1 = aa[['regulatory', 'Q1']].rename(columns={'Q1': 'Q'})
q2 = aa[['regulatory', 'Q2']].rename(columns={'Q2': 'Q'})
q3 = aa[['regulatory', 'Q3']].rename(columns={'Q3': 'Q'})
q4 = aa[['regulatory', 'Q4']].rename(columns={'Q4': 'Q'})
q1['type1'] = 'Q2'
q1['type2'] = 'Q2.1'
q2['type1'] = 'Q2'
q2['type2'] = 'Q2.2'
q3['type1'] = 'Q2'
q3['type2'] = 'Q2.3'
q4['type1'] = 'Q2'
q4['type2'] = 'Q2.4'
q = pandas.concat([q1, q2, q3, q4], ignore_index=True)

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
    # if file_num > 1000:
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
    if not (file['title'].keys().__contains__('change') or
            re.search('PENCABUTAN (ATAS )?(?=PERATURAN|UNDANG)', file['title']['name'])):
        continue
    else:
        print(file['title']['name'])
        # print(file['title']['change'])
    try:
        filename = re.sub('json', 'pdf', re.search('[^/]+$', filesource).group(0))
        ans = {'filename': filename,
               'reg_id': file['title']['reg_id'],
               'A2.1': re.sub(', ', ';\n', re.sub(r'[\[\]\']', '', str(file['law_based']))).strip(),
               'A2.2': re.sub(', ', ';\n', re.sub(r'[\[\]\']', '', str(file['law_based']))).strip(),
               'A2.3': 'none',
               'A2.4': 'none'
               }
        if not re.search('PENCABUTAN (ATAS )?(?=PERATURAN|UNDANG)', file['title']['name']):
            ans['A2.2'] = re.sub(', ', ';\n', re.sub(r'[\[\]\']', '', str(file['law_based'] + [file['title']['change']]))).strip()
            ans['A2.3'] = file['title']['change']
        Ans.append(ans)
    except KeyError:
        continue
    qv2['filename'] = filename
    qv2['reg_id'] = file['title']['reg_id']
    qv2['type1'] = 'Q5'
    qv2['type2'] = 'Q5.1'
    if not re.search('PENCABUTAN (ATAS )?(?=PERATURAN|UNDANG)', file['title']['name']) and\
            len(file['values']) > 0:
        values = pandas.DataFrame(file['values'])
        values = values[values['id'].str.contains(r'^(ARTICLE|SECTION)') &
                        ~(values['id'].str.contains(r'V\d+$') |
                          values['id'].str.contains(r'_\d+[A-Z]'))][['id', 'value']]
        values['article'] = values['id'].replace(to_replace='^(ARTICLE|SECTION)_', value='', regex=True)
        values['article'] = values['article'].replace(to_replace=r'_\d+$', value='', regex=True)
        added_values = pandas.DataFrame(file['values'])
        added_values = added_values[added_values['id'].str.contains(r'^(ARTICLE|SECTION)') &
                                    added_values['id'].str.contains(r'_\d+[A-Z]')][['id', 'value']]
        added_values['num'] = 'Pasal ' + added_values['id'].replace(to_replace=r'^[A-Z]+_', value='', regex=True)
        added_values['num'] = added_values['num'].replace(to_replace='_', value=' ayat ', regex=True)
        added_values = re.sub(', ', ';\n', re.sub(r'[\[\]\']', '', str(list(added_values['num'].drop_duplicates())))).strip()
        values['num'] = 'Pasal ' + values['id'].replace(to_replace=r'^[A-Z]+_', value='', regex=True)
        values['num'] = values['num'].replace(to_replace='_', value=' ayat ', regex=True)
        values = values.reset_index()
        # values = list(values['num'])
        if len(values) > 0:
            try:
                Vrand = numpy.random.randint(0, len(values))
                qv2['Q'] = 'Bagaimana perubahan bunyi ' + (values.iloc[Vrand])['num'] + ' dalam ' + file['title']['change'] + '?'
                qv2['A'] = (values.iloc[Vrand])['value']
                Qv2.append(qv2.copy())
                qv2['type2'] = 'Q5.2'
                qv2['Q'] = 'Apa saja pasal yang dihapus dalam ' + file['title']['change'] + '?'
                qv2['A'] = ''
                Qv2.append(qv2.copy())
                qv2['type2'] = 'Q5.3'
                qv2['Q'] = 'Apa saja pasal yang ditambahkan dalam ' + file['title']['change'] + '?'
                qv2['A'] = added_values
                Qv2.append(qv2.copy())
                qv2['type2'] = 'Q5.4'
                qv2['Q'] = 'Apakah ' + file['title']['change'] + ' mengalami perubahan?'
                qv2['A'] = 'Ya'
                Qv2.append(qv2.copy())
            except Exception:
                continue
    elif re.search('PENCABUTAN (ATAS )?(?=PERATURAN|UNDANG)', file['title']['name']):
        qv2['type2'] = 'Q5.5'
        qv2['Q'] = 'Apakah ' + file['title']['reg_id'] + ' masih berlaku?'
        qv2['A'] = 'Tidak'
        Qv2.append(qv2.copy())
        print(qv2)
    else:
        continue
#
Qv2 = pandas.DataFrame(Qv2)
Ans = pandas.DataFrame(Ans)
# # Qv2 = Qv2.merge(aa, left_on='reg_id', right_on='regulatory')
# # Qv2 = Qv2[['regulatory', 'type1', 'type2', 'Q', 'A']]
# # Qv2['Qtype'] = 'Q4'
# # Qv2['Q'] = 'Bagaimanakah bunyi '+Qv2['value']+' dalam '+Qv2['text']+'?'
# # Qv2.loc[~(Qv2['modified_by'] == ''), 'Q'] = 'perubahan ' + Qv2.loc[~(Qv2['modified_by'] == ''), 'Q']
# # Qv2.loc[~(Qv2['modified_by'] == ''), 'Qtype'] = 'Q5'
# # Qv2['Q'] = Qv2['Q'].replace(to_replace='^perubahan Bagaimanakah', value='Bagaimanakah perubahan', regex=True)
# # Qmoddified = Qv2[~(Qv2['modified_by'] == '')]
# # Qmoddified['Q1'] = 'Apa sajakah pasal baru yang ditambahkan dalam ' + Qmoddified['text'] + '?'
# # Qmoddified['Q2'] = 'Apa sajakah pasal dalam ' + Qmoddified['text'] + ' yang dihapus?'
# # Qmoddified['Qrand'] = numpy.random.randint(0, 2, size=len(Qmoddified))
# # Qmoddified.loc[Qmoddified['Qrand'] == 0, 'Q'] = Qmoddified.loc[Qmoddified['Qrand'] == 0, 'Q1']
# # Qmoddified.loc[Qmoddified['Qrand'] == 1, 'Q'] = Qmoddified.loc[Qmoddified['Qrand'] == 1, 'Q2']
# # # print(Qmoddified[['regulatory', 'Qtype', 'Q']])
# # Qv2 = Qv2[~(Qv2['value'] == '')]
#
# # Qs = pandas.concat([aa[['Qtype', 'regulatory', 'Q']],
# #                    Qv2[['Qtype', 'regulatory', 'Q']],
# #                    Qmoddified[['Qtype', 'regulatory', 'Q']]], ignore_index=True)
# # Qs = Qs.sort_values('Qtype')
# # Qs.to_csv('Q1-Q5.csv', index=False)
a1 = Ans[['filename', 'reg_id', 'A2.1']].rename(columns={'A2.1': 'A'})
a2 = Ans[['filename', 'reg_id', 'A2.2']].rename(columns={'A2.2': 'A'})
a3 = Ans[['filename', 'reg_id', 'A2.3']].rename(columns={'A2.3': 'A'})
a4 = Ans[['filename', 'reg_id', 'A2.4']].rename(columns={'A2.4': 'A'})
a1['type2'] = 'Q2.1'
a2['type2'] = 'Q2.2'
a3['type2'] = 'Q2.3'
a4['type2'] = 'Q2.4'
a = pandas.concat([a1, a2, a3, a4], ignore_index=True)
q = q.merge(a, left_on=['regulatory', 'type2'], right_on=['reg_id', 'type2'], how='inner')
q = q[['filename', 'regulatory', 'type1', 'type2', 'Q', 'A']].rename(columns={'regulatory': 'reg_id'})
# q = q[q['type2'] != 'Q2.4']
q = q.merge(Qv2[Qv2['type2'] == 'Q5.5'][['reg_id']].rename(columns={'reg_id': 'repealed_doc'}),
            left_on='reg_id', right_on='repealed_doc', how='left')
q = q.loc[~((~q['repealed_doc'].isna() & (q['type2'] == 'Q2.3')) | (q['repealed_doc'].isna() & (q['type2'] == 'Q2.4')))]
q = q[['filename', 'reg_id', 'type1', 'type2', 'Q', 'A']]
q['nQ'] = q.groupby(by='reg_id')['type2'].transform('count')
q.to_csv('appendQ2.csv', index=False)
Qv2['nQ'] = Qv2[['filename', 'reg_id', 'type1', 'type2', 'Q', 'A']].drop_duplicates().groupby(by='filename')['type2'].transform('count')
Qv2.to_csv('Q5.csv', index=False)
# Q25 = pandas.concat([q.rename(columns={'regulatory': 'reg_id'}), Qv2], ignore_index=True)
# Q25.to_csv('Q2_Q5.csv', index=False)
