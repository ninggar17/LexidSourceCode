import json
import os
import re

import pandas
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)
pandas.set_option('display.max_colwidth', 300)
pandas.set_option('display.max_rows', 300)
file_list = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')
# global reg_types
reg_types = pandas.read_csv('regulatory_type.csv')
# last_error_file = open('last_error.txt', 'r+')
# last_error = last_error_file.readline()
# last_error_file.close()
file_num = 0
state = True
# next = True
# # res = None
# failed = []
# types = []
# stop = False
# empty = 0
# filterDoc = None
# while state:
#     text = file_list.readline()
#     if text == '':
#         state = False
#     print(file_num, text)
#     text = text.strip()
#     filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_1_text_files/' + (
#         re.sub('_pdf', '.txt', re.sub('(%20)|([\\._]+)', '_',
#                                       re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
#     filetarget = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_parsed_files/' + (
#         re.search(r'(.{,100})', re.sub('_pdf', '', re.sub('(%20)|([\\._]+)', '_',
#                                                           re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
#                                                                  '', text)))).group(1).lower()) + '.json'
#     dirName = re.sub('/[^/]+$', '', filetarget)
#     try:
#         os.makedirs(dirName)
#         print("Directory ", dirName, " Created ")
#     except Exception:
#         None
#     try:
#         file = open(filesource, encoding="utf8")
#     except Exception:
#         file_num += 1
#         continue
#     res = None
#     if re.search('putusan', filesource):
#         file_num += 1
#         continue
#     text = file.read()
#     text = re.sub('\n+\s*\d+[^\w;]*([Nn][Oo]\\.\s*(-?\s*\d+\s*-?\s*)+)?\n+', '\n',
#                   re.sub(r'\n+(\s*\d+\s*\n+)?', '\n',
#                          re.sub('\\nwww.djpp.(depkumham|kemenkumham).go.id|https://jdih.bandung.go.id/|www.peraturan.go.id\\n', '\n',
#                                 re.sub(r'[^a-zA-Z\d\n./:(\-,);]', ' ',
#                                        re.sub(r'\n+', '\n',
#                                               re.sub(r'\n\s*\n', '\n\n', text)))))).strip()
#
#     if re.match(r'^[\s\n\W]*$', text):
#         file_num += 1
#         print(empty)
#         empty += 1
#         continue
#     file_num += 1
# print('empty:', empty)
# res = {}
# while state:
#     file_num += 1
#     text = file_list.readline()
#     # print(file_num, text)
#     if text == '':
#         state = False
#     filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_parsed_files/' + (
#         re.sub('_pdf', '.json', re.sub('(%20)|([\\._]+)', '_',
#                                        re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '',
#                                               text)))).lower().strip()
#
#     try:
#         file = json.load(open(filesource, 'r'))
#     except Exception:
#         continue
#     print(file_num, file['title']['type'], reg_types[reg_types['type']==file['title']['type']]['type1'].values[0])
#     try:
#         res[reg_types[reg_types['type']==file['title']['type']]['type1'].values[0]] += 1
#     except KeyError:
#         res[reg_types[reg_types['type']==file['title']['type']]['type1'].values[0]] = 0
#     # print(reg_types[reg_types['type']==file['title']['type']]['type1'].values[0])
#     # break
#     # try:
#     #     if len(file['considerans']) > 1 or (
#     #             len(file['considerans']) == 1 and not re.match(r'^\W*$', file['considerans'][0])):
#     #         try:
#     #             res['considerans'] += 1
#     #         except KeyError:
#     #             res['considerans'] = 0
#     # except KeyError:
#     #     pass
#     # try:
#     #     if len(file['law_based']) > 1 or (
#     #             len(file['law_based']) == 1 and not re.match(r'^\W*$', file['law_based'][0])):
#     #         try:
#     #             res['law_based'] += 1
#     #         except KeyError:
#     #             res['law_based'] = 0
#     # except KeyError:
#     #     pass
#     # try:
#     #     if not re.match(r'^\W*$', file['dictum']):
#     #         try:
#     #             res['dictum'] += 1
#     #         except KeyError:
#     #             res['dictum'] = 0
#     # except KeyError:
#     #     pass
#     # try:
#     #     if len(file['values']) > 1:
#     #         try:
#     #             res['values'] += 1
#     #         except KeyError:
#     #             res['values'] = 0
#     # except KeyError:
#     #     pass
#     # try:
#     #     if len(file['body']) > 1:
#     #         try:
#     #             res['body'] += 1
#     #         except KeyError:
#     #             res['body'] = 0
#     # except KeyError:
#     #     pass
#     # keys = []
#     # for k in file['title'].keys():
#     #     if isinstance(file['title'][k], str) and not re.match(r'^\W*$', file['title'][k]):
#     #         keys.append(k)
#     #     elif isinstance(file['title'][k], list) and (len(file['title'][k]) > 1 or (
#     #             len(file['title'][k]) == 1 and not re.match(r'^\W*$', file['title'][k][0]))):
#     #         keys.append(k)
#     #     elif isinstance(file['title'][k], dict) and (len(file['title'][k]) > 1 or (
#     #             len(file['title'][k]) == 1 and not re.match(r'^\W*$', str(list(file['title'][k].values())[0])))):
#     #         keys.append(k)
#     # try:
#     #     for k in file['closing_part']['enactment'].keys():
#     #         if not re.match(r'^\W*$', str(file['closing_part']['enactment'][k])):
#     #             keys.append('enaction_' + k)
#     #             print(k, file['closing_part']['enactment'][k])
#     #     print()
#     #     for k in file['closing_part']['promulgation'].keys():
#     #         if not re.match(r'^\W*$', str(file['closing_part']['promulgation'][k])):
#     #             keys.append('promulgation_' + k)
#     # except KeyError:
#     #     pass
#     # for k in keys:
#     #     try:
#     #         res[k] += 1
#     #     except KeyError:
#     #         res[k] = 0
# print(res)

# def longestCommonPrefix(strs):
#     common = strs[0]
#     for s in strs[1:]:
#         if len(s) > len(common):
#             s1 = common
#             s2 = s
#         else:
#             s1 = s
#             s2 = common
#         if s1 == '' or s1[0] != s2[0]:
#             return ''
#         elif re.search('^' + s1, s2):
#             common = s1
#         else:
#             for k in range(1, len(s1)):
#                 if s1[k] != s2[k]:
#                     common = s1[:k]
#
#     return common
#
# print(longestCommonPrefix(["flower","flow","flight"]))
#
# ["foo", "bar", "baz"].index("xx")
