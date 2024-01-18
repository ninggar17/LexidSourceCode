import re

import pandas
import requests

old_data_df = pandas.read_csv('data_peraturan.csv')
res = []
epoch = 0
for url in old_data_df['url_peraturan']:
    print(epoch, url)
    r = requests.get(url=url)
    try:
        content = r.content
        content = re.sub('</a><b>', '</a></b><b>', re.sub('<!--(([^-])|([^-]-))+-->|<br>|</b>', '', str(content)))
        texts = re.split('\\n', (re.sub('\n{2,}', '\n', re.sub('<', '\n<', re.sub('>', '>\n', re.sub('>\\s*<', '>\n<',
                                                                                                     re.sub(
                                                                                                         '(^b\')|((\\\\[rnt])+)|<!DOCTYPE html>|(\'$)',
                                                                                                         ' ',
                                                                                                         content)))))).strip())
        tab = ''
        all_dict = {}
        list_dict = [all_dict]
        current_dict = all_dict
        list_index = []
        for text in texts:
            try:
                tag = re.search('^<([^>\\s]+)', text).group(1)
                parse_texts = re.findall('[a-z-]+="[^"]+"', text)
                dict_text = {}
                dict_temp = {}
                for parse_text in parse_texts:
                    key_value = re.split('="', parse_text)
                    dict_temp[key_value[0]] = re.sub('"', '', key_value[1])
                if not re.match('^/', tag):
                    dict_text[tag] = dict_temp
                if not (re.match('meta|input|img|sc"\\+"ript|link|br|section', tag) or re.match('^/', tag)):
                    tab += '\t'
                    if not current_dict.keys().__contains__(tag):
                        current_dict[tag] = [dict_temp]
                    else:
                        current_dict[tag] += [dict_temp]
                    list_dict.append(current_dict[tag][len(current_dict[tag]) - 1])
                    current_dict = list_dict[len(list_dict) - 1]
                    list_index.append(0)
                elif re.match('^/', tag) and not re.match('/"\\+"script', tag):
                    list_dict.pop(len(list_dict) - 1)
                    list_index.pop(len(list_index) - 1)
                    current_dict = list_dict[len(list_dict) - 1]
                    tab = tab[0:len(tab) - 1]
                else:
                    if not current_dict.keys().__contains__(tag):
                        current_dict[tag] = [dict_temp]
                    else:
                        current_dict[tag] += [dict_temp]
            except AttributeError:
                current_dict['text_' + str(list_index[len(list_index) - 1])] = text
                list_index[len(list_index) - 1] += 1
        resline = [url]
        detail = {}
        for value in all_dict['html'][0]['body'][0]['div'][0]['div'][0]['div'][0]['p'][0]['div'][0]['div'][0]['div'] \
                [0]['div'][1]['form'][0]['div'][0]['div'][0]['table'][0]['tr']:
            key = value['th'][0]['text_0']
            try:
                value = value['td'][0]['div'][0]['text_0']
            except KeyError:
                value = ''
            detail[key] = value
        resline.append(str(detail))
        index_column = 0
        files = []
        try:
            for file in all_dict['html'][0]['body'][0]['div'][0]['div'][0]['div'][0]['p'][0]['div'][0]['div'] \
                    [1]['div'][0]['div'][1]['center'][index_column]['a']:
                files.append(file['href'])
            index_column += 1
        except Exception:
            pass
        resline.append(str(files))
        categories = []
        try:
            for category in all_dict['html'][0]['body'][0]['div'][0]['div'][0]['div'][0]['p'][0]['div'][0]['div'] \
                    [1]['div'][index_column]['div'][1]['span']:
                categories.append(category['text_0'])
            index_column += 1
        except Exception:
            pass
        resline.append(str(categories))
        relations = {}
        try:
            for relation in all_dict['html'][0]['body'][0]['div'][0]['div'][0]['div'][0]['p'][0]['div'][0]['div'] \
                    [1]['div'][index_column]['div'][1]['b']:
                rel_name = relation['text_0'].strip()
                list_of_legals = []
                for rel_url in relation['a']:
                    list_of_legals.append("http://peraturan.go.id" + rel_url['href'])
                relations[rel_name[0:len(rel_name) - 2]] = list_of_legals
        except Exception:
            pass
        resline.append(str(relations))
        res.append(resline)
    except Exception:
        pass
    epoch += 1
res_df = pandas.DataFrame(res, columns=['url', 'detail', 'files', 'categories', 'relations'])
res_df.to_csv('legal_datail.csv')
