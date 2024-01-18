import json
import os
import re
import time
import warnings
from datetime import datetime

import numpy
import pandas
import requests
from pandasql import sqldf
import rdfpandas

warnings.filterwarnings('ignore')
pandas.set_option('display.max_rows', 300)
pandas.set_option('display.max_colwidth', 100)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)

schema = 'lexid-s:'
data = 'lexid:'
types = 'rdf:type'
label = 'rdfs:label'
description = 'dct:description'
sameAs = 'owl:sameAs'
LegalDocument = schema + 'LegalDocument'
LegalDocumentContent = schema + 'LegalDocumentContent'
Chapter = schema + 'Chapter'
Part = schema + 'Part'
Paragraph = schema + 'Paragraph'
Article = schema + 'Article'
Section = schema + 'Section'
Items = schema + 'Item'
LawAmendment = schema + 'LawAmendment'
LawAddition = schema + 'LawAddition'
LawModification = schema + 'LawModification'
RuleExpression = schema + 'RuleExpression'
Norm = schema + 'Norm'
RuleAct = schema + 'RuleAct'
Concept = schema + 'Concept'
CompoundExpression = schema + 'CompoundExpression'
PlaceOfPromulgation = schema + 'PlaceOfPromulgation'
Person = schema + 'Person'
Position = schema + 'Position'
City = schema + 'City'
Things = 'owl:Thing'


def text_to_num(num_t):
    num_dict = {'pertama': [1, 1], 'satu': [1, 1], 'dua': [2, 1], 'tiga': [3, 1], 'empat': [4, 1], 'lima': [5, 1],
                'enam': [6, 1], 'tujuh': [7, 1], 'delapan': [8, 1], 'sembilan': [9, 1], 'sebelas': [11, 1],
                'sepuluh': [10, 1], 'seratus': [100, 1], 'seribu': [1000, 1], 'belas': [10, 1]}
    time_dict = {'puluh': [10, 1], 'ratus': [100, 2]}
    cubes = {'ribu': [1e3, 1], 'juta': [1e6, 2], 'milyar': [1e9, 3]}
    temp = 0
    res = 0
    num_t = num_t.split(' ')
    x = 0
    for t in num_t:
        try:
            x += num_dict[t][0]
        except KeyError:
            try:
                temp += x * time_dict[t][0]
                x = 0
            except KeyError:
                temp = (temp + x) * cubes[t][0]
                res += temp
                temp = 0
                x = 0
    res += temp + x
    res
    return res


def get_body_values(chapters, title, reg_ids, prev=None, suffix=''):
    triples = []
    next_part = {'changes': 'chapters', 'chapters': 'parts', 'parts': 'paragraphs', 'paragraphs': 'articles',
                 'articles': 'sections'}
    containsMap = {'Chapter': 'BAB', 'Part': 'BAGIAN', 'Paragraph': 'Paragraf', 'Article': 'Pasal', 'Section': 'ayat'}
    for chapter in chapters.keys():
        if not re.search('(?i)change|chapter|part|paragraph|article|section', chapter):
            continue
        try:
            part = re.search('(?i)change|chapter|part|paragraph|article|section', chapter).group(0).lower() + 's'
            old_chapter = chapter
            chapter = re.sub(r'^.+' + part[:-1].upper(), reg_ids + '_' + part[:-1].title(), chapter)
            new_chapter = chapter
            try:
                if ['paragraphs', 'parts'].__contains__(part):
                    part_text = re.search('(PARAGRAPH|PART)_[IVXLCDM]+_((KE)?([A-Z ]+))(_|$)', new_chapter)
                    new_part_text = text_to_num(part_text.group(4).lower())
                    new_chapter = new_chapter.replace(part_text.group(2), str(new_part_text)) + suffix
                elif ['sections', 'articles'].__contains__(part):
                    new_chapter = re.sub(r'\s', '', new_chapter) + suffix
                else:
                    new_chapter += suffix
            except Exception:
                new_chapter = re.sub(r'[\W\s\n]+', '_', new_chapter)
            if prev is not None:
                triples.append({'source': data + new_chapter, 'rel_name': schema + 'isPartOf', 'target': data + prev})
                triples.append({'source': data + prev, 'rel_name': schema + 'hasPart', 'target': data + new_chapter})
            else:
                triples.append(
                    {'source': data + new_chapter, 'rel_name': schema + 'isContentOf', 'target': data + reg_ids})
                triples.append(
                    {'source': data + reg_ids, 'rel_name': schema + 'hasContent', 'target': data + new_chapter})
            try:
                if part != 'sections' and chapters[old_chapter].keys().__contains__(next_part[part]) and len(
                        chapters[old_chapter][next_part[part]]) > 0:
                    triples += get_body_values(chapters[old_chapter][next_part[part]], title, reg_ids, new_chapter)
                elif part not in ('articles', 'sections'):
                    try:
                        triples += get_body_values(chapters[old_chapter]['articles'], title, reg_ids, new_chapter)
                    except:
                        if part == 'changes':
                            triples += get_body_values(chapters[old_chapter], title, reg_ids, new_chapter)
                        else:
                            triples += get_body_values(chapters['articles'], title, reg_ids, new_chapter)
            except Exception:
                print()
                print(part, chapter, chapters.keys())
                print(part, next_part.keys())
                print(next_part[part], chapters[old_chapter].keys())
                Exception.with_traceback()
            if prev is not None:
                triples.append({'source': data + new_chapter, 'rel_name': schema + 'isPartOf', 'target': data + prev})
                triples.append({'source': data + prev, 'rel_name': schema + 'hasPart', 'target': data + new_chapter})
            else:
                triples.append(
                    {'source': data + new_chapter, 'rel_name': schema + 'isContentOf', 'target': data + reg_ids})
                triples.append(
                    {'source': data + reg_ids, 'rel_name': schema + 'hasContent', 'target': data + new_chapter})
            if part == 'changes':
                continue
            triples += [
                {'source': data + new_chapter, 'rel_name': types, 'target': schema + part[:-1].title()},
                {'source': data + new_chapter, 'rel_name': schema + 'name',
                 'target': '"' + chapters[old_chapter]['name'] + '"^^xsd:string'},
                {'source': data + new_chapter, 'rel_name': label,
                 'target': '"' + containsMap[part[:-1].title()] + ' ' +
                           re.search(r'_([^_]+$)', re.sub('_Modified_By.+', '', new_chapter)).group(
                               1) + ' : ' + chapters[old_chapter]['name'] + '"^^xsd:string'},
                {'source': data + new_chapter, 'rel_name': types, 'target': LegalDocumentContent},
                {'source': data + new_chapter, 'rel_name': types, 'target': 'owl:Thing'},
            ]
            if title.keys().__contains__('change'):
                if not re.search(r'(SECTION|ARTICLE)_\d+[A-Z]+', new_chapter) and new_chapter != 'dihapus':
                    triples.append({'source': data + new_chapter,
                                    'rel_name': schema + 'modify' + str(title['change_num']),
                                    'target': data + new_chapter.replace(reg_id, title['change'])})
                    triples.append({'source': data + new_chapter.replace(reg_id, title['change']),
                                    'rel_name': schema + 'modifiedTo',
                                    'target': data + new_chapter})
                    triples.append({'source': data + new_chapter.replace(reg_id, title['change']),
                                    'rel_name': types,
                                    'target': LegalDocumentContent})
                elif re.search(r'(SECTION|ARTICLE)_\d+[A-Z]+', new_chapter) and new_chapter != 'dihapus':
                    triples.append({'source': data + new_chapter,
                                    'rel_name': schema + 'delete' + str(title['change_num']),
                                    'target': data + new_chapter.replace(reg_id, title['change'])})
                else:
                    triples.append({'source': data + new_chapter,
                                    'rel_name': schema + 'addedTo' + str(title['change_num']),
                                    'target': data + title['change']})
                    triples.append({'source': data + title['change'],
                                    'rel_name': schema + 'added' + str(title['change_num']),
                                    'target': data + new_chapter})
        except AttributeError:
            triples += get_body_values(chapters[old_chapter], title, reg_ids, prev)
    return triples


def normalise_segment_id(legal_id, segment_tables, column=None):
    if isinstance(segment_tables, str):
        segment_tables = re.sub(r'^([a-zA-Z0-9]+_)+\d+_\d+_', legal_id + '_', segment_tables)
        segment_tables = re.sub(r'ARTICLE', 'Article', segment_tables)
        segment_tables = re.sub(r'SECTION', 'Section', segment_tables)
        segment_tables = re.sub(r'CHAPTER', 'Chapter', segment_tables)
        segment_tables = re.sub(r'PART', 'Part', segment_tables)
        segment_tables = re.sub(r'PARAGRAPH', 'Paragraph', segment_tables)
        if re.search(r'(Article|Section)_LX\d', segment_tables):
            segment_tables = re.sub('_(?=[A-Z]$)|_(?=[A-Z][A-Z]|$)', '_Letter_', segment_tables)
            segment_tables = re.sub(r'LX', '', segment_tables)
            segment_tables = re.sub(r'X', '_', segment_tables)
        if re.search(r'_V\d$', segment_tables):
            segment_tables = re.sub(r'_V(?=\d)', '_Number_', segment_tables)
        return segment_tables
    else:
        segment_tables.loc[segment_tables[column].isnull(), column] = ''
        segment_tables[column] = segment_tables[column].replace(to_replace=r'^([a-zA-Z0-9]+_)+\d+_\d+_',
                                                                value=legal_id + '_', regex=True)
        segment_tables[column] = segment_tables[column].replace(to_replace=r'ARTICLE', value='Article', regex=True)
        segment_tables[column] = segment_tables[column].replace(to_replace=r'SECTION', value='Section', regex=True)
        segment_tables[column] = segment_tables[column].replace(to_replace=r'CHAPTER', value='Chapter', regex=True)
        segment_tables[column] = segment_tables[column].replace(to_replace=r'PART', value='Part', regex=True)
        segment_tables[column] = segment_tables[column].replace(to_replace=r'PARAGRAPH', value='Paragraph', regex=True)
        if len(segment_tables[segment_tables[column].str.contains(r'_LX\d', regex=True)]) > 0:
            segment_tables.loc[(segment_tables[column].str.contains(r'_LX\d', regex=True)),
                               column] = segment_tables.loc[
                segment_tables[column].str.contains(r'_LX\d', regex=True), column].replace(
                to_replace='X(?=[A-Z]$)|X(?=[A-Z][A-Z]|$)', value='_Letter_', regex=True)
            segment_tables[column] = segment_tables[column].replace(to_replace=r'LX(?=\d)', value='', regex=True)
            segment_tables[column] = segment_tables[column].replace(to_replace=r'X(?=\d)', value='_', regex=True)
        segment_tables[column] = segment_tables[column].replace(to_replace=r'(?<=\d_)V(?=\d)', value='Number_',
                                                                regex=True)
        segment_tables.loc[segment_tables[column] == '', column] = None


def get_enclosed_content(part, existed, legal_id_funct):
    try:
        old_tail = re.search(r'([A-Z\d]{2})$', part).group(1)
        if old_tail[-1] == 'A':
            new_tail = old_tail[:-1]
        else:
            new_tail = re.sub('.$', chr(ord(old_tail[-1]) - 1), old_tail)
        new_part = re.sub(old_tail + '$', new_tail, part)
        enclosed_part = existed[existed['parts_existed'] == new_part]
        if len(enclosed_part) > 0:
            enclosed_part = enclosed_part['partOf_existed'].values[0]
            if enclosed_part is None or str(enclosed_part) == 'nan':
                enclosed_part = legal_id_funct
        else:
            enclosed_part = get_enclosed_content(new_part, existed, legal_id_funct)
        return enclosed_part
    except AttributeError:
        return legal_id_funct


def get_amend_body_values(amendment_segment, amended_segment, amendment_id, amended_id, values):
    try:
        next_part = {'changes': 'chapters', 'chapters': 'parts', 'parts': 'paragraphs', 'paragraphs': 'articles',
                     'articles': 'sections'}
        containsMap = {'Chapter': 'BAB', 'Part': 'BAGIAN', 'Paragraph': 'Paragraf', 'Article': 'Pasal',
                       'Section': 'ayat', 'Item': 'item'}
        triple = []
        modification_id = 1
        addition_id = 1
        modification_suffix = '_Modified_By_' + amendment_id
        normalise_segment_id(amended_id, amended_segment, 'parts_existed')
        normalise_segment_id(amended_id, amended_segment, 'partOf_existed')
        article_I = amendment_id + '_Article_I'
        partof_change_triple = []
        delete_values = None
        if len(values) > 0:
            values = pandas.DataFrame(values)
            deletes_values = values[values['value'].str.contains(r'(?i)^dihapus\W*$', regex=True)]
            deletes_values.loc[deletes_values['id'].str.contains(r'LX\d+[A-Z]*X[A-Z]+$', regex=True),
                               'id'] = 'Article_' + \
                                       deletes_values.loc[deletes_values['id'].str.contains(r'^LX\d+[A-Z]*X[A-Z]+$',
                                                                                            regex=True),
                                                          'id']
            deletes_values.loc[deletes_values['id'].str.contains(r'LX\d+[A-Z]*X\d+[A-Z]*X[A-Z]+$', regex=True),
                               'id'] = 'Section_' + \
                                       deletes_values.loc[
                                           deletes_values['id'].str.contains(r'^LX\d+[A-Z]*\d+[A-Z]*X[A-Z]+$',
                                                                             regex=True),
                                           'id']
            deletes_values['id'] = amended_id + '_' + deletes_values['id']
            normalise_segment_id(amended_id, deletes_values, 'id')
            for value in deletes_values.iterrows():
                print(value[1]['id'])
                if re.search('Letter|Number', value[1]['id']):
                    itemType = 'Item'
                else:
                    itemType = re.search('Article|Section|Paragraph|Chapter|Part', value[1]['id']).group(0)
                triple.append(
                    {'source': data + article_I, 'rel_name': schema + 'deletes', 'target': data + value[1]['id']})
                triple.append(
                    {'source': data + value[1]['id'], 'rel_name': schema + 'deletedBy', 'target': data + article_I})
                triple.append({'source': data + value[1]['id'], 'rel_name': label,
                               'target': '"' + containsMap[itemType] + ' ' + re.search(r'_([^_]+$)',
                                                                                       value[1]['id']).group(1) +
                                         '"^^xsd:string'})
        for change in amendment_segment:
            for item in amendment_segment[change]:
                oldItem = item
                item = normalise_segment_id(amended_id, item)
                if (re.search(r'_\d+$', item) is not None) or len(
                        amended_segment[amended_segment['parts_existed'] == item]) > 0:
                    modification = data + 'Modification_' + str(modification_id) + '_By_' + amendment_id
                    triple.append({'source': data + article_I, 'rel_name': schema + 'modifies', 'target': modification})
                    triple.append(
                        {'source': modification, 'rel_name': schema + 'hasModificationTarget', 'target': data + item})
                    triple.append({'source': modification, 'rel_name': schema + 'hasModificationContent',
                                   'target': data + item + modification_suffix})
                    triple.append({'source': modification, 'rel_name': types, 'target': LawModification})
                    triple.append({'source': modification, 'rel_name': types, 'target': LawAmendment})
                    triple.append({'source': modification, 'rel_name': types, 'target': Things})
                    triple.append({'source': modification, 'rel_name': label,
                                   'target': '"Modification ' + str(modification_id) + '"^^xsd:string'})

                    if re.search('Letter|Number', item):
                        itemType = 'Item'
                    else:
                        itemType = re.search('Article|Section|Paragraph|Chapter|Part', item).group(0)
                    triple.append({'source': data + item, 'rel_name': types, 'target': schema + itemType})
                    triple.append({'source': data + item, 'rel_name': types, 'target': LegalDocumentContent})
                    triple.append({'source': data + item, 'rel_name': types, 'target': Things})
                    triple.append({'source': data + item, 'rel_name': label,
                                   'target': '"' + containsMap[itemType] + ' ' + re.search(r'_([^_]+$)', item).group(
                                       1) + '"^^xsd:string'})
                    triple.append(
                        {'source': data + item + modification_suffix, 'rel_name': types, 'target': schema + itemType})
                    triple.append(
                        {'source': data + item + modification_suffix, 'rel_name': types,
                         'target': LegalDocumentContent})
                    triple.append({'source': data + item + modification_suffix, 'rel_name': types, 'target': Things})
                    triple.append({'source': data + item + modification_suffix, 'rel_name': schema + 'name',
                                   'target': '"' + amendment_segment[change][oldItem]['name'] + '"^^xsd:string'})
                    triple.append({'source': data + item + modification_suffix, 'rel_name': label,
                                   'target': '"' + containsMap[itemType] + ' ' + re.search(r'_([^_]+$)', item).group(
                                       1) + '"^^xsd:string'})
                    body_triple = get_body_values(amendment_segment[change][oldItem][next_part[itemType.lower() + 's']],
                                                  {},
                                                  amended_id, item + modification_suffix, modification_suffix)
                    partof_change_triple += body_triple
                    modification_id += 1
                else:
                    addition = data + 'Addition_' + str(addition_id) + '_By_' + amended_id
                    enclosed_part = get_enclosed_content(item, amended_segment, amended_id)
                    triple.append({'source': data + article_I, 'rel_name': schema + 'adds', 'target': addition})
                    triple.append(
                        {'source': addition, 'rel_name': schema + 'hasAdditionTarget', 'target': data + enclosed_part})
                    triple.append(
                        {'source': addition, 'rel_name': schema + 'hasAdditionContent', 'target': data + item})
                    triple.append({'source': addition, 'rel_name': types, 'target': LawAddition})
                    triple.append({'source': addition, 'rel_name': types, 'target': LawAmendment})
                    triple.append({'source': addition, 'rel_name': types, 'target': Things})
                    triple.append({'source': addition, 'rel_name': label,
                                   'target': '"Modification ' + str(addition_id) + '"^^xsd:string'})
                    itemType = re.search('Article|Section|Paragraph|Chapter|Part|Item', item).group(0)
                    triple.append({'source': data + item, 'rel_name': types, 'target': schema + itemType})
                    triple.append({'source': data + item, 'rel_name': types, 'target': LegalDocumentContent})
                    triple.append({'source': data + item, 'rel_name': types, 'target': Things})
                    triple.append({'source': data + item, 'rel_name': schema + 'name',
                                   'target': '"' + amendment_segment[change][oldItem]['name'] + '"^^xsd:string'})
                    triple.append({'source': data + item, 'rel_name': label,
                                   'target': '"' + containsMap[itemType] + ' ' + re.search(r'_([^_]+$)', item).group(
                                       1) + '"^^xsd:string'})
                    body_triple = []
                    if len(amendment_segment[change][oldItem][next_part[itemType.lower() + 's']]) > 0:
                        body_triple = get_body_values(
                            amendment_segment[change][oldItem][next_part[itemType.lower() + 's']],
                            {}, amended_id, item)
                    elif itemType in ['Chapter', 'Part']:
                        body_triple = get_body_values(
                            amendment_segment[change][oldItem]['articles'],
                            {}, amended_id, item)
                    if len(body_triple) > 0:
                        partof_change_triple += body_triple
                    addition_id += 1
        triple.append({'source': data + article_I, 'rel_name': types, 'target': Article})
        triple.append({'source': data + article_I, 'rel_name': types, 'target': LegalDocumentContent})
        triple.append({'source': data + article_I, 'rel_name': types, 'target': Things})
        triple.append({'source': data + article_I, 'rel_name': label, 'target': '"Pasal I"^^xsd:string'})
        triple.append({'source': data + article_I, 'rel_name': schema + 'name', 'target': '"Pasal I"^^xsd:string'})
        triple.append({'source': data + article_I, 'rel_name': schema + 'isContentOf', 'target': data + amendment_id})
        triple.append({'source': data + amendment_id, 'rel_name': schema + 'hasContent', 'target': data + article_I})
        triple = pandas.DataFrame(triple)
        partof_change_triple = pandas.DataFrame(partof_change_triple)
        if delete_values is not None:
            deletes_values['id'] = data + deletes_values['id'] + modification_suffix
            if len(deletes_values) > 0:
                partof_change_triple = partof_change_triple[
                    ~((partof_change_triple['source'].isin(deletes_values['id'])) |
                      (partof_change_triple['target'].isin(deletes_values['id'])))]
        else:
            deletes_values = None
        triple = pandas.concat([triple, partof_change_triple], ignore_index=True)
        modifiedContent = triple[(triple['rel_name'] == types) & (triple['target'] == LegalDocumentContent) &
                                 (triple['source'].str.contains(modification_suffix))]
        modifiedContent['target'] = modifiedContent['source']
        modifiedContent['source'] = modifiedContent['source'].replace(to_replace=modification_suffix, value='',
                                                                      regex=True)
        modifiedContent = modifiedContent[['source', 'target']]
        return triple, modifiedContent, deletes_values
    except KeyError:
        KeyError.with_traceback()
        return None, None, None


def error_handling(left, right, key_left, key_right):
    new_key_left = [x + '_left' for x in key_left + ['sentence_id']]
    new_key_right = [x + '_right' for x in key_right + ['sentence_id']]
    merges = left.add_suffix('_left').merge(right.add_suffix('_right'), how='outer', left_on=new_key_left,
                                            right_on=new_key_right)
    merges = merges.drop_duplicates()[new_key_left + new_key_right]
    merges['dup'] = merges.groupby(new_key_left + new_key_right[:-1])[new_key_right[-1]].transform('count')
    merges['test'] = ~merges['sentence_id_left'].isnull() & (merges['dup'] > 1)
    drop_articles = merges[merges['sentence_id_left'].isnull() |
                           ((~merges['sentence_id_left'].isnull()) & (merges['dup'] > 1))]['sentence_id_right'].unique()
    return drop_articles


def compound_restructure(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    filtered_nodes = g_nodes[~(g_nodes['text'].isin({'Dalam hal', 'dalam hal', 'Pada saat', 'pada saat', 'apabila'}) |
                               ((g_nodes['type'] == 'num') & g_nodes['text'].str.contains(r'^\d+$', regex=True)))]
    compounds = g_edges[g_edges['rel_name'].isin(['compound', 'flat', 'mark', 'det', 'nummod']) &
                        (g_edges['state'] == 1)]
    compounds = compounds.merge(filtered_nodes.add_suffix('_source'), left_on=['source'], right_on=['id_source'])
    compounds = compounds.merge(filtered_nodes.add_suffix('_target'), left_on=['target'], right_on=['id_target'])
    compounds = compounds[(compounds['rel_name'] != 'nummod') | ((compounds['rel_name'] == 'nummod') &
                                                                 (compounds['type_source'] == compounds[
                                                                     'type_target']) &
                                                                 (compounds['type_source'] == 'num'))]
    compounds = compounds[['sentence_id', 'id', 'state', 'rel_name', 'source', 'target', 'text_source', 'text_target',
                           'min_source', 'max_source', 'type_source', 'type_target', 'min_target', 'max_target']]
    compounds['diff1'] = compounds['min_source'] - compounds['max_target']
    compounds['diff2'] = compounds['min_target'] - compounds['max_source']
    compounds['source_rank'] = compounds.groupby(['source'])['target'].transform('rank')
    compounds = compounds[(compounds['diff1'] == 1) | (compounds['diff2'] == 1)]
    compounds = compounds[~compounds['source'].isin(compounds.target.array) &
                          (compounds['source_rank'] == 1)]
    while len(compounds) > 0:
        compounds['new_type'] = 'phrase'
        compounds.loc[(compounds['type_source'] == 'verb') | (compounds['type_target'] == 'verb'),
                      'new_type'] = 'verb'
        compounds.loc[compounds['type_source'] == compounds['type_target'], 'new_type'] = compounds['type_source']
        compounds['new_min'] = compounds[['min_source', 'min_target']].min(axis=1)
        compounds['new_max'] = compounds[['max_source', 'max_target']].max(axis=1)
        compounds['new_text'] = ''
        compounds.loc[compounds['diff2'] == 1,
                      'new_text'] = compounds.loc[compounds['diff2'] == 1,
                                                  'text_source'].values + ' ' + compounds.loc[compounds['diff2'] == 1,
                                                                                              'text_target']
        compounds.loc[compounds['diff1'] == 1,
                      'new_text'] = compounds.loc[compounds['diff1'] == 1,
                                                  'text_target'].values + ' ' + compounds.loc[compounds['diff1'] == 1,
                                                                                              'text_source']

        target_out = compounds.merge(g_edges, left_on=['target', 'state'],
                                     right_on=['source', 'state'], suffixes=['', '_target_out'])
        target_out = target_out.sort_values(['id_target_out'])
        g_edges = g_edges.sort_values(['id'])
        g_edges.loc[g_edges['id'].isin(target_out.id_target_out.array),
                    ['source']] = target_out.loc[target_out['id_target_out'].isin(target_out.id_target_out.array),
                                                 ['source']].values
        compounds = compounds.sort_values(['target'])
        g_nodes = g_nodes.sort_values(['id'])
        g_nodes.loc[g_nodes['id'].isin(compounds.source.array),
                    ['type', 'min', 'max', 'text']] = compounds.loc[compounds['source'].isin(compounds.source.array),
                                                                    ['new_type', 'new_min', 'new_max',
                                                                     'new_text']].values
        g_edges.loc[g_edges['id'].isin(compounds.id.array), 'state'] = 0
        g_nodes.loc[g_nodes['id'].isin(compounds.target.array), 'state'] = 0
        filtered_nodes = g_nodes[~(g_nodes['text'].isin({'dalam hal', 'Pada saat', 'pada saat', 'apabila'}) |
                                   ((g_nodes['type'] == 'num') & g_nodes['text'].str.contains(r'^\d+$', regex=True)))]
        compounds = g_edges[
            g_edges['rel_name'].isin(['compound', 'flat', 'mark', 'det', 'nummod']) & (g_edges['state'] == 1)]
        compounds = compounds.merge(filtered_nodes.add_suffix('_source'), left_on=['source'], right_on=['id_source'])
        compounds = compounds.merge(filtered_nodes.add_suffix('_target'), left_on=['target'], right_on=['id_target'])
        compounds = compounds[(compounds['rel_name'] != 'nummod') |
                              ((compounds['rel_name'] == 'nummod') &
                               (compounds['type_source'] == compounds['type_target']) &
                               (compounds['type_source'] == 'num'))]
        compounds = compounds[['sentence_id', 'id', 'state', 'rel_name', 'source', 'target', 'text_source',
                               'text_target', 'min_source', 'max_source', 'type_source', 'type_target', 'min_target',
                               'max_target']]
        compounds['diff1'] = compounds['min_source'] - compounds['max_target']
        compounds['diff2'] = compounds['min_target'] - compounds['max_source']
        compounds['source_rank'] = compounds.groupby(['source'])['target'].transform('rank')
        compounds = compounds[(compounds['diff1'] == 1) | (compounds['diff2'] == 1)]
        compounds = compounds[~compounds['source'].isin(compounds.target.array) &
                              (compounds['source_rank'] == 1)]
    g_edges.loc[g_edges['rel_name'] == 'punct', 'state'] = 0
    return g_nodes, g_edges


def special_merge(graph, words, word_type):
    g_nodes = graph['V']
    g_edges = graph['E']
    merge = None
    new_text = ''
    id_cols = []
    for idx in range(len(words)):
        word_nodes = g_nodes[g_nodes['text'].str.contains('(?i)^{}$'.format(words[idx])) & (g_nodes['state'] == 1)]
        new_text += ' ' + words[idx]
        id_cols.append('id_{}'.format(str(idx)))
        if merge is None:
            merge = word_nodes.add_suffix('_{}'.format(str(idx)))
        else:
            left_on = ['sentence_id_{}'.format(str(idx - 1)), 'state_{}'.format(str(idx - 1))]
            right_on = ['sentence_id_{}'.format(str(idx)), 'state_{}'.format(str(idx))]
            merge = merge.merge(word_nodes.add_suffix('_{}'.format(str(idx))), left_on=left_on, right_on=right_on,
                                suffixes=['', ''])
            merge = merge[(merge['max_{}'.format(str(idx - 1))] == merge['min_{}'.format(str(idx))] - 1)]
    merge['new_text'] = new_text.strip()
    merge['min_id'] = merge[id_cols].min(axis=1)
    merge['max_id'] = merge[id_cols].max(axis=1)
    merge['new_max'] = merge['max_0'] + len(words) - 1
    merge = merge[['sentence_id_0', 'id_0', 'min_0', 'state_0', 'new_text', 'min_id', 'max_id', 'new_max']]
    merge['sentence_id'] = merge['sentence_id_0']
    merge_out = merge.merge(g_edges, left_on=['sentence_id_0', 'state_0'], right_on=['sentence_id', 'state'],
                            suffixes=['', 'e_out'])
    merge_out = merge_out[(merge_out['source'] >= merge_out['min_id']) & (merge_out['source'] <= merge_out['max_id'])]
    merge_out[['new_source', 'new_state']] = merge_out[['id_0', 'state']]
    merge_in = merge.merge(g_edges, left_on=['sentence_id_0', 'state_0'], right_on=['sentence_id', 'state'],
                           suffixes=['', 'e_in'])
    merge_in = merge_in[(merge_in['target'] >= merge_in['min_id']) & (merge_in['target'] <= merge_in['max_id']) &
                        ((merge_in['source'] < merge_in['min_id']) | (merge_in['source'] > merge_in['max_id']))]
    merge_in[['new_target', 'new_state']] = merge_in[['id_0', 'state']]
    compounds = merge_out[merge_out['rel_name'].isin(['compound', 'flat', 'det'])].copy()
    compounds['rank'] = compounds.groupby(['id_0'])['id'].transform('rank')
    compounds = compounds[compounds['rank'] == 1][['id_0', 'id', 'target']]
    merge_in = merge_in.merge(compounds, how='left', left_on='id_0', right_on='id_0', suffixes=['', '_compound'])
    merge_in.loc[(~merge_in['target_compound'].isna()), 'new_target'] = merge_in['target_compound']
    merge_out = merge_out.merge(compounds, how='left', left_on='id_0', right_on='id_0', suffixes=['', '_compound'])
    merge_out.loc[(~merge_out['target_compound'].isna()) & (merge_out['rel_name'] != 'cc'),
                  'new_source'] = merge_out['target_compound']
    merge_out.loc[merge_out['id'].isin(compounds.id.array), 'new_state'] = 0
    merge_in['new_rel_name'] = merge_in['rel_name']
    if new_text.strip() == 'sesuai dengan':
        sesuai_dengan_non_case_in = merge_in[(merge_in['new_rel_name'] != 'case') &
                                             (merge_in['new_text'] == 'sesuai dengan')].copy()
        sesuai_dengan_non_case_in['rank'] = sesuai_dengan_non_case_in.groupby(['id_0'])['id'].transform('rank')
        sesuai_dengan_non_case_in = sesuai_dengan_non_case_in[sesuai_dengan_non_case_in['rank'] == 1][['id_0',
                                                                                                       'source']]
        merge_out = merge_out.merge(sesuai_dengan_non_case_in, how='left', left_on='id_0', right_on='id_0',
                                    suffixes=['', '_non_case'])
        merge_out.loc[(~merge_out['source_non_case'].isna()), 'new_source'] = merge_out['source_non_case']
        merge_in.loc[(merge_in['new_rel_name'] != 'case') & (merge_in['new_text'] == 'sesuai dengan'), 'new_state'] = 0
    merge_out['new_rel_name'] = merge_out['rel_name']
    if word_type == 'case':
        merge_in.loc[merge_in['new_target'] == merge_in['id_0'], 'new_rel_name'] = 'case'
        merge_out.loc[merge_out['new_source'] == merge_out['id_0'], 'new_rel_name'] = 'case'
    merge_out = merge_out[['sentence_id', 'id', 'new_source', 'new_rel_name', 'new_state']].sort_values(['id'])
    merge_in = merge_in[['sentence_id', 'id', 'new_target', 'new_rel_name', 'new_state']].sort_values(['id'])
    g_edges = g_edges.sort_values(['id'])
    try:
        g_edges.loc[g_edges['id'].isin(merge_out.id.array),
                    ['source', 'rel_name', 'state']] = merge_out.loc[merge_out['id'].isin(merge_out.id.array),
                                                                     ['new_source', 'new_rel_name', 'new_state']].values
    except ValueError:
        drop_article = error_handling(g_edges.loc[g_edges['id'].isin(merge_out.id.array)],
                                      merge_out.loc[merge_out['id'].isin(merge_out.id.array)],
                                      ['id'], ['id'])
        merge_out = merge_out[~merge_out['sentence_id'].isin(drop_article)].drop_duplicates()
        merge_out = merge_out.drop_duplicates()
        merge_in = merge_in[~merge_in['sentence_id'].isin(drop_article)].drop_duplicates()
        merge_in = merge_in.drop_duplicates()
        merge = merge[~merge['sentence_id_0'].isin(drop_article)].drop_duplicates()
        merge = merge.drop_duplicates()
        g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
        g_edges.loc[g_edges['id'].isin(merge_out.id.array),
                    ['source', 'rel_name', 'state']] = merge_out.loc[merge_out['id'].isin(merge_out.id.array),
                                                                     ['new_source', 'new_rel_name', 'new_state']].values

    g_edges.loc[g_edges['id'].isin(merge_in.id.array),
                ['target', 'rel_name', 'state']] = merge_in.loc[merge_in['id'].isin(merge_in.id.array),
                                                                ['new_target', 'new_rel_name', 'new_state']].values
    merge = merge.sort_values('id_0')
    g_nodes = g_nodes.sort_values(['id'])
    try:
        g_nodes.loc[g_nodes['id'].isin(merge.id_0.array),
                    ['text', 'max']] = merge.loc[merge['id_0'].isin(merge.id_0.array), ['new_text', 'new_max']].values
    except ValueError:
        drop_article = error_handling(g_nodes.loc[g_nodes['id'].isin(merge.id_0.array)],
                                      merge.loc[merge['id_0'].isin(merge.id_0.array)],
                                      ['id'], ['id_0'])
        merge = merge[~merge['sentence_id_0'].isin(drop_article)].drop_duplicates()
        merge = merge.drop_duplicates()
        g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
        g_nodes.loc[g_nodes['id'].isin(merge.id_0.array),
                    ['text', 'max']] = merge.loc[merge['id_0'].isin(merge.id_0.array), ['new_text', 'new_max']].values
    excl_v = g_nodes.merge(merge, left_on='sentence_id', right_on='sentence_id_0')
    excl_v = excl_v[(excl_v['id'] > excl_v['min_id']) & (excl_v['id'] <= excl_v['max_id'])]
    g_nodes.loc[g_nodes['id'].isin(excl_v.id.array), 'state'] = 0
    inter_e = g_edges.merge(merge, left_on='sentence_id', right_on='sentence_id_0')
    inter_e = inter_e[(inter_e['source'] >= inter_e['min_id']) & (inter_e['source'] <= inter_e['max_id']) &
                      (inter_e['target'] >= inter_e['min_id']) & (inter_e['target'] <= inter_e['max_id'])]
    inter_e.sort_values(['id'])
    g_edges.loc[g_edges['id'].isin(inter_e.id.array), 'state'] = 0
    g_edges['rank'] = g_edges.groupby(['target', 'state'])['id'].transform('rank')
    g_edges.loc[(g_edges['rank'] != 1) & g_edges['target'].isin(merge['id_0'].array), 'state'] = 0
    graph['V'] = g_nodes
    graph['E'] = g_edges
    return False, graph


def clear_conj(g_nodes, g_edges):
    conjs = g_edges[(g_edges['rel_name'] == 'cc') & (g_edges['state'] == 1)]
    conj = g_edges[(g_edges['rel_name'] == 'conj') & (g_edges['state'] == 1)]
    conj = conj.merge(g_nodes.add_suffix('_source'), left_on='source', right_on='id_source')
    conj = conj.merge(g_nodes.add_suffix('_target'), left_on='target', right_on='id_target')
    conj_root = conjs.merge(conj.add_suffix('_root'), left_on=['source', 'state'],
                            right_on=['target_root', 'state_root'])
    conj_root = conj_root[['sentence_id_root', 'id_root', 'source_root', 'target_root', 'rel_name_root', 'state_root',
                           'is_root_sentence_root', 'text_source_root', 'type_source_root', 'text_target_root',
                           'type_target_root', 'min_source_root', 'max_source_root', 'min_target_root',
                           'max_target_root']]
    conj_root['test'] = (conj_root['text_source_root'].str.contains(r'RX', regex=True) &
                         (~conj_root['text_target_root'].str.contains(r'RX', regex=True))) | \
                        (conj_root['text_target_root'].str.contains(r'RX', regex=True) &
                         (~conj_root['text_source_root'].str.contains(r'RX', regex=True))) | \
                        ((conj_root['type_target_root'] == 'verb') & (~(conj_root['type_source_root'] == 'verb'))) | \
                        ((conj_root['type_source_root'] == 'verb') & (~(conj_root['type_target_root'] == 'verb')))
    conj_root = conj_root[(conj_root['text_source_root'].str.contains(r'RX', regex=True) &
                           (~conj_root['text_target_root'].str.contains(r'RX', regex=True))) |
                          (conj_root['text_target_root'].str.contains(r'RX', regex=True) &
                           (~conj_root['text_source_root'].str.contains(r'RX', regex=True))) |
                          ((conj_root['type_target_root'] == 'verb') & (~(conj_root['type_source_root'] == 'verb'))) |
                          ((conj_root['type_source_root'] == 'verb') & (~(conj_root['type_target_root'] == 'verb')))]
    conj_root = conj_root.merge(conj, left_on='source_root', right_on='source')
    conj_root = conj_root[(conj_root['text_target_root'].str.contains(r'RX', regex=True) &
                           (conj_root['text_target'].str.contains(r'RX', regex=True))) |
                          ((~(conj_root['text_target_root'].str.contains(r'RX', regex=True) |
                              (conj_root['type_target_root'] == 'verb'))) &
                           (~(conj_root['text_target'].str.contains(r'RX', regex=True) |
                              (conj_root['type_target'] == 'verb')))) |
                          ((conj_root['type_target_root'] == 'verb') & (conj_root['type_target'] == 'verb'))]
    conj_root['rank'] = conj_root.groupby(['source_root'])['min_target'].rank(method='first')
    new_root = conj_root[conj_root['rank'] == 1][['sentence_id', 'id', 'source', 'target', 'text_source', 'text_target',
                                                  'type_source', 'type_target']]
    new_root['new_rel_name'] = 'nsubj'
    new_conj = conj_root[conj_root['rank'] != 1][['id', 'source', 'target', 'text_source', 'text_target', 'type_source',
                                                  'type_target']]
    root = g_edges[(g_edges['rel_name'] == 'root') & (g_edges['state'] == 1)]
    new_conj = new_conj.merge(new_root.add_suffix('_new'), left_on='source',
                              right_on='source_new').drop_duplicates().sort_values(['id'])
    new_root = new_root.merge(root.add_suffix('_root'), left_on='sentence_id',
                              right_on='sentence_id_root').sort_values(['id'])
    g_edges.loc[g_edges['id'].isin(new_root.id.array),
                ['source', 'rel_name']] = new_root.loc[new_root['id'].isin(new_root.id.array),
                                                       ['target_root', 'new_rel_name']].values
    g_edges.loc[g_edges['id'].isin(new_conj.id.array),
                ['source']] = new_conj.loc[new_conj['id'].isin(new_conj.id.array), ['target_new']].values
    false_refers = g_nodes[(g_nodes['text'] == 'refer_to') & (g_nodes['state'] == 1)]
    false_refers = false_refers.merge(g_edges.add_suffix('_source'), left_on=['id', 'state'],
                                      right_on=['target_source', 'state_source'])
    false_refers = false_refers[false_refers['rel_name_source'] == 'root']
    false_refers = false_refers.merge(g_edges.add_suffix('_target'), left_on=['id', 'state'],
                                      right_on=['source_target', 'state_target'])
    false_refers = false_refers[false_refers['rel_name_target'].isin(['advcl', 'xcomp', 'parataxis'])]
    false_refers = false_refers.merge(g_nodes.add_suffix('_vtarget'), left_on='target_target', right_on='id_vtarget')
    false_refers = false_refers.merge(g_edges.add_suffix('_tout'), left_on=['target_target', 'state'],
                                      right_on=['source_tout', 'state_tout'])
    false_refers = false_refers[(false_refers['type_vtarget'].isin(['act', 'noun']) &
                                 false_refers['rel_name_tout'].str.contains('has_act_type', regex=True)) |
                                ((false_refers['type_vtarget'] == 'verb') &
                                 false_refers['rel_name_tout'].str.contains('nsubj', regex=True))]
    false_refers = false_refers[['id', 'id_source', 'id_target', 'source_source', 'target_target']].copy()
    false_refers['rank'] = false_refers.groupby(['id'])['target_target'].rank('first')
    g_edges = g_edges.sort_values(['id'])
    false_refers = false_refers[false_refers['rank'] == 1].sort_values('id_source')
    g_edges.loc[g_edges['id'].isin(false_refers.id_source.array),
                'target'] = false_refers.loc[false_refers['id_source'].isin(false_refers.id_source.array),
                                             'target_target'].values
    false_refers = false_refers[false_refers['rank'] == 1].sort_values('id_target')
    g_edges.loc[g_edges['id'].isin(false_refers.id_target.array),
                ['source', 'target']] = false_refers.loc[false_refers['id_target'].isin(false_refers.id_target.array),
                                                         ['target_target', 'id']].values
    return g_nodes, g_edges


def conj_restructure(g_nodes, g_edges):
    conjs = g_edges[(g_edges['rel_name'] == 'cc') & (g_edges['state'] == 1)]
    conj = g_edges[(g_edges['rel_name'] == 'conj') & (g_edges['state'] == 1)]
    conj_root = conjs.merge(conj, left_on=['source', 'state'], right_on=['target', 'state'], suffixes=['_cc', '_root'])
    while len(conj_root) > 0:
        conj_root = conj_root.merge(g_nodes, left_on='target_cc', right_on='id', suffixes=['', '_vcc'])
        conj_root = conj_root.merge(g_nodes, left_on='source_root', right_on='id', suffixes=['', '_vroot'])
        conj_root['tes1'] = conj_root['min'] - conj_root['max_vroot']
        conj_root['tes2'] = conj_root['max'] - conj_root['min_vroot']
        conj_root['diff'] = conj_root[['tes1', 'tes2']].max(axis=1)
        conj_root = conj_root[['sentence_id', 'state', 'id_cc', 'target_cc', 'id_root', 'source_root', 'min_vroot',
                               'diff']]
        conj_root['diff_max'] = conj_root.groupby(['source_root'])['diff'].transform(max)
        exception_root = conj_root[conj_root['diff_max'] != conj_root['diff']]
        filtered_root = conj_root[conj_root['diff_max'] == conj_root['diff']]
        child = conj_root.merge(conj, left_on=['source_root', 'state'],
                                right_on=['source', 'state'], suffixes=['', '_child'])[['sentence_id', 'state', 'id_cc',
                                                                                        'target_cc', 'id_root',
                                                                                        'source_root', 'id', 'target']]
        child = child.rename(columns={'id_cc': 'e_cc', 'id_root': 'e_root', 'id': 'e_child',
                                      'target_cc': 'cc', 'source_root': 'conj_root', 'target': 'child'}, inplace=False)
        child['count_cc'] = child.groupby(['conj_root'])['cc'].transform('nunique')
        child['count_child'] = child.groupby(['conj_root'])['child'].transform('nunique')
        child_as_root = child[child['conj_root'].isin(child['child'].array)]['conj_root'].array
        except_root = child[child['child'].isin(child_as_root)]['conj_root'].array
        child = child[(~child['conj_root'].isin(except_root))]
        child = child.merge(g_nodes, left_on='cc', right_on='id', suffixes=['', '_vcc'])
        child = child.merge(g_nodes, left_on='child', right_on='id', suffixes=['', '_vchild'])
        child['tes1'] = (child['min'] - child['max_vchild'])
        child['tes2'] = (child['min_vchild'] - child['max'])
        child.loc[child['tes1'] < 0, 'tes1'] = numpy.nan
        child.loc[child['tes2'] < 0, 'tes2'] = numpy.nan
        child['min_diff1'] = child.groupby(['e_child'])['tes1'].transform(min)
        child['max_diff1'] = child.groupby(['e_child'])['tes1'].transform(max)
        child['min_diff2'] = child.groupby(['e_child'])['tes2'].transform(min)
        child['x1'] = ((child['tes2'] == child['min_diff2']) & ((child['min_diff2'] == 1) | child['max_diff1'].isna()))
        child['x2'] = ((child['max_diff1'] == child['tes1']) & (child['min_diff2'] != 1) & (child['min_diff1'] > 2))
        child['x3'] = ((child['tes1'] == 1) & (child['min_diff2'] != 1) & (child['min_diff1'] <= 2))
        child = child[((child['tes2'] == child['min_diff2']) &
                       ((child['min_diff2'] == 1) | child['max_diff1'].isna())) |
                      ((child['max_diff1'] == child['tes1']) & (child['min_diff2'] != 1) & (child['min_diff1'] > 2)) |
                      ((child['tes1'] == child['min_diff1']) & (child['min_diff2'] != 1) & (child['min_diff1'] <= 2))]
        child = child[(~((child['count_cc'] == child['count_child']) & child['cc'].isin(exception_root['target_cc'])))]
        if len(filtered_root[filtered_root['source_root'].isin(child.conj_root.array) &
                             filtered_root['target_cc'].isin(child.cc.array)]) == 0:
            g_edges.loc[g_edges['id'].isin(filtered_root.id_cc.array), 'state'] = 0
        filtered_root = filtered_root[filtered_root['source_root'].isin(child.conj_root.array) &
                                      filtered_root['target_cc'].isin(child.cc.array)]
        exception_root = exception_root[exception_root['source_root'].isin(child.conj_root.array) &
                                        exception_root['target_cc'].isin(child.cc.array)]

        g_edges.loc[g_edges['id'].isin(exception_root['id_cc'].array),
                    ['source']] = exception_root.loc[exception_root['id_cc'].isin(exception_root['id_cc'].array),
                                                     ['source_root']].values
        g_edges.loc[g_edges['id'].isin(exception_root['id_cc'].array), 'rel_name'] = 'conj'
        child_out = pandas.concat([child[['cc', 'child', 'min_vchild', 'state']],
                                   filtered_root[['target_cc', 'source_root', 'min_vroot',
                                                  'state']].rename(columns={'target_cc': 'cc',
                                                                            'source_root': 'child',
                                                                            'min_vroot': 'min_child'})])
        child_out['new_min'] = child_out.groupby(['cc'])['min_child'].transform(min)
        child_out = child_out.merge(g_edges, left_on=['child', 'state'],
                                    right_on=['source', 'state'], suffixes=['', '_child'])

        child_out = child_out[['cc', 'child', 'rel_name', 'id', 'target', 'new_min']]
        child_out = child_out.rename(columns={'target': 'root_out', 'id': 'root_eout'}, inplace=False)
        child_out = child_out.merge(g_nodes, left_on='child', right_on='id', suffixes=['', '_root'])
        child_out = child_out.merge(g_nodes, left_on='root_out', right_on='id', suffixes=['', '_target'])
        child_out = child_out[~child_out['rel_name'].isin(['nummod', 'appos', 'det', 'advmod', 'compound', 'flat', 'cc',
                                                           'conj', 'amod', 'nsubj', 'nsubj:pass']) |
                              ((child_out['rel_name'] == 'nsubj') & (child_out['text_target'] == 'yang')) |
                              ((child_out['rel_name'] == 'conj') & child_out['id'].isin(exception_root['id_cc'].array))]
        child_out = child_out[['sentence_id', 'cc', 'child', 'root_out', 'root_eout', 'rel_name', 'min', 'max_target',
                               'new_min', 'text', 'text_target']]
        child_out = child_out[(child_out['max_target'] < child_out['new_min']) | (child_out['rel_name'] == 'conj')]
        child_out = child_out.sort_values(['root_eout'])
        g_edges.loc[g_edges['id'].isin(child_out['root_eout'].array),
                    ['source']] = child_out.loc[child_out['root_eout'].isin(child_out['root_eout'].array),
                                                ['cc']].values
        root_in = filtered_root[['target_cc', 'source_root', 'state']].merge(g_edges, left_on=['source_root', 'state'],
                                                                             right_on=['target', 'state'],
                                                                             suffixes=['', '_root_in'])
        root_in = root_in[['target_cc', 'source_root', 'rel_name', 'id', 'source']]
        root_in = root_in.rename(columns={'target_cc': 'cc', 'source_root': 'conj_root', 'source': 'root_in',
                                          'id': 'root_ein'}, inplace=False)
        root_in = root_in.merge(g_nodes, left_on='conj_root', right_on='id', suffixes=['', '_root'])
        root_in = root_in.merge(g_nodes, left_on='root_in', right_on='id', suffixes=['', '_target'])
        root_in = root_in[['sentence_id', 'cc', 'conj_root', 'root_in', 'root_ein', 'rel_name', 'min', 'max_target',
                           'text', 'text_target']]
        root_in = root_in.sort_values(['root_ein'])
        g_edges.loc[g_edges['id'].isin(root_in['root_ein'].array), ['target']] = root_in.loc[
            root_in['root_ein'].isin(root_in['root_ein'].array), ['cc']].values
        g_edges.loc[g_edges['id'].isin(filtered_root['id_cc'].array),
                    ['source', 'target']] = filtered_root.loc[filtered_root['id_cc'].isin(filtered_root['id_cc'].array),
                                                              ['target_cc', 'source_root']].values
        g_edges.loc[g_edges['id'].isin(filtered_root['id_cc'].array), ['rel_name']] = 'conj'
        g_edges.loc[g_edges['id'].isin(child['e_child'].array),
                    ['source']] = child.loc[child['e_child'].isin(child['e_child'].array), ['cc']].values
        conjs = g_edges[(g_edges['rel_name'] == 'cc') & (g_edges['state'] == 1)]
        conj = g_edges[(g_edges['rel_name'] == 'conj') & (g_edges['state'] == 1)]
        conj_root = conjs.merge(conj, left_on=['source', 'state'], right_on=['target', 'state'],
                                suffixes=['_cc', '_root'])
    g_edges.loc[g_edges['rel_name'] == 'cc', 'state'] = 0

    fake_cc = g_edges[(g_edges['rel_name'] == 'conj') & (g_edges['state'] == 1)]
    fake_cc = fake_cc.merge(g_nodes.add_suffix('_source'), left_on='source', right_on='id_source')
    fake_cc = fake_cc.merge(g_nodes.add_suffix('_target'), left_on='target', right_on='id_target')
    fake_cc_verb = fake_cc[(fake_cc['type_source'] == 'verb') &
                           (fake_cc['type_source'] == fake_cc['type_target'])].sort_values(['id'])
    g_edges = g_edges.sort_values(['id'])
    g_edges.loc[g_edges['id'].isin(fake_cc_verb.id.array), 'rel_name'] = 'advcl'
    fake_cc = fake_cc[(~fake_cc['type_source'].isin(['cconj', 'verb'])) &
                      (fake_cc['type_source'] == fake_cc['type_target'])][['id_source', 'sentence_id',
                                                                           'is_root_sentence']].drop_duplicates()
    fake_cc['id'] = fake_cc['id_source']
    fake_cc = fake_cc.drop(columns=['id_source'])
    new_vid = len(g_nodes)
    new_eid = len(g_edges)
    added_v = []
    added_e = []
    new_cc = []
    for cc in fake_cc.iterrows():
        sentence_id = cc[1]['sentence_id']
        cc_id = cc[1]['id']
        is_root_sentence = cc[1]['is_root_sentence']
        new_v = {'sentence_id': sentence_id, 'id': new_vid, 'text': 'other', 'type': 'cconj', 'min': -1, 'max': -1,
                 'is_root_sentence': is_root_sentence, 'state': 1}
        new_cc.append(new_vid)
        added_v.append(new_v)

        new_e = {'sentence_id': sentence_id, 'id': new_eid, 'source': new_vid, 'target': cc_id,
                 'rel_name': 'conj', 'state': 1, 'is_root_sentence': is_root_sentence}
        new_vid += 1
        new_eid += 1
        added_e.append(new_e)
    g_nodes = g_nodes.append(added_v, ignore_index=True)
    g_edges = g_edges.append(added_e, ignore_index=True)
    new_ecc = g_edges[g_edges['source'].isin(new_cc)]
    old_ecc = g_edges[g_edges['target'].isin(fake_cc['id'].array) & (~g_edges['id'].isin(new_ecc.id.array)) &
                      (g_edges['state'] == 1)]
    old_ecc = old_ecc.merge(new_ecc, left_on='source', right_on='target', suffixes=['_old', '_new'])
    old_ecc = old_ecc.sort_values(['id_old'])
    child = list(g_edges[g_edges['source'].isin(fake_cc.id.array) & (g_edges['rel_name'] == 'conj') &
                         (g_edges['state'] == 1)]['target'].array)
    child = child + list(fake_cc['id'].array)

    child_out = g_edges[g_edges['source'].isin(child) & (g_edges['state'] == 1)]
    child_out = child_out[~child_out['rel_name'].isin(['nummod', 'appos', 'det', 'advmod', 'compound', 'flat', 'cc',
                                                       'amod'])]
    child_out = child_out.rename(columns={'source': 'child'}, inplace=False)
    child_out = child_out.merge(g_nodes, left_on='child', right_on='id', suffixes=['', '_child'])
    child_out = child_out.merge(g_nodes, left_on='target', right_on='id', suffixes=['', '_target'])
    child_out = child_out[['sentence_id', 'id', 'child', 'target', 'rel_name', 'min', 'max_target',
                           'text', 'text_target']]
    child_out = child_out[(child_out['max_target'] < child_out['min']) | (child_out['rel_name'] == 'conj')]
    child_out = child_out.merge(new_ecc, left_on='child', right_on='target', suffixes=['', 'cc'])
    child_out = child_out.sort_values(['id'])
    root_in = g_edges[g_edges['target'].isin(fake_cc.id.array) & (~g_edges['id'].isin(new_ecc.id.array)) &
                      (g_edges['state'] == 1)]
    root_in = root_in.merge(new_ecc, left_on='target', right_on='target', suffixes=['_old', '_new'])
    root_in = root_in.sort_values(['id_old'])
    g_edges = g_edges.sort_values(['id'])

    g_edges.loc[g_edges['id'].isin(child_out['id'].array), ['source']] = child_out.loc[
        child_out['id'].isin(child_out['id'].array), ['source']].values
    g_edges.loc[g_edges['id'].isin(old_ecc['id_old'].array), 'source'] = old_ecc.loc[
        old_ecc['id_old'].isin(old_ecc['id_old'].array), ['source_new']].values
    g_edges.loc[g_edges['id'].isin(root_in['id_old'].array), 'target'] = root_in.loc[
        root_in['id_old'].isin(root_in['id_old'].array), ['source_new']].values
    g_edges['rank'] = g_edges.groupby(['source', 'target', 'rel_name'])['id'].transform('rank')
    g_edges.loc[g_edges['rank'] != 1, 'state'] = 0
    return g_nodes, g_edges


def cases_handler(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    cases = g_edges[(g_edges['rel_name'].isin(['case', 'mark'])) & (g_edges['state'] == 1)].copy()
    handled_cases = []
    root = g_edges[(g_edges['rel_name'] == 'root') & (g_edges['state'] == 1)].target.array
    while len(cases) > 0:
        cases = cases.merge(g_nodes.add_suffix('_vsource'), left_on='source', right_on='id_vsource')
        cases = cases.merge(g_nodes.add_suffix('_vtarget'), left_on='target', right_on='id_vtarget')
        g_edges.loc[g_edges['id'].isin(cases.id.array) & g_edges['source'].isin(root), 'state'] = 0
        cases = cases[~cases['source'].isin(root)]
        cases = cases[~(cases['text_vsource'].str.contains('^(?i)dalam hal$') |
                        cases['text_vtarget'].str.contains('^(?i)dalam hal$'))]
        cases['count'] = cases.groupby('source')['id'].transform('nunique')
        cases['count1'] = cases.groupby('source')['id'].transform('count')
        cases = cases[~cases['source'].isin(cases.target.array)]
        handled_cases += list(cases['id'].array)
        if len(cases) == 0:
            break
        exception_cases = cases[(cases['count'] > 1)]
        filtered_cases = cases[(cases['count'] == 1)]
        source_in = filtered_cases.merge(g_edges, left_on=['source', 'state'], right_on=['target', 'state'],
                                         suffixes=['', '_source_in'])
        source_in = source_in.merge(g_nodes, left_on='source_source_in', right_on='id', suffixes=['', '_source'])
        source_in = source_in.merge(g_nodes, left_on='target_source_in', right_on='id', suffixes=['', '_target'])
        source_in = source_in.sort_values(['id_source_in'])
        g_edges.loc[g_edges.id.isin(source_in.id_source_in.array),
                    'target'] = source_in.loc[source_in['id_source_in'].isin(source_in.id_source_in.array),
                                              'target'].values
        exception_source_in = exception_cases.merge(g_edges, left_on=['source', 'state'], right_on=['target', 'state'],
                                                    suffixes=['', '_source_in'])
        g_edges = g_edges[['sentence_id', 'id', 'source', 'target', 'rel_name', 'state', 'is_root_sentence']]
        new_eid = len(g_edges)
        added_e = []
        for case in exception_source_in.iterrows():
            source = case[1]['source_source_in']
            target = case[1]['target']
            sentence_id = case[1]['sentence_id_source_in']
            rel_name = case[1]['rel_name_source_in']
            rel_state = case[1]['state']
            is_root_sentence = case[1]['is_root_sentence']
            added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': source, 'target': target,
                            'rel_name': rel_name, 'state': rel_state, 'is_root_sentence': is_root_sentence})
            new_eid += 1
        g_edges.loc[g_edges.id.isin(exception_source_in.id_source_in.array), 'state'] = 0
        g_edges = g_edges.append(added_e, ignore_index=True)

        target_out = cases.merge(g_edges, left_on=['target', 'state'], right_on=['source', 'state'],
                                 suffixes=['', '_target_out'])
        target_out['rank'] = target_out.groupby(['id_target_out'])['id'].transform('nunique')
        target_out['new_rel_name'] = target_out['rel_name_target_out']
        target_out.loc[target_out['new_rel_name'].isin(['flat', 'det', 'compound', 'mark', 'amod']),
                       'new_rel_name'] = 'has_subject'
        g_edges.loc[g_edges.id.isin(target_out.id_target_out.array),
                    ['source',
                     'rel_name']] = target_out.loc[target_out['id_target_out'].isin(target_out.id_target_out.array),
                                                   ['target', 'new_rel_name']].values
        g_edges.loc[g_edges.id.isin(cases.id.array),
                    ['source', 'target']] = g_edges.loc[g_edges.id.isin(cases.id.array),
                                                        ['target', 'source']].values
        cases = g_edges[(g_edges['rel_name'].isin(['case', 'mark'])) & (g_edges['state'] == 1)].copy()
        cases = cases[~cases['id'].isin(handled_cases)]
    return g_nodes, g_edges


def nsubj_handler(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    nsubjs = g_edges[g_edges['rel_name'].str.contains('(n|c)subj', regex=True) & (g_edges['state'] == 1)]
    conj = g_edges[(g_edges['rel_name'] == 'conj') & g_edges['target'].isin(nsubjs.source.array) &
                   (g_edges['state'] == 1)].copy()
    nsubjs = nsubjs.merge(g_nodes.add_suffix('_vsource'), left_on='source', right_on='id_vsource')
    nsubjs = nsubjs.merge(g_nodes.add_suffix('_vtarget'), left_on='target', right_on='id_vtarget')
    nsubjs = nsubjs[(~nsubjs['source'].isin(conj.target.array) | (nsubjs['text_vtarget'] == 'yang')) &
                    (nsubjs['type_vsource'] == 'verb')]
    nsubjs['new_rel_name'] = 'has_act_type'
    nsubj_in = nsubjs.merge(g_edges.add_suffix('_in'), left_on=['source', 'state'],
                            right_on=['target_in', 'state_in'])
    nsubj_in['rank'] = nsubj_in.drop_duplicates().groupby(['id_in'])['target'].rank(method='first')
    nsubj_in = nsubj_in[nsubj_in['rank'] == 1][['sentence_id', 'id_in', 'target']].sort_values(['id_in'])
    g_edges.loc[(g_edges['id'].isin(nsubj_in.id_in.array)),
                "target"] = nsubj_in.loc[(nsubj_in['id_in'].isin(nsubj_in.id_in.array)), "target"].values
    nsubjs = nsubjs[['id', 'source', 'target', 'new_rel_name']].drop_duplicates().sort_values(['id'])
    g_edges.loc[(g_edges['id'].isin(nsubjs.id.array)),
                ["source", 'target', 'rel_name']] = nsubjs.loc[(nsubjs['id'].isin(nsubjs.id.array)),
                                                               ['target', 'source', 'new_rel_name']].values
    g_edges.loc[g_edges['rel_name'].str.contains('(n|c)subj'), 'rel_name'] = 'has_subject'
    return g_nodes, g_edges


def range_formatter(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    ranges_conjs = g_nodes[g_nodes['text'] == 'sampai dengan']
    for ranges_conj in ranges_conjs.iterrows():
        conj_min = ranges_conj[1]['min']
        conj_id = ranges_conj[1]['id']
        sentence_id = ranges_conj[1]['sentence_id']
        conj_out = g_edges[(g_edges['source'] == conj_id) & (g_edges['rel_name'] == 'case') &
                           (g_edges['state'] == 1)]['target'].array
        try:
            conj_in = g_edges[(g_edges['target'] == conj_id) & (g_edges['state'] == 1)]['id'].array[0]
        except Exception:
            g_edges.loc[g_edges['sentence_id'] == sentence_id, 'state'] = 0
            continue
        end_refer = g_nodes[(g_nodes['id'].isin(conj_out)) &
                            (g_nodes['text'].str.contains('RX', regex=True))]['id'].array
        if len(end_refer) > 0:
            start_refer = conj_min - 1
            start_node = g_nodes[
                (g_nodes['sentence_id'] == sentence_id) & (g_nodes['max'] == start_refer) & (g_nodes['state'] == 1)][
                'id'].values[0]
            g_edges.loc[(g_edges['target'] == start_node) &
                        (g_edges['state'] == 1), 'target'] = conj_id
            g_edges.loc[(g_edges['id'] == conj_in) &
                        (g_edges['state'] == 1), ['rel_name', 'source', 'target']] = ['refer_start', conj_id,
                                                                                      start_node]
            g_edges.loc[(g_edges['source'] == conj_id) & (g_edges['rel_name'] == 'case') &
                        (g_edges['target'].isin(end_refer))
                        & (g_edges['state'] == 1), 'rel_name'] = 'refer_end'
            g_nodes.loc[(g_nodes['id'] == conj_id), 'text'] = 'Refers'
    return g_nodes, g_edges


def condition_construction(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    conditions = g_nodes[g_nodes['text'].isin(['yang'])]
    conditions = conditions.merge(g_edges.add_suffix('_source'), left_on='id', right_on='target_source')
    conditions = conditions.merge(g_nodes.add_suffix('_vsource'), left_on='source_source', right_on='id_vsource')
    conditions = conditions.merge(g_edges.add_suffix('_target'), left_on='id', right_on='source_target')
    conditions = conditions.merge(g_nodes.add_suffix('_vtarget'), left_on='target_target', right_on='id_vtarget')
    conditions = conditions[['sentence_id', 'id', 'id_source', 'rel_name_source', 'id_vsource', 'id_target',
                             'rel_name_target', 'id_vtarget', 'text_vsource', 'type_vsource', 'text', 'type',
                             'text_vtarget', 'type_vtarget']]
    g_edges.loc[g_edges['id'].isin(conditions.id_source.array), 'rel_name'] = 'has_condition'
    g_nodes.loc[g_nodes['id'].isin(conditions.id.array), ['text', 'type']] = ['Act', 'act']
    conj_cond = conditions[conditions['type_vtarget'] == 'cconj']
    conj_cond = conj_cond.sort_values(['id_source'])
    g_edges = g_edges.sort_values(['id'])
    g_edges.loc[g_edges['id'].isin(conj_cond.id_source.array),
                ['target']] = conj_cond.loc[conj_cond['id_source'].isin(conj_cond.id_source.array),
                                            ['id_vtarget']].values
    g_edges.loc[g_edges['id'].isin(conj_cond.id_target.array), 'state'] = 0
    g_nodes.loc[g_nodes['id'].isin(conj_cond.id.array), 'state'] = 0
    return g_nodes, g_edges


def if_else_construction(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    conditions = g_nodes[g_nodes['text'].isin(['Dalam hal', 'dalam hal', 'apabila']) & (g_nodes['state'] == 1)]
    conditions = conditions.merge(g_edges.add_suffix('_source'), left_on=['id', 'state'],
                                  right_on=['target_source', 'state_source'])
    conditions = conditions.merge(g_edges.add_suffix('_target'), how='left', left_on=['id', 'state'],
                                  right_on=['source_target', 'state_target'])
    conditions = conditions.merge(g_nodes.add_suffix('_vsource'), left_on='source_source', right_on='id_vsource')
    conditions = conditions.merge(g_nodes.add_suffix('_vtarget'), how='left', left_on='target_target',
                                  right_on='id_vtarget')
    except_cond = conditions[~conditions['id_target'].isna()][['id', 'sentence_id', 'is_root_sentence', 'state',
                                                               'id_target', 'rel_name_target', 'id_vtarget',
                                                               'text_vtarget', 'type_vtarget']]
    g_nodes.loc[g_nodes['id'].isin(except_cond.id.array), ['text', 'type']] = ['Act', 'act']
    g_edges.loc[g_edges['target'].isin(except_cond.id.array), 'rel_name'] = 'has_condition'
    except_cond[['is_act_type', 'new_rel_name']] = [0, except_cond['rel_name_target']]
    except_cond.loc[except_cond['type_vtarget'] == 'verb', ['is_act_type', 'new_rel_name']] = [1, 'has_act_type']
    except_cond = except_cond.sort_values(['id_target'])
    g_edges = g_edges.sort_values(['id'])
    try:
        g_edges.loc[g_edges['id'].isin(except_cond.id_target.array),
                    ['source', 'rel_name']] = except_cond.loc[
            except_cond['id_target'].isin(except_cond.id_target.array),
            ['id', 'new_rel_name']].values
    except ValueError:
        drop_article = error_handling(g_edges.loc[g_edges['id'].isin(except_cond.id_target.array)],
                                      except_cond.loc[except_cond['id_target'].isin(except_cond.id_target.array)],
                                      ['id'], ['id_target'])
        except_cond = except_cond[~except_cond['sentence_id'].isin(drop_article)].drop_duplicates()
        g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
        g_edges.loc[g_edges['id'].isin(except_cond.id_target.array),
                    ['source', 'rel_name']] = except_cond.loc[
            except_cond['id_target'].isin(except_cond.id_target.array),
            ['id', 'new_rel_name']].values
    no_verb_exception = except_cond[except_cond['is_act_type'] == 0]
    added_v = []
    added_e = []
    new_vid = len(g_nodes)
    new_eid = len(g_edges)
    for noverb in no_verb_exception.iterrows():
        sentence_id = noverb[1]['sentence_id']
        is_root_sentence = noverb[1]['is_root_sentence']
        source = noverb[1]['id']
        added_v.append({'sentence_id': sentence_id, 'id': new_vid, 'text': 'berkondisi', 'type': 'verb',
                        'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': source, 'target': new_vid,
                        'rel_name': 'has_act_type', 'state': 1, 'is_root_sentence': is_root_sentence})
        new_eid += 1
        new_vid += 1
    g_nodes = g_nodes.append(added_v, ignore_index=True)
    g_edges = g_edges.append(added_e, ignore_index=True)
    conditions = conditions[conditions['id_target'].isna()][['sentence_id', 'id', 'state', 'id_source',
                                                             'rel_name_source', 'id_vsource', 'text_vsource',
                                                             'type_vsource']]
    conditions = conditions.sort_values(['id_source'])
    conditions = conditions.merge(g_edges.add_suffix('_sin'), how='left', left_on=['id_vsource', 'state'],
                                  right_on=['target_sin', 'state_sin'])
    conditions = conditions.merge(g_nodes.add_suffix('_vsin'), how='left', left_on='source_sin', right_on='id_vsin')
    conditions = conditions.merge(g_edges.add_suffix('_sin2'), how='left', left_on=['source_sin', 'state'],
                                  right_on=['target_sin2', 'state_sin2'])
    conditions = conditions.merge(g_nodes.add_suffix('_vsin2'), how='left', left_on='source_sin2', right_on='id_vsin2')
    cond_source = conditions[['sentence_id', 'id', 'state', 'id_source', 'rel_name_source', 'id_vsource',
                              'text_vsource', 'type_vsource', 'id_sin', 'rel_name_sin', 'target_sin']]
    cond_sin = conditions[conditions['rel_name_sin'] != 'root'][['sentence_id', 'id', 'state', 'id_sin', 'rel_name_sin',
                                                                 'id_vsin', 'text_vsin', 'type_vsin', 'id_sin2',
                                                                 'rel_name_sin2', 'target_sin2']]
    cond_sin = cond_sin.rename(columns={'id_sin': 'id_source', 'rel_name_sin': 'rel_name_source',
                                        'id_vsin': 'id_vsource', 'text_vsin': 'text_vsource',
                                        'type_vsin': 'type_vsource', 'id_sin2': 'id_sin',
                                        'rel_name_sin2': 'rel_name_sin', 'target_sin2': 'target_sin'}, inplace=False)
    cond_source = pandas.concat([cond_source, cond_sin])
    if len(cond_source) == 0:
        return g_nodes, g_edges
    cond_source.loc[cond_source['rel_name_sin'] == 'root', 'is_root'] = 1
    cond_source.loc[cond_source['rel_name_sin'] != 'root', 'is_root'] = 0
    cond_source['is_root'] = cond_source.groupby(['id'])['is_root'].transform(max)
    cond_source[['new_source', 'new_estate', 'new_rel_name']] = [0, 1, 'has_subject']
    cond_source.loc[(cond_source['rel_name_sin'] == 'root') &
                    (cond_source['type_vsource'] == 'cconj'),
                    'new_source'] = cond_source.loc[(cond_source['rel_name_sin'] == 'root') &
                                                    (cond_source['type_vsource'] == 'cconj'), 'id_vsource'].values
    cond_source['new_source'] = cond_source.groupby(['id'])['new_source'].transform(max)
    cond_source = cond_source.sort_values(['id'])
    cond_source.loc[cond_source['new_source'] == 0, 'new_source'] = cond_source.loc[cond_source['new_source'] == 0,
                                                                                    'id'].values
    cond_source.loc[cond_source['id'] != cond_source['new_source'], 'new_estate'] = 0
    cond_source.loc[cond_source['type_vsource'] == 'verb', 'new_rel_name'] = 'has_act_type'
    cond_out = cond_source[cond_source['rel_name_sin'] == 'root']['id_vsource'].unique()
    if len(cond_out) > 0:
        cond_out = g_edges[g_edges['source'].isin(cond_out) & (g_edges['state'] == 1)]
        cond_out.loc[cond_out['rel_name'].isin(['advcl', 'parataxis']), 'is_new_root'] = 1
        cond_out.loc[~cond_out['rel_name'].isin(['advcl', 'parataxis']), 'is_new_root'] = 0
        cond_out['is_any_newroot'] = cond_out.groupby(['source'])['is_new_root'].transform(max)
        try:
            cond_out2 = cond_out[cond_out['is_any_newroot'] == 0]['target'].unique()
            cond_out2 = g_edges[g_edges['source'].isin(cond_out2) & (g_edges['state'] == 1) &
                                g_edges['rel_name'].isin(['advcl', 'parataxis'])]
            cond_out2['is_new_root'] = 1
            cond_out2 = cond_out2.merge(cond_out.add_suffix('_in'), left_on='source', right_on='target_in')
            cond_out = pandas.concat([cond_out2[['sentence_id', 'id', 'source_in',
                                                 'target']].rename(columns={'source_in': 'source'}, inplace=False),
                                      cond_out[cond_out['is_new_root'] == 1][['sentence_id', 'id', 'source',
                                                                              'target']]])
        except Exception:
            cond_out = cond_out[cond_out['is_new_root'] == 1]
            pass
        cond_out['root_num'] = cond_out.groupby(['source'])['id'].transform('nunique')
        double_root = cond_out[cond_out['root_num'] > 1]['target'].unique()
        double_root = g_nodes[g_nodes['id'].isin(double_root) & (g_nodes['state'] == 1)]
        double_root = double_root.merge(g_edges.add_suffix('_droot'), left_on=['id', 'state'],
                                        right_on=['source_droot', 'state_droot'])
        double_root = double_root[(double_root['rel_name_droot'] == 'has_act_type') | (double_root['type'] == 'verb')]
        cond_out = cond_out[(~cond_out['target'].isin(double_root['id'])) & (cond_out['root_num'] == 1)]
        cond_out = cond_out.merge(cond_source.add_suffix('_cond'), left_on='source', right_on='id_vsource_cond')
        cond_out['new_rel_name'] = 'has_condition'
        cond_out = cond_out[['sentence_id', 'id', 'id_sin_cond', 'new_source_cond', 'source', 'target',
                             'new_rel_name']].sort_values(['id'])

        g_edges = g_edges.sort_values(['id'])
        try:
            g_edges.loc[g_edges['id'].isin(cond_out.id.array),
                        ['source', 'target', 'rel_name']] = cond_out.loc[cond_out['id'].isin(cond_out.id.array),
                                                                         ['target', 'new_source_cond',
                                                                          'new_rel_name']].values
        except ValueError:
            drop_article = error_handling(g_edges.loc[g_edges['id'].isin(cond_out.id.array)],
                                          cond_out.loc[cond_out['id'].isin(cond_out.id.array)], ['id'], ['id'])
            cond_out = cond_out[~cond_out['sentence_id'].isin(drop_article)].drop_duplicates()
            g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
            g_edges.loc[g_edges['id'].isin(cond_out.id.array),
                        ['source', 'target', 'rel_name']] = cond_out.loc[cond_out['id'].isin(cond_out.id.array),
                                                                         ['target', 'new_source_cond',
                                                                          'new_rel_name']].values
        cond_out = cond_out.sort_values(['id_sin_cond'])
        try:
            g_edges.loc[g_edges['id'].isin(cond_out.id_sin_cond.array),
                        ['target']] = cond_out.loc[cond_out['id_sin_cond'].isin(cond_out.id_sin_cond.array),
                                                   ['target']].values
        except ValueError:
            drop_article = error_handling(g_edges.loc[g_edges['id'].isin(cond_out.id_sin_cond.array)],
                                          cond_out.loc[cond_out['id_sin_cond'].isin(cond_out.id_sin_cond.array)],
                                          ['id'], ['id_sin_cond'])
            cond_out = cond_out[~cond_out['sentence_id'].isin(drop_article)].drop_duplicates()
            g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
            g_edges.loc[g_edges['id'].isin(cond_out.id_sin_cond.array),
                        ['target']] = cond_out.loc[cond_out['id_sin_cond'].isin(cond_out.id_sin_cond.array),
                                                   ['target']].values
    cond_source = cond_source.sort_values('id_sin')
    try:
        g_edges.loc[g_edges['id'].isin(cond_source.id_sin.array) &
                    g_edges['rel_name'].isin(['advcl', 'parataxis']),
                    ['target', 'rel_name']] = cond_source.loc[cond_source['id_sin'].isin(cond_source.id_sin.array) &
                                                              cond_source['rel_name_sin'].isin(['advcl', 'parataxis']),
                                                              'new_source'].values, 'has_condition'
    except ValueError:
        drop_article = error_handling(g_edges.loc[g_edges['id'].isin(cond_source.id_sin.array) &
                                                  g_edges['rel_name'].isin(['advcl', 'parataxis'])],
                                      cond_source.loc[cond_source['id_sin'].isin(cond_source.id_sin.array) &
                                                      cond_source['rel_name_sin'].isin(['advcl', 'parataxis'])],
                                      ['id'], ['id_sin'])
        cond_source = cond_source[~cond_source['sentence_id'].isin(drop_article)].drop_duplicates()
        cond_source = cond_source.drop_duplicates()
        g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
        g_edges.loc[g_edges['id'].isin(cond_source.id_sin.array) &
                    g_edges['rel_name'].isin(['advcl', 'parataxis']),
                    ['target', 'rel_name']] = cond_source.loc[cond_source['id_sin'].isin(cond_source.id_sin.array) &
                                                              cond_source['rel_name_sin'].isin(['advcl', 'parataxis']),
                                                              'new_source'].values, 'has_condition'
    cond_source = cond_source.sort_values('id_source')
    try:
        g_edges.loc[g_edges['id'].isin(cond_source.id_source.array),
                    ['source', 'target', 'rel_name',
                     'state']] = cond_source.loc[cond_source['id_source'].isin(cond_source.id_source.array),
                                                 ['new_source', 'id_vsource', 'new_rel_name', 'new_estate']].values
    except ValueError:
        drop_article = error_handling(g_edges.loc[g_edges['id'].isin(cond_source.id_source.array)],
                                      cond_source.loc[cond_source['id_source'].isin(cond_source.id_source.array)],
                                      ['id'], ['id_source'])
        cond_source = cond_source[~cond_source['sentence_id'].isin(drop_article)].drop_duplicates()
        g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
        g_edges.loc[g_edges['id'].isin(cond_source.id_source.array),
                    ['source', 'target', 'rel_name',
                     'state']] = cond_source.loc[cond_source['id_source'].isin(cond_source.id_source.array),
                                                 ['new_source', 'id_vsource', 'new_rel_name', 'new_estate']].values
    cond_source = cond_source[cond_source['id'] == cond_source['new_source']]['id'].unique()
    g_nodes.loc[g_nodes['id'].isin(cond_source), ['text', 'type']] = ['Act', 'act']
    g_nodes.loc[g_nodes.text.str.contains('sebagaimana (dimaksud|tersebut)', regex=True), 'text'] = 'refer_to'
    return g_nodes, g_edges


def act_construction(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    g_edges['rank'] = g_edges.groupby(['source', 'target', 'rel_name', 'state'])['id'].transform('rank')
    g_edges.loc[g_edges['rank'] != 1, 'state'] = 0
    g_edges = g_edges.drop(columns=['rank'])
    act_types = g_edges[(g_edges.rel_name.isin(['has_act_type', 'has_act_type:pass'])) & (g_edges.state == 1)]
    act_types = act_types.merge(g_nodes, left_on='source', right_on='id', suffixes=['', '_source'])
    act_types = act_types[act_types['text'] != 'Act']

    act_types['rank'] = act_types['id'].transform('rank') - 1
    act_types['new_vid'] = len(g_nodes) + act_types['rank']
    act_types['new_eid'] = len(g_edges) + act_types['rank']
    act_in = act_types.merge(g_edges, left_on=['source', 'state'], right_on=['target', 'state'],
                             suffixes=['', '_source_in'])
    act_types = act_types.sort_values(['id'])
    act_in = act_in.sort_values(['id_source_in'])
    added_e = []
    added_v = []
    for act_type in act_types.iterrows():
        source = act_type[1]['source']
        sentence_id = act_type[1]['sentence_id']
        is_root_sentence = act_type[1]['is_root_sentence']
        new_vid = act_type[1]['new_vid']
        new_eid = act_type[1]['new_eid']
        added_v.append({'sentence_id': sentence_id, 'id': new_vid, 'text': 'Act', 'type': 'act',
                        'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': new_vid, 'target': source,
                        'rel_name': 'has_subject', 'state': 1, 'is_root_sentence': is_root_sentence})
    g_nodes = g_nodes.append(added_v, ignore_index=True)
    g_edges = g_edges.append(added_e, ignore_index=True)
    g_edges = g_edges.sort_values(['id'])
    try:
        g_edges.loc[g_edges.id.isin(act_in.id_source_in.array),
                    ['target']] = act_in.loc[act_in['id_source_in'].isin(act_in.id_source_in.array),
                                             ['new_vid']].values
    except ValueError:
        drop_article = error_handling(g_edges.loc[g_edges.id.isin(act_in.id_source_in.array)],
                                      act_in.loc[act_in['id_source_in'].isin(act_in.id_source_in.array)], ['id'],
                                      ['id_source_in'])
        act_in = act_in[~act_in['sentence_id'].isin(drop_article)].drop_duplicates()
        g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
        g_edges.loc[g_edges.id.isin(act_in.id_source_in.array),
                    ['target']] = act_in.loc[act_in['id_source_in'].isin(act_in.id_source_in.array),
                                             ['new_vid']].values
    g_edges.loc[g_edges.id.isin(act_types.id.array),
                'source'] = act_types.loc[act_types['id'].isin(act_types.id.array),
                                          ['new_vid']].values

    root = g_edges[(g_edges['rel_name'] == 'root') & (g_edges['state'] == 1)]
    root = root.merge(g_edges.add_suffix('_out'), left_on=['target', 'state'], right_on=['source_out', 'state_out'])
    root = root.merge(g_nodes.add_suffix('_target_out'), left_on='target_out', right_on='id_target_out')
    root['eouts1'] = root.groupby('id')['id_out'].transform('nunique')
    root = root[~(root['rel_name_out'].str.contains('has_act_type') |
                  (root['rel_name_out'].str.contains('conj') & (root['type_target_out'] == 'verb')))]
    root['eouts2'] = root.groupby('id')['id_out'].transform('nunique')
    root = root[root['eouts1'] == root['eouts2']]
    root = root[(root['type_target_out'] == 'verb') & (root['rel_name_out'] != 'conj')]
    g_edges.loc[g_edges['id'].isin(root.id_out.array), 'rel_name'] = 'has_act_type'
    return g_nodes, g_edges


def conj_finishing(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    conjs = g_edges[(g_edges.rel_name == 'conj') & (g_edges.state == 1)]
    while len(conjs) > 0:
        convert = [{'cc': 'dan/atau', 'new_rel_name': '_or'},
                   {'cc': 'dan/ atau', 'new_rel_name': '_or'},
                   {'cc': 'dan atau', 'new_rel_name': '_or'},
                   {'cc': 'dan /atau', 'new_rel_name': '_or'},
                   {'cc': 'dan / atau', 'new_rel_name': '_or'},
                   {'cc': 'dan', 'new_rel_name': '_and'},
                   {'cc': 'atau', 'new_rel_name': '_xor'},
                   {'cc': 'serta', 'new_rel_name': '_and'},
                   {'cc': 'other', 'new_rel_name': '_uconj'}]
        convert = pandas.DataFrame(convert)
        conjs = conjs.merge(g_nodes.add_suffix('_cc'), left_on='source', right_on='id_cc')
        conjs = conjs.merge(g_nodes.add_suffix('_child'), left_on='target', right_on='id_child')
        conjs = conjs.merge(convert, left_on='text_cc', right_on='cc')
        if len(conjs) == 0:
            temp = conjs.merge(g_edges.add_suffix('_temp'), left_on=['source', 'state'],
                               right_on=['target_temp', 'state_temp'])
            temp = temp[temp['rel_name_temp'].str.contains(r'_(and|or|xor)$', regex=True)]
            temp = temp.sort_values(['id'])
            if len(temp) > 0:
                g_edges.loc[g_edges.id.isin(temp.id.array),
                            ['source', 'rel_name']] = temp[temp['id'].isin(temp.id.array),
                                                           ['source_temp', 'rel_name_temp']].values
                continue
            else:
                g_edges.loc[g_edges.id.isin(conjs.id.array), 'rel_name'] = 'advcl'
                break
        conjs['new_cc_text'] = 'Concept'
        conjs.loc[conjs['type_child'] == 'verb', 'new_cc_text'] = 'Act'
        conjs.loc[(conjs['type_child'] == 'cconj') & (~conjs['text_child'].isin(['dan', 'atau', 'dan/atau', 'other'])),
                  'new_cc_text'] = conjs.loc[(conjs['type_child'] == 'cconj') &
                                             (~conjs['text_child'].isin(['dan', 'atau', 'dan/atau', 'other'])),
                                             'text_child'].str.slice(stop=-1).values
        conjs.loc[conjs['text_child'].str.contains(r'Pasal|ayat|RX', regex=True), 'new_cc_text'] = 'Refer'
        conjs['new_rel_name'] = conjs['new_cc_text'].str.lower() + conjs['new_rel_name']
        exception_conjs = conjs[conjs['text_child'].isin(convert.cc.array)]
        conjs = conjs[(~conjs['source'].isin(exception_conjs.source.array))]
        if len(conjs) == 0:
            g_edges.loc[g_edges.id.isin(exception_conjs.id.array), 'state'] = 0

        cc = conjs[['sentence_id', 'id', 'source', 'target', 'is_root_sentence',
                    'new_cc_text']].copy().drop_duplicates()
        cc['count'] = cc.groupby(['source', 'new_cc_text'])['id'].transform('nunique')
        cc['max_f'] = cc.groupby(['source'])['count'].transform(max)
        cc = cc[cc['count'] == cc['max_f']].drop(columns=['id', 'target']).drop_duplicates()
        cc['rank'] = cc.groupby(['source'])['max_f'].rank(method='first')
        cc = cc[cc['rank'] == 1]
        cc['xx'] = cc.groupby(['source'])['count'].transform('count')
        cc = cc.sort_values(['source'])
        conjs = conjs.sort_values(['id'])
        g_nodes = g_nodes.sort_values('id')
        g_nodes.loc[g_nodes.id.isin(cc.source.array),
                    'text'] = cc.loc[cc['source'].isin(cc.source.array), 'new_cc_text'].values + 's'
        g_nodes.loc[g_nodes.id.isin(cc.source.array),
                    'type'] = g_nodes.loc[g_nodes.id.isin(cc.source.array), 'text'].str.lower()
        g_edges.loc[g_edges.id.isin(conjs.id.array),
                    'rel_name'] = conjs.loc[conjs['id'].isin(conjs.id.array), 'new_rel_name'].values
        act_conjs = conjs[conjs['new_cc_text'] == 'Act'].copy()
        act_conjs['rank'] = act_conjs['target'].rank(method='dense') - 1
        act_conjs['new_vid'] = act_conjs['rank'] + len(g_nodes)
        act_conjs['new_eid'] = act_conjs['rank'] + len(g_edges)
        new_nodes = act_conjs[['sentence_id', 'target', 'is_root_sentence', 'new_vid', 'new_eid']].drop_duplicates()
        added_v = []
        added_e = []
        for new_n in new_nodes.iterrows():
            sentence_id = new_n[1]['sentence_id']
            is_root_sentence = new_n[1]['is_root_sentence']
            new_vid = new_n[1]['new_vid']
            new_eid = new_n[1]['new_eid']
            target = new_n[1]['target']
            added_v.append({'sentence_id': sentence_id, 'id': new_vid, 'text': 'Act', 'type': 'act',
                            'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
            added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': new_vid, 'target': target,
                            'rel_name': 'has_act_type', 'state': 1, 'is_root_sentence': is_root_sentence})
        g_nodes = g_nodes.append(added_v, ignore_index=True)
        g_edges = g_edges.append(added_e, ignore_index=True)
        act_conjs = act_conjs.sort_values(['id'])
        act_conjs_out = act_conjs.merge(g_edges.add_suffix('_out'), how='left', left_on=['target', 'state'],
                                        right_on=['source_out', 'state_out'])
        act_conjs_out = act_conjs_out[~act_conjs_out['rel_name_out'].isin(['amod', 'appos', 'advmod', 'compound',
                                                                           'flat', 'xcomp', 'acl', 'nummod', 'det'])]
        act_conjs_out = act_conjs_out[['id_out', 'new_vid']].dropna().drop_duplicates().sort_values(['id_out'])

        g_edges = g_edges.sort_values(['id'])
        g_edges.loc[g_edges.id.isin(act_conjs.id.array),
                    ['target']] = act_conjs.loc[act_conjs['id'].isin(act_conjs.id.array), 'new_vid'].values
        g_edges.loc[g_edges.id.isin(act_conjs_out.id_out.array),
                    'source'] = act_conjs_out.loc[act_conjs_out['id_out'].isin(act_conjs_out.id_out.array),
                                                  'new_vid'].values
        conjs = g_edges[(g_edges.rel_name == 'conj') & (g_edges.state == 1)]
    return g_nodes, g_edges


def cases_finishing(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    '''initiate case'''
    cases = g_edges[(g_edges['rel_name'].isin(['case', 'mark'])) & (g_edges['state'] == 1)]
    i = 0
    while len(cases) > 0:
        cases = cases.merge(g_nodes.add_suffix('_vsource'), left_on='source', right_on='id_vsource')
        cases = cases.merge(g_nodes.add_suffix('_vtarget'), left_on='target', right_on='id_vtarget')
        all_refers = g_nodes[g_nodes['text'].str.contains(r'RX', regex=True)].id.array
        cases = cases.merge(g_edges, how='left', left_on=['target', 'state'], right_on=['source', 'state'],
                            suffixes=['', '_tout'])
        cases = cases.merge(g_edges, left_on=['source', 'state'], right_on=['target', 'state'], suffixes=['', '_sin'])
        cases = cases[cases['rel_name_sin'] != 'has_condition']
        cases = cases.merge(g_nodes.add_suffix('_vsin'), left_on='source_sin', right_on='id_vsin')
        cases = cases.merge(g_edges, how='left', left_on=['source_sin', 'state'], right_on=['target', 'state'],
                            suffixes=['', '_sin2'])
        cases = cases.merge(g_nodes.add_suffix('_vsin2'), how='left', left_on='source_sin2', right_on='id_vsin2')

        '''filter overlapped cases'''
        nodes_c = cases.merge(g_nodes.add_suffix('_v'), left_on=['sentence_id', 'state'],
                              right_on=['sentence_id_v', 'state_v'])[
            ['sentence_id', 'id', 'id_vsource', 'id_vtarget', 'id_vsin',
             'source_sin2', 'id_v']]
        nodes_c = nodes_c[(nodes_c.id_v == nodes_c.id_vsource) | (nodes_c.id_v == nodes_c.id_vtarget) |
                          (nodes_c.id_v == nodes_c.id_vsin) | (nodes_c.id_v == nodes_c.source_sin2)].drop_duplicates()
        nodes_c['rank'] = nodes_c.groupby(['id_v'])['id'].rank(method='first')
        nodes_c['rank'] = nodes_c.groupby(['id'])['rank'].transform('max')
        nodes_c = nodes_c.drop(columns=['id_v']).drop_duplicates()
        nodes_c = nodes_c[nodes_c['rank'] == 1]['id'].unique()
        cases = cases[cases['id'].isin(nodes_c)]
        if len(cases) == 0:
            break
        '''splitting case'''
        cases = cases.reset_index()
        reg_refers = cases[(cases['text_vsin'] == 'refer_to') &
                           (cases['target'].isin(all_refers) |
                            ((cases['rel_name_tout'].str.contains('^refer_(and|or|xor|uconj)')) &
                             cases['target_tout'].isin(all_refers)))]
        verbs = cases[(cases['type_vtarget'] == 'verb') & cases['text_vtarget'].str.contains('(?i)^(me|ter|di)')]
        cases = cases.drop(index=reg_refers.index.union(verbs.index))
        '''handle when case is refer'''
        excl_refers = reg_refers[reg_refers['text_vsin2'].isin(['Concept', 'Act']) |
                                 (reg_refers['type_vsin2'] == 'verb')]
        reg_refers = reg_refers[(~(reg_refers['text_vsin2'].isin(['Concept', 'Act']) |
                                   (reg_refers['type_vsin2'] == 'verb')))]
        reg_refers = reg_refers.merge(g_edges, how='left', left_on=['source_sin2', 'state'],
                                      right_on=['target', 'state'],
                                      suffixes=['', '_sin3'])[['sentence_id', 'state', 'id', 'id_sin', 'id_sin2',
                                                               'id_sin3', 'source', 'source_sin2', 'source_sin',
                                                               'type_vsin2']].drop_duplicates()
        refers_out = reg_refers.merge(g_edges.add_suffix('_sin_out'), how='left', left_on=['sentence_id', 'state'],
                                      right_on=['sentence_id_sin_out', 'state_sin_out'])
        refers_out = refers_out[((refers_out['source_sin_out'] == refers_out['source_sin']) &
                                 (refers_out['id_sin_out'] != refers_out['id_sin'])) |
                                ((refers_out['source_sin_out'] == refers_out['source_sin2']) &
                                 (refers_out['id_sin_out'] != refers_out['id_sin2']) &
                                 (~refers_out['rel_name_sin_out'].isin(['amod', 'appos', 'advmod', 'compound', 'flat',
                                                                        'xcomp', 'acl', 'nummod', 'det'])))]
        reg_refers['new_rel_name_sin'] = 'has_subject'
        reg_refers = reg_refers.sort_values(['id'])
        refers_sin3 = reg_refers[['sentence_id', 'id_sin3', 'source']].sort_values(['id_sin3'])
        refers_sin = reg_refers[['id_sin', 'source', 'source_sin2',
                                 'new_rel_name_sin']].drop_duplicates().sort_values(['id_sin'])
        refers_vsin2 = reg_refers[['sentence_id', 'type_vsin2', 'source']].drop_duplicates().sort_values(['source'])
        g_edges = g_edges.sort_values(['id'])
        try:
            g_edges.loc[g_edges.id.isin(refers_sin3.id_sin3.array),
                        'target'] = refers_sin3.loc[refers_sin3['id_sin3'].isin(refers_sin3.id_sin3.array),
                                                    'source'].values
        except ValueError:
            drop_article = error_handling(g_edges.loc[g_edges.id.isin(refers_sin3.id_sin3.array)],
                                          refers_sin3.loc[refers_sin3['id_sin3'].isin(refers_sin3.id_sin3.array)],
                                          ['id'], ['id_sin3'])
            refers_sin3 = refers_sin3[~refers_sin3['sentence_id'].isin(drop_article)].drop_duplicates()
            g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
            g_edges.loc[g_edges.id.isin(refers_sin3.id_sin3.array),
                        'target'] = refers_sin3.loc[refers_sin3['id_sin3'].isin(refers_sin3.id_sin3.array),
                                                    'source'].values
        try:
            g_edges.loc[g_edges.id.isin(refers_sin.id_sin.array),
                        ['source', 'target',
                         'rel_name']] = refers_sin.loc[refers_sin['id_sin'].isin(refers_sin.id_sin.array),
                                                       ['source', 'source_sin2', 'new_rel_name_sin']].values
        except ValueError:
            drop_article = error_handling(g_edges.loc[g_edges.id.isin(refers_sin.id_sin.array)],
                                          refers_sin.loc[refers_sin['id_sin'].isin(refers_sin.id_sin.array)],
                                          ['id'], ['id_sin'])
            refers_sin = refers_sin[~refers_sin['sentence_id'].isin(drop_article)]
            g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
            g_edges.loc[g_edges.id.isin(refers_sin.id_sin.array),
                        ['source', 'target', 'rel_name']] = refers_sin.loc[
                refers_sin['id_sin'].isin(refers_sin.id_sin.array),
                ['source', 'source_sin2', 'new_rel_name_sin']].values
        refers_out = refers_out[['id_sin_out', 'source']].drop_duplicates().sort_values(['id_sin_out'])
        g_edges.loc[g_edges.id.isin(reg_refers.id.array), 'rel_name'] = 'refer_to'
        g_nodes.loc[g_nodes.id.isin(refers_vsin2.source.array),
                    ['text', 'type']] = ['Concept', 'concept']
        g_edges.loc[g_edges.id.isin(reg_refers.id_sin2.array), 'state'] = 0
        g_nodes.loc[g_nodes.id.isin(reg_refers.source_sin.array), 'state'] = 0
        try:
            g_edges.loc[g_edges.id.isin(refers_out.id_sin_out),
                        'source'] = refers_out.loc[refers_out['id_sin_out'].isin(refers_out.id_sin_out.array),
                                                   'source'].values
        except ValueError:
            drop_article = error_handling(g_edges.loc[g_edges.id.isin(refers_out.id_sin_out)],
                                          refers_out.loc[refers_out['id_sin_out'].isin(refers_out.id_sin_out.array)],
                                          ['id'], ['id_sin_out'])
            refers_out = refers_out[~refers_out['sentence_id'].isin(drop_article)]
            g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
            g_edges.loc[g_edges.id.isin(refers_out.id_sin_out),
                        'source'] = refers_out.loc[refers_out['id_sin_out'].isin(refers_out.id_sin_out.array),
                                                   'source'].values
        excl_ref_sin = excl_refers[['id', 'source_sin2', 'text_vsin']].drop_duplicates()
        g_edges.loc[g_edges.id.isin(excl_ref_sin.id.array),
                    ['source', 'rel_name']] = excl_ref_sin.loc[excl_ref_sin['id'].isin(excl_ref_sin.id.array),
                                                               ['source_sin2', 'text_vsin']].values
        excl_ref_out = excl_refers.merge(g_edges.add_suffix('_out'), how='left', left_on=['sentence_id', 'state'],
                                         right_on=['sentence_id_out', 'state_out'])
        excl_ref_out = excl_ref_out[((excl_ref_out['source_out'] == excl_ref_out['source_sin']) &
                                     (excl_ref_out['id_out'] != excl_ref_out['id_sin'])) |
                                    ((excl_ref_out['source_out'] == excl_ref_out['source']) &
                                     (excl_ref_out['id_out'] != excl_ref_out['id']))]
        excl_ref_out = excl_ref_out[['source_sin2', 'id_out']].dropna().drop_duplicates().sort_values(['id_out'])
        g_edges.loc[g_edges.id.isin(excl_ref_out.id_out.array),
                    ['source']] = excl_ref_out.loc[excl_ref_out['id_out'].isin(excl_ref_out.id_out.array),
                                                   ['source_sin2']].values
        g_edges.loc[g_edges.id.isin(excl_refers.id_sin.array) | g_edges.id.isin(excl_refers.id_sin2.array),
                    'state'] = 0

        '''handle when case is verb'''
        verbs_out = verbs.merge(g_edges.add_suffix('_out'), left_on=['target', 'state'],
                                right_on=['source_out', 'state_out'])
        verbs_out = verbs_out[(~verbs_out['rel_name_out'].isin(['amod', 'appos', 'advmod', 'compound', 'flat', 'xcomp',
                                                                'acl', 'nummod', 'det']))][['sentence_id', 'id_out',
                                                                                            'source_out',
                                                                                            'source']].drop_duplicates()
        verbs_sin = verbs[['id_sin', 'text_vsource']].drop_duplicates()
        verbs_sin = verbs_sin.sort_values('id_sin')
        verbs_out = verbs_out.sort_values(['id_out'])
        g_edges.loc[g_edges.id.isin(verbs_sin.id_sin.array),
                    'rel_name'] = verbs_sin.loc[verbs_sin['id_sin'].isin(verbs_sin.id_sin.array),
                                                'text_vsource'].values
        g_edges.loc[g_edges.id.isin(verbs.id.array), 'rel_name'] = 'has_act_type'
        g_nodes.loc[g_nodes.id.isin(verbs.id_vsource.array), ['text', 'type']] = ['Act', 'act']
        g_edges.loc[g_edges.id.isin(verbs_out.id_out),
                    'source'] = verbs_out.loc[verbs_out['id_out'].isin(verbs_out.id_out.array), 'source'].values

        '''handle when case is other'''
        cases = cases[['sentence_id', 'id', 'source_sin', 'id_sin', 'text_vsource']].drop_duplicates()
        cases = cases.sort_values(['id'])
        try:
            g_edges.loc[g_edges.id.isin(cases.id.array),
                        ['source', 'rel_name']] = cases.loc[cases['id'].isin(cases.id.array),
                                                            ['source_sin', 'text_vsource']].values
        except ValueError:
            drop_article = error_handling(g_edges.loc[g_edges.id.isin(cases.id.array)],
                                          cases.loc[cases['id'].isin(cases.id.array)],
                                          ['id'], ['id'])
            cases = cases[~cases['sentence_id'].isin(drop_article)]
            g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
            g_edges.loc[g_edges.id.isin(cases.id.array),
                        ['source', 'rel_name']] = cases.loc[cases['id'].isin(cases.id.array),
                                                            ['source_sin', 'text_vsource']].values
        g_edges.loc[g_edges.id.isin(cases.id_sin.array), 'state'] = 0
        '''re-initiate case'''
        cases = g_edges[(g_edges['rel_name'].isin(['case', 'mark'])) & (g_edges['state'] == 1)]
        i += 1
    return g_nodes, g_edges


def acts_reformat(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    conj_acts = g_nodes[(g_nodes.text == 'Acts') & (g_nodes.state == 1)]
    new_eid = len(g_edges)
    added_e = []
    for acts in conj_acts.iterrows():
        acts_id = acts[1]['id']
        sentence_id = acts[1]['sentence_id']
        is_root_sentence = acts[1]['is_root_sentence']
        act_child = g_edges[(g_edges.source == acts_id) &
                            g_edges.rel_name.str.contains('^act_', regex=True) &
                            (g_edges.state == 1)].target.array
        other_rel = g_edges[(g_edges.source == acts_id) &
                            (~g_edges.rel_name.str.contains('^act_', regex=True)) &
                            (g_edges.state == 1)]
        for child in act_child:
            child_acttype = g_edges[(g_edges.rel_name == 'has_act_type') & (g_edges.source == child) &
                                    (g_edges.state == 1)].target.array[0]
            for rel in other_rel.iterrows():
                added_e.append({'sentence_id': sentence_id, 'id': new_eid,
                                'source': child_acttype, 'target': rel[1]['target'],
                                'rel_name': rel[1]['rel_name'], 'state': 1, 'is_root_sentence': is_root_sentence})
                new_eid += 1
        g_edges.loc[g_edges.id.isin(other_rel.id.array), 'state'] = 0
    g_edges = g_edges.append(added_e, ignore_index=True)
    return g_nodes, g_edges


def modality_handler(graph, modality):
    g_nodes = graph['V']
    g_edges = graph['E']
    advmods = g_edges[(g_edges.rel_name == 'advmod') & (g_edges.state == 1)]
    for advmod in advmods.iterrows():
        rel_id = advmod[1]['id']
        source = advmod[1]['source']
        target = advmod[1]['target']
        sentence_id = advmod[1]['sentence_id']
        is_root_sentence = advmod[1]['is_root_sentence']
        source_detail = g_nodes[(g_nodes.id == source)].reset_index().to_dict()
        source_name = source_detail['text'][0]
        source_type = source_detail['type'][0]
        source_min = source_detail['min'][0]
        source_in_detail = g_edges[(g_edges.target == source) & (g_edges.state == 1)].reset_index().to_dict()
        try:
            source_in = source_in_detail['source'][0]
            source_in_rel_name = source_in_detail['rel_name'][0]
            target_detail = g_nodes[(g_nodes.id == target)].reset_index().to_dict()
            target_name = target_detail['text'][0]
            target_min = target_detail['min'][0]
            target_max = target_detail['max'][0]
            state_funct = target_detail['state'][0]
        except KeyError:
            g_edges.loc[g_edges['sentence_id'] == sentence_id, 'state'] = 0
            continue
        if target_name in modality and source_type == 'verb' and re.search('has_act_type', source_in_rel_name):
            g_edges.loc[(g_edges.id == rel_id), ['rel_name', 'source']] = ['has_modality', source_in]
        elif target_name in modality and source_type != 'verb':
            new_vid = len(g_nodes)
            new_eid = len(g_edges)
            added_v = [{'sentence_id': sentence_id, 'id': new_vid, 'text': 'Act', 'type': 'act',
                        'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': state_funct},
                       {'sentence_id': sentence_id, 'id': new_vid + 1, 'text': 'berupa', 'type': 'verb',
                        'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': state_funct}]
            added_e = [{'sentence_id': sentence_id, 'id': new_eid, 'source': new_vid, 'target': new_vid + 1,
                        'rel_name': 'has_act_type', 'state': 1, 'is_root_sentence': is_root_sentence},
                       {'sentence_id': sentence_id, 'id': new_eid + 1, 'source': new_vid, 'target': source,
                        'rel_name': 'has_subject', 'state': 1, 'is_root_sentence': is_root_sentence}]
            g_edges.loc[(g_edges.id == rel_id), ['rel_name', 'source']] = ['has_modality', new_vid]
            g_edges.loc[(g_edges.target == source), 'target'] = new_vid
            g_edges = g_edges.append(added_e, ignore_index=True)
            g_nodes = g_nodes.append(added_v, ignore_index=True)
        elif source_type == 'verb' and target_max == source_min - 1:
            g_nodes.loc[(g_nodes.id == source), ['text', 'min']] = [target_name + ' ' + source_name, target_min]
            g_edges.loc[(g_edges.id == rel_id), 'state'] = 0
    return g_nodes, g_edges


def based_on_formatter(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    berdasarkan = g_nodes[g_nodes.text.isin(['berdasarkan', 'Berdasarkan', 'sebagaimana dimaksud', 'diatur'])]
    all_refers = g_nodes[g_nodes.text.str.contains(r'RX', regex=True)].id.array
    for refer in berdasarkan.iterrows():
        refer_id = refer[1]['id']
        refer_min = refer[1]['min']
        sentence_id = refer[1]['sentence_id']
        refer_eout = g_edges[(g_edges.source == refer_id) & (g_edges.target.isin(all_refers)) &
                             (g_edges.state == 1)].id.array
        if len(refer_eout) == 0:
            continue
        refer_ein = g_edges[(g_edges.target == refer_id) & (g_edges.state == 1)].id.array[0]
        refer_in = g_edges[(g_edges.id == refer_ein)].source.array[0]
        refer_relin = g_edges[(g_edges.id == refer_ein)].rel_name.array[0]
        try:
            subject = g_nodes[(g_nodes['max'] < refer_min) & g_nodes.type.isin(['propn', 'noun', 'phrase']) &
                              (g_nodes.sentence_id == sentence_id)].sort_values(by=['max', 'min', 'id'],
                                                                                ascending=[False, True, True])[
                'id'].array[0]
        except IndexError:
            g_edges.loc[g_edges['sentence_id'] == sentence_id, 'state'] = 0
            continue
        subject = list(g_edges[(g_edges.target == subject) & (g_edges.state == 1) &
                               (g_edges.rel_name.str.contains('and|xor|or', regex=True))].source.array) + [subject]
        if re.search('has_act_type', refer_relin):
            refer_in_xcomp = g_edges[(g_edges['source'] == refer_id) &
                                     g_edges['rel_name'].isin(['parataxis', 'xcomp', 'advcl']) &
                                     (g_edges['state'] == 1)].sort_values('rel_name', ascending=True)['target'].array
            g_edges.loc[
                g_edges['id'].isin(refer_eout), 'source'] = subject[0]
            g_edges.loc[
                g_edges['id'].isin(refer_eout), 'rel_name'] = 'refer_to'
            if len(refer_in_xcomp) > 0:
                g_edges.loc[(g_edges['id'] == refer_ein), 'target'] = refer_in_xcomp[0]
                g_edges.loc[(g_edges['source'] == refer_id) &
                            (g_edges['target'] == refer_in_xcomp[0]), 'state'] = 0
                g_edges.loc[(g_edges['source'] == refer_id) &
                            (g_edges['state'] == 1), 'source'] = refer_in_xcomp[0]
            else:
                g_edges.loc[(g_edges['id'] == refer_ein), 'state'] = 0
                g_edges.loc[(g_edges['target'] == refer_in), 'state'] = 0
        else:
            g_edges.loc[g_edges['id'].isin(refer_eout), ['rel_name', 'source']] = ['refer_to', subject[0]]
            g_edges.loc[(g_edges['id'] == refer_ein), 'state'] = 0
            g_edges.loc[g_edges['source'] == refer_id, 'source'] = subject[0]
    return g_nodes, g_edges


def verb_to_verb_handler(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    all_verb1 = g_nodes[g_nodes['type'].isin(['verb', 'act'])]['id'].array
    all_verb2 = g_nodes[g_nodes['type'].isin(['verb'])]['id'].array
    vtovs = g_edges[g_edges['source'].isin(all_verb1) &
                    (g_edges['rel_name'].str.contains('advcl|xcomp|parataxis', regex=True)) &
                    g_edges['target'].isin(all_verb2) & (g_edges['state'] == 1)]
    for vtov in vtovs.iterrows():
        eid = vtov[1]['id']
        source = vtov[1]['source']
        target = vtov[1]['target']
        target_text = g_nodes[(g_nodes['id'] == target)]['text'].array[0]
        obj_in = g_edges[(g_edges['source'] == target) & (g_edges['rel_name'] == 'obj') &
                         (g_edges['state'] == 1)]['id'].array
        if len(obj_in > 0):
            g_edges.loc[g_edges['id'].isin(obj_in), 'rel_name'] = target_text
            g_edges.loc[(g_edges['source'] == target) &
                        (g_edges['state'] == 1), 'source'] = source
            g_edges.loc[(g_edges['id'] == eid), 'state'] = 0
    g_edges.loc[g_edges['rel_name'] == 'obj', 'rel_name'] = 'has_object'
    return g_nodes, g_edges


def numeral_construction(graph, modality):
    g_nodes = graph['V']
    g_edges = graph['E']
    modality_ids = g_nodes[g_nodes['text'].isin(modality)]['id'].array
    acts = g_nodes[g_nodes['text'].isin(['Act', 'Acts'])]['id'].array
    g_edges.loc[(g_edges['rel_name'] == 'advmod') & g_edges['source'].isin(acts) &
                g_edges['target'].isin(modality_ids) &
                (g_edges['state'] == 1), 'rel_name'] = 'has_modality'
    pointers = g_nodes[g_nodes['text'].str.contains('^(paling |jangka waktu|setiap|dari|atas)', regex=True)]['id'].array
    verbs = list(g_edges[g_edges['rel_name'].str.contains('^act_type|root', regex=True) & g_edges['state'] == 1][
                     'target'].array)
    nums = g_nodes[g_nodes['text'].str.contains(r'^\d+$', regex=True)]
    for num in nums.iterrows():
        num_d = num[1]['id']
        num_d_min = num[1]['min']
        num_d_max = num[1]['max']
        sentence_id = num[1]['sentence_id']
        is_root_sentence = num[1]['is_root_sentence']
        pointer = g_nodes[g_nodes['id'].isin(pointers) & (g_nodes['max'] < num_d_min) &
                          (g_nodes['sentence_id'] == sentence_id)].merge(g_edges,
                                                                         left_on=['id', 'sentence_id'],
                                                                         right_on=['target', 'sentence_id'],
                                                                         suffixes=['', '_sin'])
        num_t_text = ''
        is_continue = False
        num_t = -1
        pointer = pointer[(pointer['rel_name'] != 'has_modality') & (pointer['state'] == 1)].reset_index().to_dict()
        pointer_max, pointer_min = None, None
        num_t_max, num_t_min = None, None
        unit_max, unit_min = None, None
        if len(pointer['id']) > 0:
            pointer_id = pointer['id'][0]
            pointer_max = pointer['max'][0]
            pointer_min = pointer['min'][0]
            is_continue = True
        else:
            pointer_id = -1
        try:
            num_t_detail = g_nodes[(g_nodes['min'] == num_d_max + 2) & (g_nodes['sentence_id'] == sentence_id) &
                                   (g_nodes['type'] == 'num')].sort_values('max',
                                                                           ascending=False).reset_index().to_dict()
            num_t = num_t_detail['id'][0]
            num_t_text = num_t_detail['text'][0]
            num_t_max = num_t_detail['max'][0]
            num_t_min = num_t_detail['min'][0]
            is_continue = True
        except Exception:
            pass
        unit_text = ''
        unit = -1
        try:
            unit_detail = g_nodes[(g_nodes['min'] > max(num_d_max, num_t_max)) &
                                  (g_nodes['min'] <= max(num_d_max, num_t_max) + 2) &
                                  (g_nodes['sentence_id'] == sentence_id) &
                                  (g_nodes['type'] == 'noun')].sort_values('max',
                                                                           ascending=False).reset_index().to_dict()
            unit = unit_detail['id'][0]
            unit_text = unit_detail['text'][0]
            unit_max = unit_detail['max'][0]
            unit_min = unit_detail['min'][0]
            is_continue = True
        except Exception:
            pass
        root = g_edges[(g_edges['rel_name'] == 'root') & (g_edges['state'] == 1)]['target'].array
        verb = g_nodes[g_nodes['id'].isin(root) & (g_nodes['type'].isin(['verb', 'act'])) &
                       (g_nodes['sentence_id'] == sentence_id)]['id'].array
        noun = g_nodes[g_nodes['id'].isin(root) & g_nodes['type'].isin(['noun', 'phrase', 'concepts', 'concept']) &
                       (g_nodes['sentence_id'] == sentence_id) & (g_nodes['state'] == 1)]['id'].array
        new_vid = len(g_nodes)
        new_eid = len(g_edges)
        added_e = []
        added_e_idx = 0
        comp_min = [num_d_min]
        comp_max = [num_d_max]
        if is_continue:
            if pointer_id != -1:
                added_e.append({'sentence_id': sentence_id, 'id': new_eid + added_e_idx,
                                'source': new_vid, 'target': pointer_id,
                                'rel_name': 'has_qualifier_type', 'state': 1, 'is_root_sentence': is_root_sentence})
                added_e_idx += 1
                comp_min.append(pointer_min)
                comp_max.append(pointer_max)
            if num_t_text != '':
                added_e.append({'sentence_id': sentence_id, 'id': new_eid + added_e_idx,
                                'source': new_vid, 'target': num_t,
                                'rel_name': 'has_qualifier_value', 'state': 1, 'is_root_sentence': is_root_sentence})
                added_e_idx += 1
                comp_min.append(num_t_min)
                comp_max.append(num_t_max)
            if unit_text != '':
                added_e.append({'sentence_id': sentence_id, 'id': new_eid + added_e_idx,
                                'source': new_vid, 'target': unit,
                                'rel_name': 'has_qualifier_unit', 'state': 1, 'is_root_sentence': is_root_sentence})
                added_e_idx += 1
                comp_min.append(unit_min)
                comp_max.append(unit_max)
            new_v = {'sentence_id': sentence_id, 'id': new_vid, 'text': 'NumeralConcept', 'type': 'concept',
                     'min': min(comp_min), 'max': max(comp_max), 'state': 1, 'is_root_sentence': is_root_sentence}
            g_nodes = g_nodes.append(new_v, ignore_index=True)
            added_e.append({'sentence_id': sentence_id, 'id': new_eid,
                            'source': new_vid, 'target': num_d,
                            'rel_name': 'has_qualifier_value', 'state': 1, 'is_root_sentence': is_root_sentence})
            added_e_idx += 1
            source = new_vid
            if len(verb) > 0:
                added_e.append({'sentence_id': sentence_id, 'id': new_eid + added_e_idx, 'source': verb[0],
                                'target': new_vid, 'rel_name': 'has_qualifier',
                                'state': 1, 'is_root_sentence': is_root_sentence})
                added_e_idx += 1
                source = verb[0]
            elif len(noun) > 0:
                noun_in = g_edges[(g_edges['target'] == noun[0]) &
                                  (g_edges['state'] == 1)]['rel_name'].array[0]
                new_v = {'sentence_id': sentence_id, 'id': new_vid + 1, 'text': 'Act', 'type': 'act',
                         'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1}
                new_v1 = {'sentence_id': sentence_id, 'id': new_vid + 2, 'text': 'memenuhi', 'type': 'verb',
                          'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1}
                g_nodes = g_nodes.append([new_v, new_v1], ignore_index=True)
                new_eid = len(g_edges)
                source = new_vid + 1
                if noun_in == 'root':
                    added_e.append({'sentence_id': sentence_id, 'id': new_eid + added_e_idx, 'source': new_vid + 1,
                                    'target': noun[0], 'rel_name': 'has_subject', 'state': 1,
                                    'is_root_sentence': is_root_sentence})
                    g_edges.loc[(g_edges['target'] == noun[0]) &
                                (g_edges['state'] == 1), 'target'] = new_vid + 1
                    verbs.append(new_vid + 2)
                else:
                    added_e.append({'sentence_id': sentence_id, 'id': new_eid + added_e_idx, 'source': noun[0],
                                    'target': new_vid + 1, 'rel_name': 'has_condition', 'state': 1,
                                    'is_root_sentence': is_root_sentence})
                added_e.append({'sentence_id': sentence_id, 'id': new_eid + added_e_idx + 1, 'source': new_vid + 1,
                                'target': new_vid + 2, 'rel_name': 'has_act_type', 'state': 1,
                                'is_root_sentence': is_root_sentence})
                added_e.append({'sentence_id': sentence_id, 'id': new_eid + added_e_idx + 2, 'source': new_vid + 1,
                                'target': new_vid, 'rel_name': 'has_qualifier', 'state': 1,
                                'is_root_sentence': is_root_sentence})
            g_edges.loc[g_edges['source'].isin([num_d, num_t, unit, new_vid]) &
                        (~g_edges['target'].isin([num_d, num_t, unit, new_vid])) &
                        (g_edges['state'] == 1) & (~g_edges['rel_name'].isin(['nummod', 'has_subject'])),
                        'source'] = source
            if len(pointer['id']) > 0:
                g_edges.loc[(g_edges['target'].isin([num_d, num_t, unit, pointer['id'][0]]) |
                             g_edges['source'].isin([num_d, num_t, unit, pointer['id'][0]])), 'state'] = 0
            else:
                g_edges.loc[(g_edges['target'].isin([num_d, num_t, unit]) |
                             g_edges['source'].isin([num_d, num_t, unit])), 'state'] = 0
            g_edges = g_edges.append(added_e, ignore_index=True)
        g_edges.loc[g_edges['source'] == g_edges['target'], 'state'] = 0
    return g_nodes, g_edges


def compound_finishing(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    words = g_edges[g_edges['rel_name'].isin(['amod', 'appos', 'advmod', 'compound', 'flat', 'xcomp', 'acl',
                                              'nummod', 'det']) & g_edges['state'] == 1]

    words = words.merge(g_nodes.add_suffix('_vsource'), left_on=['source'], right_on=['id_vsource'])
    words = words.merge(g_nodes.add_suffix('_vtarget'), left_on=['target'], right_on=['id_vtarget'])
    words['diff1'] = words['min_vsource'] - words['max_vtarget']
    words['diff2'] = words['min_vtarget'] - words['max_vsource']
    words['source_rank'] = words.groupby(['source'])['target'].transform('rank')
    words = words[(words['diff1'] == 1) | (words['diff2'] == 1)]
    words = words[(~words['source'].isin(words.target.array)) & (words['source_rank'] == 1)]
    while len(words) > 0:
        words['new_type'] = 'phrase'
        words.loc[(words['type_vsource'] == 'verb') | (words['type_vtarget'] == 'verb'), 'new_type'] = 'verb'
        words.loc[words['type_vsource'] == words['type_vtarget'], 'new_type'] = words['type_vsource']
        words['new_min'] = words[['min_vsource', 'min_vtarget']].min(axis=1)
        words['new_max'] = words[['max_vsource', 'max_vtarget']].max(axis=1)
        words['new_text'] = ''
        words.loc[words['diff2'] == 1,
                  'new_text'] = words.loc[words['diff2'] == 1,
                                          'text_vsource'].values + ' ' + words.loc[words['diff2'] == 1,
                                                                                   'text_vtarget']
        words.loc[words['diff1'] == 1,
                  'new_text'] = words.loc[words['diff1'] == 1,
                                          'text_vtarget'].values + ' ' + words.loc[words['diff1'] == 1,
                                                                                   'text_vsource']
        source = words[['source', 'new_min', 'new_max', 'new_text', 'new_type']].drop_duplicates().sort_values(
            ['source'])
        g_nodes = g_nodes.sort_values(['id'])
        g_nodes.loc[(g_nodes['id'].isin(source.source.array)),
                    ['min', 'max', 'text', 'type']] = source.loc[source['source'].isin(source.source.array),
                                                                 ['new_min', 'new_max', 'new_text',
                                                                  'new_type']].values
        target_out = words[['source', 'target', 'state']].merge(g_edges.add_suffix('_out'),
                                                                left_on=['target', 'state'],
                                                                right_on=['source_out', 'state_out'])
        target_out = target_out.sort_values(['id_out'])
        g_edges = g_edges.sort_values(['id'])
        g_edges.loc[g_edges.id.isin(target_out.id_out.array),
                    'source'] = target_out.loc[target_out['id_out'].isin(target_out.id_out.array),
                                               'source'].values
        g_edges.loc[g_edges.id.isin(words.id.array), 'state'] = 0
        g_nodes.loc[g_nodes.id.isin(words.target.array), 'state'] = 0
        words = g_edges[g_edges.rel_name.isin(['amod', 'appos', 'advmod', 'compound', 'flat', 'xcomp', 'acl',
                                               'nummod', 'det']) & g_edges.state == 1]
        words = words.merge(g_nodes.add_suffix('_vsource'), left_on='source', right_on='id_vsource')
        words = words.merge(g_nodes.add_suffix('_vtarget'), left_on='target', right_on='id_vtarget')
        words = words[(words['min_vsource'] == words['max_vtarget'] + 1) |
                      (words['min_vtarget'] == words['max_vsource'] + 1)]
        words['diff1'] = words['min_vsource'] - words['max_vtarget']
        words['diff2'] = words['min_vtarget'] - words['max_vsource']
        words['source_rank'] = words.groupby(['source'])['target'].transform('rank')
        words = words[(words['diff1'] == 1) | (words['diff2'] == 1)]
        words = words[(~words['source'].isin(words.target.array)) & (words['source_rank'] == 1)]
    g_edges.loc[(g_edges['rel_name'].isin(['compound', 'flat'])) & (g_edges['state'] == 1), 'rel_name'] = 'untuk'
    return g_nodes, g_edges


def condition_finishing(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    conditions = g_edges[g_edges['rel_name'] == 'has_condition']
    excl_exceptions = g_nodes[(~g_nodes['text'].isin(['root', 'concepts'])) & (g_nodes['type'] != 'verb')]
    conditions = conditions.merge(excl_exceptions, left_on=['source', 'sentence_id'], right_on=['id', 'sentence_id'],
                                  suffixes=['', '_v'])
    new_vid = len(g_nodes)
    new_eid = len(g_edges)
    added_e = []
    added_v = []

    for condition in conditions.iterrows():
        source = condition[1]['source']
        sentence_id = condition[1]['sentence_id']
        is_root_sentence = condition[1]['is_root_sentence']
        added_v.append({'sentence_id': sentence_id, 'id': new_vid, 'text': 'Concept', 'type': 'concept', 'min': -1,
                        'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': new_vid, 'target': source,
                        'rel_name': 'has_subject', 'state': 1, 'is_root_sentence': is_root_sentence})
        g_edges.loc[(g_edges['target'] == source) &
                    (g_edges['state'] == 1), 'target'] = new_vid
        g_edges.loc[(g_edges['source'] == source) &
                    (~g_edges['rel_name'].str.contains('(concept|act|refer)_(and|xor|or|uconj)')) &
                    (g_edges['state'] == 1), 'source'] = new_vid
        new_vid += 1
        new_eid += 1
    g_nodes = g_nodes.append(added_v, ignore_index=True)
    g_edges = g_edges.append(added_e, ignore_index=True)
    return g_nodes, g_edges


def refer_to_finishing(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    reg_refers = g_edges[(g_edges.rel_name == 'refer_to') & (g_edges['state'] == 1)]
    excl_exceptions = g_nodes[(~g_nodes.type.isin(['verb', 'concept']))]
    reg_refers = reg_refers.merge(excl_exceptions, left_on='source', right_on='id', suffixes=['', '_v'])
    reg_refers['rank'] = reg_refers['id'].rank(method='first') - 1
    new_vid = len(g_nodes)
    new_eid = len(g_edges)
    reg_refers['new_vid'] = reg_refers['rank'] + new_vid
    reg_refers['new_eid'] = reg_refers['rank'] + new_eid
    refers_sin = reg_refers.merge(g_edges, left_on=['source', 'state'], right_on=['target', 'state'],
                                  suffixes=['', '_source_in'])
    refers_sout = reg_refers.merge(g_edges, left_on=['source', 'state'], right_on=['source', 'state'],
                                   suffixes=['', '_source_out'])
    refers_sout = refers_sout[(~refers_sout['rel_name'].str.contains('concept_', regex=True))]
    added_e = []
    added_v = []
    for refer in reg_refers.iterrows():
        source = refer[1]['source']
        sentence_id = refer[1]['sentence_id']
        is_root_sentence = refer[1]['is_root_sentence']
        new_vid = refer[1]['new_vid']
        new_eid = refer[1]['new_eid']
        added_v.append({'sentence_id': sentence_id, 'id': new_vid, 'text': 'Concept', 'type': 'concept', 'min': -1,
                        'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': new_vid, 'target': source,
                        'rel_name': 'has_subject', 'state': 1, 'is_root_sentence': is_root_sentence})
    g_edges = g_edges.sort_values(['id'])
    refers_sin = refers_sin.sort_values(['id'])
    refers_sout = refers_sout.sort_values(['id'])
    try:
        g_edges.loc[g_edges.id.isin(refers_sin.id_source_in.array),
                    'target'] = refers_sin.loc[refers_sin['id_source_in'].isin(refers_sin.id_source_in.array),
                                               'new_vid'].values
    except ValueError:
        drop_articles = error_handling(g_edges.loc[g_edges.id.isin(refers_sin.id_source_in.array)],
                                       refers_sin.loc[refers_sin['id_source_in'].isin(refers_sin.id_source_in.array)],
                                       ['id'], ['id_source_in'])
        refers_sin = refers_sin[~refers_sin['sentence_id'].isin(drop_articles)]
        g_edges.loc[g_edges['sentence_id'].isin(drop_articles), 'state'] = 0
        g_edges.loc[g_edges.id.isin(refers_sin.id_source_in.array),
                    'target'] = refers_sin.loc[refers_sin['id_source_in'].isin(refers_sin.id_source_in.array),
                                               'new_vid'].values
    refers_sout = refers_sout.sort_values('id_source_out')
    try:
        g_edges.loc[g_edges.id.isin(refers_sout.id_source_out.array),
                    'source'] = refers_sout.loc[refers_sout['id_source_out'].isin(refers_sout.id_source_out.array),
                                                'new_vid'].values
    except ValueError:
        drop_articles = error_handling(g_edges.loc[g_edges.id.isin(refers_sout.id_source_out.array)],
                                       refers_sout.loc[
                                           refers_sout['id_source_out'].isin(refers_sout.id_source_out.array)],
                                       ['id'], ['id_source_out'])
        refers_sout = refers_sout[~refers_sout['sentence_id'].isin(drop_articles)]
        g_edges.loc[g_edges['sentence_id'].isin(drop_articles), 'state'] = 0
        g_edges.loc[g_edges.id.isin(refers_sout.id_source_out.array),
                    'source'] = refers_sout.loc[refers_sout['id_source_out'].isin(refers_sout.id_source_out.array),
                                                'new_vid'].values
    g_nodes = g_nodes.append(added_v, ignore_index=True)
    g_edges = g_edges.append(added_e, ignore_index=True)
    return g_nodes, g_edges


def conj_out_finishing(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    conjs = g_nodes[g_nodes.text == 'Concepts'].id.array
    conjs = g_edges[g_edges.source.isin(conjs) &
                    g_edges.rel_name.str.contains('concept_', regex=True) &
                    (g_edges.state == 1)].copy()
    conj_out = conjs.merge(g_edges, left_on=['target', 'state'], right_on=['source', 'state'],
                           suffixes=['', '_out'])
    conj_out = conj_out[(~conj_out['rel_name_out'].str.contains(
        'compound|flat|det|appos|amod|refer_to|condition|mark|nummod|subject|concept_',
        regex=True))]
    conjs = conjs[conjs['id'].isin(conj_out.id.array)]
    conjs = conjs.sort_values(['target'])
    conj_out = conj_out.sort_values(['target'])
    new_vid = len(g_nodes)
    new_eid = len(g_edges)
    conjs['rank'] = conjs['target'].rank(method='dense') - 1
    conj_out['rank'] = conj_out['target'].rank(method='dense') - 1
    conjs['new_vid'] = conjs['rank'] * 3 + new_vid
    conjs['new_eid'] = conjs['rank'] * 3 + new_eid
    conj_out['new_vid'] = conj_out['rank'] * 3 + new_vid
    conj_out['new_eid'] = conj_out['rank'] * 3 + new_eid
    added_v = []
    added_e = []
    new_conjs = conjs[['sentence_id', 'new_vid', 'new_eid', 'target', 'is_root_sentence']].drop_duplicates()
    for conj in new_conjs.iterrows():
        sentence_id = conj[1]['sentence_id']
        new_vid = conj[1]['new_vid']
        new_eid = conj[1]['new_eid']
        conj_id = conj[1]['target']
        is_root_sentence = conj[1]['is_root_sentence']
        added_v.append({'sentence_id': sentence_id, 'id': new_vid, 'text': 'Concept', 'type': 'concept',
                        'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_v.append({'sentence_id': sentence_id, 'id': new_vid + 1, 'text': 'Act', 'type': 'act',
                        'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_v.append({'sentence_id': sentence_id, 'id': new_vid + 2, 'text': 'berlaku',
                        'type': 'verb', 'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': new_vid,
                        'target': new_vid + 1, 'rel_name': 'has_condition', 'state': 1,
                        'is_root_sentence': is_root_sentence})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid + 1, 'source': new_vid + 1,
                        'target': new_vid + 2, 'rel_name': 'has_act_type', 'state': 1,
                        'is_root_sentence': is_root_sentence})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid + 2, 'source': new_vid,
                        'target': conj_id, 'rel_name': 'has_subject', 'state': 1,
                        'is_root_sentence': is_root_sentence})
    g_edges = g_edges.sort_values(['id'])
    conjs = conjs.sort_values(['id'])
    conj_out = conj_out.sort_values(['id_out'])
    g_edges.loc[(g_edges['id'].isin(conjs.id.array)),
                'target'] = conjs.loc[conjs['id'].isin(conjs.id.array), 'new_vid'].values
    try:
        g_edges.loc[g_edges['id'].isin(conj_out.id_out.array),
                    'source'] = conj_out.loc[conj_out['id'].isin(conj_out.id.array), 'new_vid'].values + 1
    except ValueError:
        drop_articles = error_handling(g_edges.loc[g_edges['id'].isin(conj_out.id_out.array)],
                                       conj_out.loc[conj_out['id'].isin(conj_out.id.array)],
                                       ['id'], ['id'])
        conj_out = conj_out[~conj_out['sentence_id'].isin(drop_articles)]
        g_edges.loc[g_edges['sentence_id'].isin(drop_articles), 'state'] = 0
        g_edges.loc[g_edges['id'].isin(conj_out.id_out.array),
                    'source'] = conj_out.loc[conj_out['id'].isin(conj_out.id.array), 'new_vid'].values + 1
    g_nodes = g_nodes.append(added_v, ignore_index=True)
    g_edges = g_edges.append(added_e, ignore_index=True)
    return g_nodes, g_edges


def invalid_rels_refinement(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    g_nodes.loc[g_nodes['text'] == 'yang', 'state'] = 0
    null_state = g_nodes[g_nodes['state'] == 0]['id'].values
    g_edges.loc[g_edges['source'].isin(null_state) | g_edges['target'].isin(null_state), 'state'] = 0
    pada_saat = g_edges[(g_edges['rel_name'] == 'pada saat') & (g_edges['state'] == 1)]
    pada_saat = pada_saat.merge(g_edges.add_suffix('_in'), left_on=['source', 'state'],
                                right_on=['target_in', 'state_in'])
    pada_saat = pada_saat[~pada_saat['rel_name_in'].str.contains('has_act_type', regex=True)]
    pada_saat['rank'] = pada_saat['id'].rank(method='dense') - 1
    pada_saat['new_vid'] = len(g_nodes) + pada_saat['rank']
    pada_saat['new_eid'] = len(g_edges) + pada_saat['rank']
    pada_saat['new_rel_name'] = 'has_subject'
    pada_saat = pada_saat.sort_values('id_in')
    try:
        g_edges.loc[g_edges['id'].isin(pada_saat.id_in.array),
                    ['target', 'rel_name']] = g_edges.loc[g_edges['id'].isin(pada_saat.id_in.array),
                                                          ['new_vid', 'rel_name']].values
    except Exception:
        drop_articles = error_handling(g_edges.loc[g_edges['id'].isin(pada_saat.id_in.array)],
                                       g_edges.loc[g_edges['id'].isin(pada_saat.id_in.array)], ['id'], ['id'])
        g_edges.loc[g_edges['sentence_id'].isin(drop_articles), 'state'] = 0
    pada_saat = pada_saat[['id', 'source', 'is_root_sentence', 'sentence_id', 'rel_name', 'new_vid', 'new_eid',
                           'new_rel_name']].drop_duplicates().sort_values('id')
    g_edges.loc[g_edges['id'].isin(pada_saat.id.array),
                ['source', 'rel_name']] = pada_saat.loc[pada_saat['id'].isin(pada_saat.id.array),
                                                        ['new_vid', 'new_rel_name']].values
    added_v = []
    added_e = []
    for when in pada_saat.iterrows():
        new_vid = when[1]['new_vid']
        new_eid = when[1]['new_eid']
        sentence_id = when[1]['sentence_id']
        is_root_sentence = when[1]['is_root_sentence']
        target = when[1]['source']
        added_v.append({'sentence_id': sentence_id, 'id': new_vid, 'text': 'Act', 'type': 'act',
                        'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': new_vid,
                        'target': target, 'rel_name': 'has_act_type', 'state': 1,
                        'is_root_sentence': is_root_sentence})
    g_nodes = g_nodes.append(added_v, ignore_index=True)
    g_edges = g_edges.append(added_e, ignore_index=True)
    g_edges.loc[g_edges['rel_name'].isin(['appos', 'amod']), 'rel_name'] = 'undefined'
    g_edges.loc[g_edges['rel_name'].str.contains('nsubj', regex=True), 'rel_name'] = 'has_subject'
    g_edges.loc[g_edges['rel_name'].str.contains('has_act_type'), 'rel_name'] = 'has_act_type'
    acts_node = g_nodes[g_nodes['text'] == 'Acts']['id'].array
    concepts_node = g_nodes[g_nodes['text'] == 'Concepts']['id'].array
    refers_node = g_nodes[g_nodes['text'] == 'Refers']['id'].array
    numeral_node = g_nodes[g_nodes['text'] == 'NumeralConcept']['id'].array
    concept_node = g_nodes[g_nodes['text'] == 'Concept']['id'].array
    act_root_node = g_nodes[g_nodes['text'].isin(['Act', 'root'])]['id'].array
    invalid_source = g_edges[(~((g_edges['source'].isin(acts_node) &
                                 g_edges['rel_name'].isin(['act_and', 'act_or', 'act_xor', 'act_uconj'])) |
                                (g_edges['source'].isin(concepts_node) &
                                 g_edges['rel_name'].isin(['concept_and', 'concept_or', 'concept_xor',
                                                           'concept_uconj', 'refer_to'])) |
                                (g_edges['source'].isin(refers_node) &
                                 g_edges['rel_name'].str.contains('^refer_(and|or|xor|start|uconj|end)$',
                                                                  regex=True)) |
                                (g_edges['source'].isin(numeral_node) &
                                 g_edges['rel_name'].isin(
                                     ['has_qualifier_type', 'has_qualifier_value', 'has_qualifier_unit'])) |
                                (g_edges['source'].isin(concept_node) &
                                 g_edges['rel_name'].isin(['has_subject', 'has_condition', 'refer_to'])) |
                                g_edges['rel_name'].isin(['undefined', 'conj']) |
                                g_edges['source'].isin(act_root_node)) &
                              (g_edges['state'] == 1))]
    invalid_source = invalid_source.merge(g_edges, left_on=['source', 'sentence_id', 'state'],
                                          right_on=['target', 'sentence_id', 'state'], suffixes=['', '_sin'])
    temp = invalid_source
    while len(invalid_source) > 0:
        invalid_source = invalid_source.sort_values(['id'])
        g_edges = g_edges.sort_values(['id'])
        invalid_source = invalid_source[['sentence_id', 'id', 'source_sin']].drop_duplicates()
        try:
            g_edges.loc[g_edges['id'].isin(invalid_source.id.array),
                        'source'] = invalid_source.loc[invalid_source['id'].isin(invalid_source.id.array),
                                                       'source_sin'].values
        except ValueError:
            drop_articles = error_handling(g_edges.loc[g_edges['id'].isin(invalid_source.id.array)],
                                           invalid_source.loc[invalid_source['id'].isin(invalid_source.id.array)],
                                           ['id'], ['id'])
            invalid_source = invalid_source[~invalid_source['sentence_id'].isin(drop_articles)]
            g_edges.loc[g_edges['sentence_id'].isin(drop_articles), 'state'] = 0
            g_edges.loc[g_edges['id'].isin(invalid_source.id.array),
                        'source'] = invalid_source.loc[invalid_source['id'].isin(invalid_source.id.array),
                                                       'source_sin'].values
        remove_sentence = g_edges.loc[(g_edges['source'] == g_edges['target']) &
                                      (g_edges['state'] == 1), 'sentence_id'].unique()
        g_edges.loc[g_edges['sentence_id'].isin(remove_sentence), 'state'] = 0
        acts_node = g_nodes[g_nodes['text'] == 'Acts']['id'].array
        concepts_node = g_nodes[g_nodes['text'] == 'Concepts']['id'].array
        refers_node = g_nodes[g_nodes['text'] == 'Refers']['id'].array
        numeral_node = g_nodes[g_nodes['text'] == 'NumeralConcept']['id'].array
        concept_node = g_nodes[g_nodes['text'] == 'Concept']['id'].array
        act_root_node = g_nodes[g_nodes['text'].isin(['Act', 'root'])]['id'].array
        invalid_source = g_edges[(~((g_edges['source'].isin(acts_node) &
                                     g_edges['rel_name'].isin(['act_and', 'act_or', 'act_xor', 'act_uconj'])) |
                                    (g_edges['source'].isin(concepts_node) &
                                     g_edges['rel_name'].isin(['concept_and', 'concept_or', 'concept_xor',
                                                               'concept_uconj', 'refer_to'])) |
                                    (g_edges['source'].isin(refers_node) &
                                     g_edges['rel_name'].str.contains('^refer_(and|or|xor|start|uconj|end)$',
                                                                      regex=True)) |
                                    (g_edges['source'].isin(numeral_node) &
                                     g_edges['rel_name'].isin(
                                         ['has_qualifier_type', 'has_qualifier_value', 'has_qualifier_unit'])) |
                                    (g_edges['source'].isin(concept_node) &
                                     g_edges['rel_name'].isin(['has_subject', 'has_condition', 'refer_to'])) |
                                    g_edges['rel_name'].isin(['undefined', 'conj']) |
                                    g_edges['source'].isin(act_root_node)) &
                                  (g_edges['state'] == 1))]

        invalid_source = invalid_source.merge(g_edges, left_on=['source', 'sentence_id', 'state'],
                                              right_on=['target', 'sentence_id', 'state'], suffixes=['', '_sin'])
        if set(temp['id'].array) - set(invalid_source['id'].array) == set():
            g_edges.loc[g_edges['sentence_id'].isin(invalid_source.sentence_id.array), 'state'] = 0
            break
        temp = invalid_source
    g_edges.loc[g_edges['rel_name'].isin(['advcl', 'parataxis']) &
                (g_edges['state'] == 1), 'rel_name'] = 'undefined'
    return g_nodes, g_edges


def postag_finishing(graph):
    g_nodes = graph['V']
    g_edges = graph['E']
    n_edges = len(graph['E'])
    n_nodes = len(graph['V'])
    temp_edges = g_edges[g_edges['state'] == 1].merge(g_nodes.add_suffix('_source'), left_on=['source', 'sentence_id'],
                                                      right_on=['id_source', 'sentence_id_source'])
    temp_edges = temp_edges.merge(g_nodes.add_suffix('_target'), left_on=['target', 'sentence_id'],
                                  right_on=['id_target', 'sentence_id_target'])
    temp_edges = temp_edges[
        ['sentence_id', 'id', 'source', 'target', 'rel_name', 'type_source', 'type_target', 'state']].drop_duplicates()
    temp_edges = temp_edges[(~temp_edges['rel_name'].isin(
        ['act_and', 'act_or', 'act_xor', 'act_uconj', 'has_act_type', 'concept_and', 'concept_or', 'concept_xor',
         'concept_uconj', 'has_condition', 'has_modality', 'has_object', 'has_qualifier', 'has_qualifier_type',
         'has_qualifier_value', 'has_qualifier_unit', 'refer_and', 'refer_or', 'refer_xor', 'refer_uconj',
         'refer_start', 'refer_end', 'refer_to', 'has_subject', 'root']))]
    new_node = temp_edges[['sentence_id', 'rel_name', 'state']].drop_duplicates()
    new_node['temp'] = new_node['rel_name'].rank(method='min')
    new_node['id'] = new_node['sentence_id'].rank(method='min') - 1 + new_node.groupby('sentence_id')['temp'].rank(
        method='min') - 1 + n_nodes
    new_node[['text', 'type', 'min', 'max', 'is_root_sentence']] = [new_node['rel_name'], 'has_qualifier', '-1', '-1',
                                                                    False]
    new_node = new_node[['sentence_id', 'id', 'text', 'type', 'min', 'max', 'is_root_sentence', 'state']]
    new_edge = temp_edges[temp_edges['type_target'].isin(['concept'])]
    new_edge[['state', 'is_root_sentence']] = [1, False]
    new_edge['temp'] = new_edge['id'].rank() + n_edges - 1
    new_edge = new_edge.merge(new_node.add_suffix('_target'), left_on=['sentence_id', 'rel_name'],
                              right_on=['sentence_id_target', 'text_target'])
    new_edge[['id', 'source', 'target', 'rel_name']] = [new_edge['temp'], new_edge['target'], new_edge['id_target'],
                                                        'has_qualifier_type']
    new_edge = new_edge[['sentence_id', 'id', 'source', 'target', 'rel_name', 'state', 'is_root_sentence']]
    n_nodes += len(new_node)
    n_edges += len(new_edge)
    new_node_1 = temp_edges[~temp_edges['type_target'].isin(['concept'])]
    new_node_1[['state', 'is_root_sentence']] = [1, False]
    new_node_1['temp'] = new_node_1['id'].rank(method='min') + n_nodes - 1
    new_node_1[['id', 'text', 'type', 'min', 'max', 'is_root_sentence', 'state', 'rank']] = [new_node_1['temp'],
                                                                                             'Concept', 'concept', '-1',
                                                                                             '-1', False, 1, new_node_1[
                                                                                                 'temp'] - n_nodes]
    new_node_1 = new_node_1[['sentence_id', 'id', 'text', 'type', 'min', 'max', 'is_root_sentence', 'state', 'rank']]
    new_edge_1 = temp_edges[~temp_edges['type_target'].isin(['concept'])]
    new_edge_1[['state', 'is_root_sentence']] = [1, False]
    new_edge_1['temp'] = new_edge_1['id'].rank() + n_edges - 1
    new_edge_1['rank'] = new_edge_1['temp'] - n_edges
    new_edge_1 = new_edge_1.merge(new_node_1.add_suffix('_source'), left_on=['sentence_id', 'rank'],
                                  right_on=['sentence_id_source', 'rank_source'])
    new_edge_1 = new_edge_1.merge(new_node.add_suffix('_target'), left_on=['sentence_id', 'rel_name'],
                                  right_on=['sentence_id_target', 'text_target'])
    new_edge_1[['id', 'source', 'target', 'rel_name']] = [new_edge_1['temp'], new_edge_1['id_source'],
                                                          new_edge_1['id_target'], 'has_qualifier_type']
    new_edge_1 = new_edge_1[['sentence_id', 'id', 'source', 'target', 'rel_name', 'state', 'is_root_sentence']]
    n_edges += len(new_edge_1)
    new_edge_2 = temp_edges[~temp_edges['type_target'].isin(['concept'])]
    repl_edge = temp_edges[temp_edges['type_target'].isin(['concept'])]['target'].values
    repl_edge = g_edges[
        g_edges['source'].isin(repl_edge) & (g_edges['rel_name'] == 'has_subject') & (g_edges['state'] == 1)]
    repl_edge = repl_edge.sort_values('id')
    new_edge_2[['state', 'is_root_sentence']] = [1, False]
    new_edge_2['temp'] = new_edge_2['id'].rank() + n_edges - 1
    new_edge_2['rank'] = new_edge_2['temp'] - n_edges
    new_edge_2 = new_edge_2.merge(new_node_1.add_suffix('_source'), left_on=['sentence_id', 'rank'],
                                  right_on=['sentence_id_source', 'rank_source'])
    g_edges = g_edges.sort_values('id')
    new_edge_2 = new_edge_2.sort_values('id')
    g_edges.loc[g_edges['id'].isin(new_edge_2['id']), 'target'] = new_edge_2.loc[
        new_edge_2['id'].isin(new_edge_2['id']), 'id_source'].values
    new_edge_2[['id', 'source', 'rel_name']] = [new_edge_2['temp'], new_edge_2['id_source'], 'has_qualifier_value']
    new_edge_2 = new_edge_2[['sentence_id', 'id', 'source', 'target', 'rel_name', 'state', 'is_root_sentence']]
    g_edges.loc[g_edges['id'].isin(temp_edges['id']), 'rel_name'] = 'has_qualifier'
    new_node_1 = new_node_1[['sentence_id', 'id', 'text', 'type', 'min', 'max', 'is_root_sentence', 'state']]
    g_nodes = pandas.concat([g_nodes, new_node, new_node_1], ignore_index=True).drop_duplicates().reset_index()
    g_edges = pandas.concat([g_edges, new_edge, new_edge_1, new_edge_2],
                            ignore_index=True).drop_duplicates().reset_index()
    g_edges.loc[g_edges['id'].isin(repl_edge['id']), 'rel_name'] = 'has_qualifier_value'

    conj_rel = g_edges[g_edges['rel_name'].str.contains('(?i)and|or|xor|uconj|start|end') & (g_edges['state'] == 1)]
    conj_node = g_nodes.merge(conj_rel, left_on=['sentence_id', 'id'], right_on=['sentence_id', 'source'])
    conj_node['text'] = conj_node['rel_name'].replace(to_replace='^.+_', value='', regex=True).str.title()
    conj_node['type'] = conj_node['text'].str.lower()
    conj_node['dup'] = conj_node.groupby('source')['text'].transform('nunique')
    conj_node['rank'] = 0
    conj_node.loc[conj_node['text'] == 'Or', 'rank'] = 1
    conj_node.loc[conj_node['text'] == 'Xor', 'rank'] = 2
    conj_node['min'] = conj_node.groupby('source')['rank'].transform('min')
    conj_node.loc[conj_node['min'] == 0, ['text', 'type']] = ['And', 'and']
    conj_node.loc[conj_node['min'] == 1, ['text', 'type']] = ['Or', 'or']
    conj_node.loc[conj_node['min'] == 2, ['text', 'type']] = ['Xor', 'xor']
    conj_node = conj_node[['source', 'text', 'type', 'dup']].drop_duplicates()
    conj_rel['rel_name'] = conj_rel['rel_name'].replace(to_replace='^.+_', value='', regex=True).str.title()
    conj_rel.loc[conj_rel['rel_name'].isin(['Uconj']), 'rel_name'] = 'And'
    g_nodes.loc[g_nodes['id'].isin(conj_node['source']), ['text', 'type']] = conj_node.loc[
        conj_node['source'].isin(conj_node['source']), ['text', 'type']].values
    g_edges.loc[g_edges['id'].isin(conj_rel['id']), 'rel_name'] = conj_rel.loc[
        conj_rel['id'].isin(conj_rel['id']), 'rel_name'].values
    roots = g_nodes[(g_nodes['text'] == 'root') & (g_nodes['is_root_sentence'])]['id'].array
    oroot = g_edges[(g_edges['source'].isin(roots)) & (g_edges['rel_name'] != 'root') & (g_edges['state'] == 1)]
    eroot = g_edges[(g_edges['source'].isin(oroot.source.array)) & (g_edges['rel_name'] == 'root') &
                    (g_edges['state'] == 1)]
    eroot = pandas.concat([eroot, oroot])
    eroot = eroot.merge(g_nodes.add_suffix('_target'), left_on='target', right_on='id_target')
    eroot = eroot[['sentence_id', 'id', 'source', 'target', 'rel_name', 'state', 'is_root_sentence',
                   'sentence_id_target', 'id_target', 'text_target', 'type_target']]
    eroot['new_rel_name'] = eroot['rel_name']
    eroot.loc[eroot['rel_name'] == 'nsubj', 'new_rel_name'] = 'has_subject'
    eroot[['is_any_verb', 'is_any_subject']] = 0
    eroot.loc[eroot['type_target'] == 'verb', 'is_any_verb'] = 1
    eroot.loc[eroot['rel_name'] == 'nsubj', 'is_any_subject'] = 1
    eroot['is_any_verb'] = eroot.groupby(['sentence_id'])['is_any_verb'].transform(max)
    eroot['is_any_subject'] = eroot.groupby(['sentence_id'])['is_any_subject'].transform(max)
    eroot.loc[(eroot['is_any_subject'] == 1) & (eroot['rel_name'] == 'root'), 'new_rel_name'] = 'has_object'
    eroot.loc[(eroot['is_any_subject'] != 1) & (eroot['rel_name'] == 'root'), 'new_rel_name'] = 'has_subject'
    eroot.loc[(eroot['type_target'] == 'verb'), 'new_rel_name'] = 'has_act_type'
    eroot['root_rank'] = eroot['source'].rank(method='dense')
    eroot['new_vid'] = len(g_nodes) + eroot['root_rank'] - 1
    eroot['new_eid'] = len(g_edges) + eroot['root_rank'] - 1
    added_v = []
    added_e = []
    new_root = eroot[['sentence_id', 'source', 'new_vid', 'new_eid', 'is_root_sentence']].drop_duplicates()
    for root in new_root.iterrows():
        source = root[1]['source']
        new_vid = root[1]['new_vid']
        new_eid = root[1]['new_eid']
        sentence_id = root[1]['sentence_id']
        is_root_sentence = root[1]['is_root_sentence']
        added_v.append({'sentence_id': sentence_id, 'id': new_vid, 'text': 'Act', 'type': 'act',
                        'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': source,
                        'target': new_vid, 'rel_name': 'root', 'state': 1,
                        'is_root_sentence': is_root_sentence})
    no_verb = eroot[eroot['is_any_verb'] == 0][['sentence_id', 'source', 'new_vid',
                                                'is_root_sentence']].drop_duplicates()
    no_verb['root_rank'] = no_verb['source'].rank(method='dense')
    no_verb['new_vid2'] = len(g_nodes) + no_verb['root_rank'] + len(new_root) - 1
    no_verb['new_eid'] = len(g_edges) + no_verb['root_rank'] + len(new_root) - 1
    for verb in no_verb.iterrows():
        source = verb[1]['new_vid']
        new_vid = verb[1]['new_vid2']
        new_eid = verb[1]['new_eid']
        sentence_id = verb[1]['sentence_id']
        is_root_sentence = verb[1]['is_root_sentence']
        added_v.append({'sentence_id': sentence_id, 'id': new_vid, 'text': 'berlaku', 'type': 'verb',
                        'min': -1, 'max': -1, 'is_root_sentence': is_root_sentence, 'state': 1})
        added_e.append({'sentence_id': sentence_id, 'id': new_eid, 'source': source,
                        'target': new_vid, 'rel_name': 'has_act_type', 'state': 1,
                        'is_root_sentence': is_root_sentence})
    eroot = eroot.sort_values(['id'])
    g_edges.loc[g_edges['id'].isin(eroot.id.array),
                ['source', 'rel_name']] = eroot.loc[eroot['id'].isin(eroot.id.array),
                                                    ['new_vid', 'new_rel_name']].values
    g_nodes = g_nodes.append(added_v, ignore_index=True)
    g_edges = g_edges.append(added_e, ignore_index=True)
    g_edges['rel_name'] = g_edges['rel_name'].str.lower()
    g_edges['rel_first'] = g_edges['rel_name'].str.extract(r'(^[a-z]+)')
    g_edges['rel_last'] = g_edges['rel_name'].replace(to_replace=r'^[a-z]+', value='', regex=True)
    g_edges['rel_name'] = g_edges['rel_first'] + g_edges['rel_last'].str.title()
    g_edges['rel_name'] = schema + g_edges['rel_name'].replace(to_replace=r'[\W\s_]', value='', regex=True)
    g_nodes['uri'] = g_nodes['text']
    g_nodes.loc[~g_nodes['uri'].str.contains('[A-Z]{2,}', regex=True),
                'uri'] = g_nodes.loc[~g_nodes['uri'].str.contains('[A-Z]{2,}', regex=True), 'uri'].str.title()
    g_edges['rel_name'] = g_edges['rel_name'].replace(to_replace=r'\s+', value='_', regex=True)
    g_nodes['uri'] = g_nodes['uri'].replace(to_replace=r'\W+', value='_', regex=True)
    g_nodes.loc[
        ~(g_nodes['uri'].str.contains(r'(?i)(concept|lx|cx|rx|v\d+|pasal|huruf|act|:(and|or|xor))', regex=True) |
          (g_nodes['uri'] == '_')),
        'uri'] = data + 'Concept_' + g_nodes.loc[
        ~(g_nodes['uri'].str.contains(r'(?i)(concept|lx|cx|rx|v\d+|pasal|huruf|act|:(and|or|xor))',
                                      regex=True) |
          (g_nodes['uri'] == '_')), 'uri'].values
    g_nodes['uri'] = g_nodes['uri'].replace(to_replace=r'lexid:Concept_$', value='', regex=True)
    g_nodes['uri'] = g_nodes['uri'].replace(to_replace=r'_+$', value='', regex=True)
    g_nodes['text'] = g_nodes['text'].replace(to_replace=r'\W+$', value='', regex=True)
    grules = g_nodes[g_nodes['type'].isin(['concept', 'act', 'and', 'or', 'xor']) & (g_nodes['state'] == 1)]
    grules['rank'] = grules.groupby(['sentence_id', 'type'])['id'].rank(method='dense')
    grules = grules.sort_values(['sentence_id'])
    grules['rank'] = grules['text'].str.title() + '_' + grules['rank'].apply(int).apply(str) + '_' + grules[
        'sentence_id']
    grules['rank'] = data + grules['rank']
    grules['uri'] = grules['rank']
    grules = grules.drop(columns=['rank']).sort_values(['id'])
    g_nodes = g_nodes.sort_values(['id'])
    g_nodes.loc[g_nodes['id'].isin(grules.id.array) & (g_nodes['state'] == 1), 'uri'] = grules.loc[
        grules['id'].isin(grules.id.array), 'uri'].values
    g_nodes.loc[~(g_nodes['id'].isin(g_edges.source.array) | g_nodes['id'].isin(g_edges.target.array)), 'state'] = 0
    vroot = g_nodes[g_nodes['text'] == 'root'].sort_values(['id'])
    vroot = vroot.merge(g_edges.add_suffix('_out'), left_on=['id', 'state'], right_on=['source_out', 'state_out'])
    vroot = vroot[vroot['rel_name_out'] == schema + 'root']
    vroot = vroot.merge(g_nodes.add_suffix('_vout'), left_on=['target_out', 'state'],
                        right_on=['id_vout', 'state_vout'])
    vnorm = vroot[(~vroot['sentence_id'].str.contains(r'[A-Z]+$', regex=True)) |
                  (vroot['sentence_id'].str.contains(r'[A-Z]+$', regex=True) &
                   vroot['type_vout'].str.contains('act|and|or|xor', regex=True))]

    vlttr = vroot[vroot['sentence_id'].str.contains(r'[A-Z]+$', regex=True) &
                  ~vroot['type_vout'].str.contains('act', regex=True)]
    vconcept = vnorm.merge(g_edges.add_suffix('_atype'), left_on=['id_vout', 'state'],
                           right_on=['source_atype', 'state_atype'])
    vconcept = vconcept[
        ['sentence_id', 'id', 'text', 'type', 'min', 'max', 'is_root_sentence', 'state', 'uri', 'sentence_id_out',
         'id_out', 'source_out', 'target_out', 'rel_name_out', 'state_out', 'is_root_sentence_out', 'rel_first_out',
         'rel_last_out', 'sentence_id_vout', 'id_vout', 'text_vout', 'type_vout', 'min_vout', 'max_vout',
         'is_root_sentence_vout', 'state_vout', 'uri_vout', 'sentence_id_atype', 'id_atype', 'source_atype',
         'target_atype', 'rel_name_atype', 'state_atype', 'is_root_sentence_atype', 'rel_first_atype',
         'rel_last_atype']]
    csubj = vconcept[vconcept['rel_name_atype'] == schema + 'hasSubject']
    cobj = vconcept[vconcept['rel_name_atype'] == schema + 'hasObject']
    vconcept = vconcept[
        (~vconcept.isnull().any(axis=1)) &
        vconcept['rel_name_atype'].str.contains('(?i)' + schema + '(hasActType|And|Or|Uconj)')]
    vconcept = vconcept.merge(g_nodes.add_suffix('_vatype'), left_on='target_atype',
                              right_on='id_vatype')
    vnorm = vconcept[~vconcept['text_vatype'].str.contains('(?i)^merupakan$', regex=True)]
    vconcept = vconcept[vconcept['text_vatype'].str.contains('(?i)^merupakan$', regex=True)]
    vconcept = vconcept.merge(csubj.add_suffix('_subj'), left_on='id_vout', right_on='id_vout_subj')
    vconcept = vconcept.merge(cobj.add_suffix('_obj'), left_on='id_vout', right_on='id_vout_obj')
    e_cobj = vconcept[['sentence_id', 'state', 'id', 'id_out', 'id_vout', 'id_atype', 'target_atype', 'id_atype_subj',
                       'id_atype_obj', 'target_atype_subj',
                       'target_atype_obj']].drop_duplicates().sort_values(['id_atype_obj'])
    e_cobj['new_rel_name'] = schema + 'hasSubject'
    e_cobj.sort_values(['id_atype_obj'])
    g_edges = g_edges.sort_values(['id'])
    e_cobj_out = e_cobj.merge(g_edges.add_suffix('_objout'), left_on=['target_atype_obj', 'state'],
                              right_on=['source_objout', 'state_objout']).sort_values(['id_objout'])
    try:
        g_edges.loc[g_edges['id'].isin(e_cobj.id_atype_obj.array),
                    ['rel_name', 'source']] = e_cobj.loc[e_cobj['id_atype_obj'].isin(e_cobj.id_atype_obj.array),
                                                         ['new_rel_name', 'target_atype_subj']].values
    except ValueError:
        drop_articles = error_handling(g_edges.loc[g_edges['id'].isin(e_cobj.id_atype_obj.array)],
                                       e_cobj.loc[e_cobj['id_atype_obj'].isin(e_cobj.id_atype_obj.array)],
                                       ['id'], ['id_atype_obj'])
        e_cobj = e_cobj[~e_cobj['sentence_id'].isin(drop_articles)]
        g_edges.loc[g_edges['sentence_id'].isin(drop_articles), 'state'] = 0
        g_edges.loc[g_edges['id'].isin(e_cobj.id_atype_obj.array),
                    ['rel_name', 'source']] = e_cobj.loc[e_cobj['id_atype_obj'].isin(e_cobj.id_atype_obj.array),
                                                         ['new_rel_name', 'target_atype_subj']].values
    g_nodes.loc[g_nodes['id'].isin(e_cobj.target_atype_subj.array), 'type'] = 'root'
    e_cobj_out.sort_values(['id_objout'])
    g_edges.loc[g_edges['id'].isin(e_cobj.id_out.array) |
                g_edges['id'].isin(e_cobj.id_atype.array) |
                g_edges['id'].isin(e_cobj.id_atype_subj.array), ['state']] = 0
    g_nodes.loc[g_nodes['id'].isin(e_cobj.id.array), 'state'] = 0
    act = e_cobj[['sentence_id', 'state', 'id_vout', 'target_atype_subj', 'target_atype_obj']].drop_duplicates()
    act = act.merge(g_edges.add_suffix('_atype_out'), left_on=['id_vout', 'state'],
                    right_on=['source_atype_out', 'state_atype_out'])
    act = act
    act = act.sort_values(['id_atype_out'])
    try:
        g_edges.loc[g_edges['id'].isin(act.id_atype_out.array),
                    'source'] = act.loc[act['id_atype_out'].isin(act.id_atype_out.array),
                                        'target_atype_obj'].values
    except ValueError:
        drop_article = error_handling(g_edges.loc[g_edges['id'].isin(act.id_atype_out.array)],
                                      act.loc[act['id_atype_out'].isin(act.id_atype_out.array)],
                                      ['id'], ['id_atype_out'])
        act = act[~act['sentence_id'].isin(drop_article)]
        g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
        g_edges.loc[g_edges['id'].isin(act.id_atype_out.array),
                    'source'] = act.loc[act['id_atype_out'].isin(act.id_atype_out.array),
                                        'target_atype_obj'].values
    g_nodes = g_nodes.sort_values(['id'])
    vnorm = vnorm[['sentence_id', 'state', 'id', 'id_out', 'text', 'type', 'uri', 'id_vout']].drop_duplicates()
    vnorm[['text', 'type']] = ['Norm', 'root']
    vnorm['uri'] = data + 'Norm_' + vnorm['sentence_id']
    vnorm_subj = vnorm.merge(g_edges.add_suffix('_nsubj'), left_on=['id_vout', 'state'],
                             right_on=['source_nsubj', 'state_nsubj']).dropna()
    vnorm_subj = vnorm_subj.merge(g_edges.add_suffix('_nsubj2'), left_on=['target_nsubj', 'state_nsubj'],
                                  right_on=['source_nsubj2', 'state_nsubj2'], how='left')
    vnorm_subj = vnorm_subj[
        (vnorm_subj['rel_name_nsubj'].str.contains(schema + 'has(Modality|Subject|ActType)', regex=True)) |
        ((vnorm_subj['rel_name_nsubj'].str.contains(schema + '(and|or|xor)', regex=True)) &
         (vnorm_subj['rel_name_nsubj2'].str.contains(schema + 'has(Modality|Subject|ActType)',
                                                     regex=True)))]
    vnorm = vnorm[['id', 'type', 'text', 'uri']].drop_duplicates()

    vnorm = vnorm.sort_values(['id'])
    g_nodes.loc[g_nodes['id'].isin(vnorm.id.array),
                ['type', 'text', 'uri']] = vnorm.loc[vnorm['id'].isin(vnorm.id.array), ['type', 'text', 'uri']].values
    vnorm_subj = vnorm_subj.sort_values(['id_out'])
    g_edges.loc[g_edges['id'].isin(vnorm_subj.id_out.array), ['rel_name']] = schema + 'hasAct'
    vnorm_subj = vnorm_subj[['sentence_id_nsubj', 'state', 'is_root_sentence_nsubj', 'rel_name_nsubj',
                             'id', 'target_nsubj']]
    vnorm_subj = vnorm_subj.drop_duplicates().rename(columns={'is_root_sentence_nsubj': 'is_root_sentence',
                                                              'target_nsubj': 'target', 'id': 'source',
                                                              'rel_name_nsubj': 'rel_name',
                                                              'sentence_id_nsubj': 'sentence_id'}, inplace=False)
    vnorm_subj['id'] = vnorm_subj['source'].rank(method='first') - 1 + len(g_edges)
    g_edges = pandas.concat([g_edges, vnorm_subj])
    vlttr[['text', 'type']] = ['Concept', 'root']
    vlttr['uri'] = data + 'Concept_0_' + vlttr['sentence_id']
    vlttr_out = vlttr['id_out'].array
    vlttr = vlttr[['sentence_id', 'id', 'type', 'text', 'uri']].drop_duplicates().sort_values(['id'])
    vlttr['stype'] = 'Article_'
    vlttr.loc[vlttr['uri'].str.contains(r'LX\d+[A-WYZ]*X\d+[A-WYZ]*'), 'stype'] = 'Section_'
    vlttr['uri'] = vlttr.apply(lambda x: x['uri'].replace('LX', x['stype']), axis=1)
    vlttr['uri'] = vlttr['uri'].replace(to_replace=r'[X_]V(?=\d)', value='_Number_', regex=True)
    vlttr['uri'] = vlttr['uri'].replace(to_replace='X(?=[A-Z])', value='_Letter_', regex=True)
    vlttr['uri'] = vlttr['uri'].replace(to_replace='X', value='_', regex=True)
    vlttr['uri'] = vlttr['uri'].replace(to_replace=r'Number_0(_\d+)?_Number', value='Number', regex=True)
    vlttr = vlttr.drop(columns=['stype'])
    vlttr = vlttr.sort_values(['id'])
    g_nodes.loc[g_nodes['id'].isin(vlttr.id.array),
                ['type', 'text', 'uri']] = vlttr.loc[vroot['id'].isin(vlttr.id.array), ['type', 'text', 'uri']].values
    # vlttr = vlttr.sort_values(['id_out'])
    g_edges.loc[g_edges['id'].isin(vlttr_out), ['rel_name']] = schema + 'hasSubject'
    lnode = g_nodes[g_nodes['uri'].str.contains('LX')].sort_values(['id'])
    lnode['stype'] = 'Article_'
    lnode.loc[lnode['uri'].str.contains(r'LX\d+[A-WYZ]*X\d+[A-WYZ]*'), 'stype'] = 'Section_'
    lnode['uri'] = lnode.apply(lambda x: x['uri'].replace('LX', x['stype']), axis=1)
    lnode['uri'] = lnode['uri'].replace(to_replace=r'[X_]V(?=\d)', value='_Number_', regex=True)
    lnode['uri'] = lnode['uri'].replace(to_replace='X(?=[A-Z])', value='_Letter_', regex=True)
    lnode['uri'] = lnode['uri'].replace(to_replace='X', value='_', regex=True)
    lnode['uri'] = data + lnode['sentence_id'].replace(to_replace=r'(?i)(Article|Section).+', value='', regex=True) + \
                   lnode['uri'].replace(to_replace=r'^.*((?=Article)|(?=Section))', value='', regex=True) + lnode[
                       'suffix']
    lnode['uri'] = lnode['uri'].replace(to_replace=r'Number_0(_\d+)?_Number', value='Number', regex=True)
    lnode = lnode.drop(columns=['stype'])
    lnode = lnode.sort_values(['id'])
    g_nodes.loc[g_nodes['id'].isin(lnode.id.array),
                'uri'] = lnode.loc[lnode['id'].isin(lnode.id.array),
                                   'uri'].values
    vnode = g_nodes[g_nodes['text'].str.contains(r'^V\d+$', regex=True)]
    vnode['uri'] = data + vnode['sentence_id'].replace(to_replace=r'_V.+', value='', regex=True) + \
                   vnode['uri'].replace(to_replace=r'V', value='_Number_', regex=True)
    vnode['uri'] = vnode['uri'].replace(to_replace=r'Number_0(_\d+)?_Number', value='Number', regex=True)
    vnode = vnode.sort_values(['id'])
    g_nodes.loc[g_nodes['id'].isin(vnode.id.array),
                'uri'] = vnode.loc[vnode['id'].isin(vnode.id.array), 'uri'].values
    false_rel = g_edges.merge(g_nodes.add_suffix('_source'), left_on=['source', 'state'],
                              right_on=['id_source', 'state_source'])
    false_rel = false_rel.merge(g_nodes.add_suffix('_target'), left_on=['target', 'state'],
                                right_on=['id_target', 'state_target'])
    false_rel = false_rel[(false_rel['state'] == 1) &
                          (((false_rel['type_source'].isin(['act'])) &
                            (~false_rel['rel_name'].isin(['lexid-s:hasSubject', 'lexid-s:hasActType',
                                                          'lexid-s:hasObject', 'lexid-s:hasModality',
                                                          'lexid-s:hasQualifier']))) |
                           ((false_rel['type_source'].isin(['root', 'none'])) &
                            (~false_rel['rel_name'].isin(['lexid-s:hasSubject', 'lexid-s:hasAct', 'lexid-s:root',
                                                          'lexid-s:hasCondition']))) |
                           ((false_rel['type_source'].isin(['and', 'or', 'xor'])) &
                            (~false_rel['rel_name'].isin(['lexid-s:and', 'lexid-s:or', 'lexid-s:xor',
                                                          'lexid-s:referTo']))) |
                           ((~false_rel['type_source'].isin(['act', 'and', 'or', 'xor', 'root', 'none'])) &
                            (~false_rel['rel_name'].isin(['lexid-s:hasSubject', 'lexid-s:hasCondition',
                                                          'lexid-s:hasQualifierValue', 'lexid-s:hasQualifierType',
                                                          'lexid-s:referTo']))))][['sentence_id', 'type_source', 'id',
                                                                                   'type_target', 'source', 'target',
                                                                                   'rel_name', 'state',
                                                                                   'is_root_sentence']]
    root_false = false_rel[(~(false_rel['type_source'].isin(['and', 'or', 'xor'])) &
                            (false_rel['rel_name'].isin(['lexid-s:and', 'lexid-s:or', 'lexid-s:xor'])))]
    false_rel = false_rel[~false_rel['id'].isin(root_false['id'])]
    non_act_false = false_rel[(~false_rel['type_source'].isin(['act'])) &
                              (false_rel['rel_name'].isin(['lexid-s:hasSubject', 'lexid-s:hasObject',
                                                           'lexid-s:hasQualifier', 'lexid-s:hasActType']))]
    act_false = false_rel[(false_rel['type_source'].isin(['act'])) &
                          (~false_rel['rel_name'].isin(['lexid-s:hasSubject', 'lexid-s:hasObject',
                                                        'lexid-s:hasQualifier', 'lexid-s:hasActType']))]

    non_act_with_cond = g_edges[
        (g_edges['rel_name'] == 'lexid-s:hasCondition') &
        (g_edges['state'] == 1)].add_suffix('_condition').merge(non_act_false,
                                                                left_on='source_condition',
                                                                right_on='source')[
        ['source_condition', 'target_condition']].drop_duplicates()
    non_act_with_cond['rank'] = non_act_with_cond.groupby('source_condition')['target_condition'].transform('rank')
    non_act_with_cond = non_act_with_cond[non_act_with_cond['rank'] == 1][['source_condition', 'target_condition']]
    non_act_with_no_cond = non_act_false[~non_act_false['source'].isin(non_act_with_cond['source_condition'])][
        ['sentence_id', 'source']].drop_duplicates()
    non_act_with_no_cond = non_act_with_no_cond.drop_duplicates()
    non_act_with_no_cond['rank'] = non_act_with_no_cond['source'].rank()
    non_act_with_no_cond['rank1'] = non_act_with_no_cond['rank'] + len(g_nodes) - 1
    non_act_with_no_cond['rank2'] = non_act_with_no_cond['rank'] + len(g_nodes) + len(non_act_with_no_cond) - 1
    non_act_with_no_cond['rank3'] = non_act_with_no_cond['rank'] + len(g_nodes) + 2 * len(non_act_with_no_cond) - 1
    non_act_with_no_cond[
        ['text1', 'text2', 'text3', 'type1', 'type2', 'type3', 'min', 'max', 'is_root_sentence', 'state', 'uri2']] = [
        'Act', 'berupa', 'Concept', 'act', 'verb', 'concept', -1, -1, False, 1, 'lexid:Concept_Berupa']
    act_per_sentence = g_nodes[(g_nodes['type'] == 'act') & (g_nodes['state'] == 1)]
    concept_per_sentence = g_nodes[(g_nodes['type'] == 'concept') & (g_nodes['state'] == 1)]
    act_per_sentence['acts'] = act_per_sentence['uri'].str.extract(data + r'Act_(\d+)')
    concept_per_sentence['concepts'] = concept_per_sentence['uri'].str.extract(data + r'(?i)(?:numeral)?concept_(\d+)')
    act_per_sentence['acts'] = act_per_sentence.groupby('sentence_id')['acts'].transform('max')
    concept_per_sentence['concepts'] = concept_per_sentence.groupby('sentence_id')['concepts'].transform('max')
    act_per_sentence = act_per_sentence[['sentence_id', 'acts']].drop_duplicates()
    concept_per_sentence = concept_per_sentence[['sentence_id', 'concepts']].drop_duplicates()
    non_act_with_no_cond = non_act_with_no_cond.merge(act_per_sentence, left_on='sentence_id', right_on='sentence_id')
    non_act_with_no_cond = non_act_with_no_cond.merge(concept_per_sentence, left_on='sentence_id',
                                                      right_on='sentence_id')
    non_act_with_no_cond['id_act'] = non_act_with_no_cond['acts'].astype(int) + \
                                     non_act_with_no_cond.groupby('sentence_id')['source'].transform('rank')
    non_act_with_no_cond['id_concept'] = non_act_with_no_cond['concepts'].astype(int) + \
                                         non_act_with_no_cond.groupby('sentence_id')['source'].transform('rank')
    non_act_with_no_cond['uri1'] = data + non_act_with_no_cond['text1'] + '_' + \
                                   non_act_with_no_cond['id_act'].astype(int).astype(str) + '_' + \
                                   non_act_with_no_cond['sentence_id']
    non_act_with_no_cond['uri3'] = data + non_act_with_no_cond['text3'] + '_' + \
                                   non_act_with_no_cond['id_concept'].astype(int).astype(str) + '_' + \
                                   non_act_with_no_cond['sentence_id']
    source_in = g_edges[g_edges['target'].isin(non_act_with_no_cond['source'].values)].add_suffix('_in').merge(
        non_act_with_no_cond, left_on='target_in', right_on='source')
    source_in = source_in.merge(g_nodes.add_suffix('_vin'), left_on='source_in', right_on='id_vin')
    source_in.loc[source_in['type_vin'].isin(['concept', 'root']), 'rank3'] = source_in.loc[
        source_in['type_vin'].isin(['concept', 'root']), 'source_in'].values
    new_edge1 = source_in[['sentence_id', 'rank3', 'rank1']]
    new_edge2 = source_in[['sentence_id', 'rank1', 'rank2']]
    new_edge3 = source_in[['sentence_id', 'rank3', 'source']]
    new_edge1[['rel_name', 'state', 'is_root_sentence', 'id']] = [schema + 'hasCondition', 1, False,
                                                                  new_edge1['rank3'].rank()]
    new_edge2[['rel_name', 'state', 'is_root_sentence', 'id']] = [schema + 'hasActType', 1, False,
                                                                  new_edge2['rank1'].rank()]
    new_edge3[['rel_name', 'state', 'is_root_sentence', 'id']] = [schema + 'hasSubject', 1, False,
                                                                  new_edge3['rank3'].rank()]
    new_edge1['id'] = new_edge1['id'] + len(g_edges) - 1
    new_edge2['id'] = new_edge2['id'] + len(g_edges) + len(new_edge1) - 1
    new_edge3['id'] = new_edge3['id'] + len(g_edges) + len(new_edge1) + len(new_edge2) - 1
    new_edge1 = new_edge1.rename(columns={'rank3': 'source', 'rank1': 'target'}, inplace=False)
    new_edge2 = new_edge2.rename(columns={'rank1': 'source', 'rank2': 'target'}, inplace=False)
    new_edge3 = new_edge3.rename(columns={'rank3': 'source', 'source': 'target'}, inplace=False)
    non_act_with_cond = pandas.concat([non_act_with_cond,
                                       non_act_with_no_cond[['source', 'rank1']].rename(
                                           columns={'source': 'source_condition',
                                                    'rank1': 'target_condition'},
                                           inplace=False)],
                                      ignore_index=True)
    non_act_false = non_act_false.merge(non_act_with_cond, left_on='source', right_on='source_condition')
    non_act_false = non_act_false.sort_values('id')
    g_edges = g_edges.drop_duplicates().sort_values('id')
    new_node1 = source_in[
        ['sentence_id', 'rank1', 'text1', 'type1', 'min', 'max', 'is_root_sentence', 'state', 'uri1']]
    new_node2 = source_in[
        ['sentence_id', 'rank2', 'text2', 'type2', 'min', 'max', 'is_root_sentence', 'state', 'uri2']]
    new_node3 = source_in[~source_in['type_vin'].isin(['concept', 'root'])][
        ['sentence_id', 'rank3', 'text3', 'type3', 'min', 'max', 'is_root_sentence', 'state', 'uri3']]
    source_in = source_in[~source_in['type_vin'].isin(['concept', 'root'])][
        ['id_in', 'rank3']].drop_duplicates().sort_values(['id_in'])
    non_act_false = non_act_false[['id', 'target_condition']].drop_duplicates()
    g_edges.loc[g_edges['id'].isin(non_act_false['id']),
                'source'] = non_act_false.loc[non_act_false['id'].isin(non_act_false['id']),
                                              'target_condition'].values
    g_edges.loc[g_edges['id'].isin(source_in['id_in']),
                'target'] = source_in.loc[source_in['id_in'].isin(source_in['id_in']),
                                          'rank3'].values
    g_edges = g_edges[['sentence_id', 'id', 'source', 'target', 'rel_name', 'state', 'is_root_sentence']]
    new_node1 = new_node1.rename(columns={'rank1': 'id', 'text1': 'text', 'type1': 'type', 'uri1': 'uri'})
    new_node2 = new_node2.rename(columns={'rank2': 'id', 'text2': 'text', 'type2': 'type', 'uri2': 'uri'})
    new_node3 = new_node3.rename(columns={'rank3': 'id', 'text3': 'text', 'type3': 'type', 'uri3': 'uri'})
    g_nodes = g_nodes[['sentence_id', 'id', 'text', 'type', 'min', 'max', 'is_root_sentence', 'state', 'uri']]
    g_edges = pandas.concat([g_edges, new_edge1, new_edge2, new_edge3], ignore_index=False).sort_values('id')
    g_nodes = pandas.concat([g_nodes, new_node1, new_node2, new_node3], ignore_index=False).sort_values('id')
    act_false = act_false.merge(g_edges.add_suffix('_sourcein'), left_on='source', right_on='target_sourcein')
    act_false = act_false[act_false['state_sourcein'] == 1]
    act_false = act_false.sort_values('id')
    act_false['rank_temp'] = act_false.groupby('id')['source_sourcein'].transform('rank')
    act_false = act_false[act_false['rank_temp'] == 1]
    g_edges.loc[g_edges['id'].isin(act_false['id']),
                'source'] = act_false.loc[act_false['id'].isin(act_false['id']), 'source_sourcein'].values
    g_edges.loc[g_edges['id'].isin(root_false['id']), 'state'] = 0
    return g_nodes, g_edges


def postag_handle(graph, modified, deleted):
    special_phrase = {
        'atas nama': [['atas', 'nama'], 'adp'],
        'sebagaimana dimaksud': [['sebagaimana', 'dimaksud'], 'phrase'],
        'sebagaimana tersebut': [['sebagaimana', 'tersebut'], 'phrase'],
        'dan / atau': [['dan', '/', 'atau'], 'cconj'],
        'dan /atau': [['dan', '/atau'], 'cconj'],
        'dan atau': [['dan', 'atau'], 'cconj'],
        'Dalam hal': [['Dalam', 'hal'], 'case'],
        'Selain dalam rangka': [['Selain', 'dalam', 'rangka'], 'case'],
        'dalam rangka': [['dalam', 'rangka'], 'case'],
        'tidak sesuai dengan': [['tidak', 'sesuai', 'dengan'], 'adp'],
        'sesuai dengan': [['sesuai', 'dengan'], 'adp'],
        'tidak boleh': [['tidak', 'boleh'], 'adv'],
        'sampai dengan': [['sampai', 'dengan'], 'cconj'],
        'sampai sebelum': [['sampai', 'sebelum'], 'adp'],
        'di luar': [['di', 'luar'], 'adp'],
        'di atas': [['di', 'atas'], 'adp'],
        'paling sedikit': [['paling', 'sedikit'], 'adv'],
        'paling banyak': [['paling', 'banyak'], 'adv'],
        'paling lambat': [['paling', 'lambat'], 'adv'],
        'paling lama': [['paling', 'lama'], 'adv'],
        'hanya untuk': [['hanya', 'untuk'], 'adv'],
        'hanya dapat': [['hanya', 'dapat'], 'adv'],
        'tidak dapat': [['tidak', 'dapat'], 'adv'],
        'pada saat': [['pada', 'saat'], 'case'],
        'sedang dalam': [['sedang', 'dalam'], 'case'],
        'masa berlaku': [['masa', 'berlaku'], 'noun']
    }
    modality = ['harus', 'dapat', 'wajib', 'tidak dapat', 'tidak boleh', 'tetap', 'paling sedikit', 'hanya untuk',
                'hanya dapat', 'tidak wajib', 'paling lama']

    '''Pre-Processing'''
    '''add root node'''
    root_nodes = pandas.DataFrame(
        [{'dummy': 0, 'id': 0, 'text': 'root', 'type': 'none', 'min': 0, 'max': 0, 'state': 1}])
    sentences_id = graph['V'][['sentence_id', 'suffix', 'is_root_sentence']].drop_duplicates()
    sentences_id['dummy'] = 0
    '''remove duplicates article'''
    root_nodes = root_nodes.merge(sentences_id, left_on='dummy', right_on='dummy').drop(columns=['dummy'])
    graph['V'] = pandas.concat([graph['V'], root_nodes])
    graph['V']['dup'] = graph['V'].groupby(['sentence_id', 'suffix', 'id'])['id'].transform('count')
    dup = graph['V'][graph['V']['dup'] > 1]['sentence_id'].unique()
    graph['V'] = graph['V'][~graph['V']['sentence_id'].isin(dup)].drop(columns=['dup'])
    graph['E'] = graph['E'][~graph['E']['sentence_id'].isin(dup)]
    graph['V']['state'] = 1
    '''reset index order by document and article id'''
    graph['V'] = graph['V'].sort_values(['sentence_id', 'suffix', 'id']).reset_index()
    graph['E'] = graph['E'].sort_values(['sentence_id', 'suffix', 'id']).reset_index()
    graph['V'].reset_index(inplace=True)
    graph['E'].reset_index(inplace=True)
    graph['E'] = graph['E'].merge(graph['V'], left_on=['source', 'sentence_id', 'suffix'],
                                  right_on=['id', 'sentence_id', 'suffix'],
                                  suffixes=['', '_source'])
    graph['E'] = graph['E'].merge(graph['V'], left_on=['target', 'sentence_id', 'suffix'],
                                  right_on=['id', 'sentence_id', 'suffix'],
                                  suffixes=['', '_target'])
    graph['V']['id'] = graph['V']['level_0']
    graph['E'][['id', 'source', 'target']] = graph['E'][['level_0', 'level_0_source', 'level_0_target']]
    graph['V'] = graph['V'].drop(['level_0', 'index'], 1)
    graph['E'] = graph['E'][
        ['sentence_id', 'id', 'source', 'target', 'rel_name', 'state', 'is_root_sentence', 'suffix']]
    graph['V']['stype'] = 'Article_'
    graph['V'].loc[graph['V']['sentence_id'].str.contains(r'LX\d+[A-WYZ]*X\d+[A-WYZ]*'), 'stype'] = 'Section_'
    graph['V']['sentence_id'] = graph['V'].apply(lambda x: x['sentence_id'].replace('LX', x['stype']), axis=1)
    graph['V']['sentence_id'] = graph['V']['sentence_id'].replace(to_replace=r'[X_]V(?=\d)', value='_Number_',
                                                                  regex=True)
    graph['V']['sentence_id'] = graph['V']['sentence_id'].replace(to_replace='X(?=[A-Z])', value='_Letter_', regex=True)
    graph['V']['sentence_id'] = graph['V']['sentence_id'].replace(to_replace='X', value='_', regex=True)
    graph['V'] = graph['V'].drop(columns=['stype'])
    graph['E']['stype'] = 'Article_'
    graph['E'].loc[graph['E']['sentence_id'].str.contains(r'LX\d+[A-WYZ]*X\d+[A-WYZ]*'), 'stype'] = 'Section_'
    graph['E']['sentence_id'] = graph['E'].apply(lambda x: x['sentence_id'].replace('LX', x['stype']), axis=1)
    graph['E']['sentence_id'] = graph['E']['sentence_id'].replace(to_replace=r'[X_]V(?=\d)', value='_Number_',
                                                                  regex=True)
    graph['E']['sentence_id'] = graph['E']['sentence_id'].replace(to_replace='X(?=[A-Z])', value='_Letter_', regex=True)
    graph['E']['sentence_id'] = graph['E']['sentence_id'].replace(to_replace='X', value='_', regex=True)
    graph['E'] = graph['E'].drop(columns=['stype'])

    if len(modified) > 0:
        modified['source'] = modified['source'].replace(to_replace=data, value='', regex=True)
        modified['suffix'] = modified['target'].str.extract('(_Modified_By.+$)')
        print(modified[modified['source'].str.contains('Permen_Dagri_2015_40', regex=True)])
        if deleted is not None and len(deleted) > 0:
            deleted['id'] = deleted['id'].replace(to_replace='^' + data + '|_Modified_By_.+$', value='', regex=True)
            graph['V'].loc[(graph['V']['sentence_id'].isin(deleted['id'])), 'state'] = 0
            graph['E'].loc[(graph['E']['sentence_id'].isin(deleted['id'])), 'state'] = 0
        graph['V']['article_section'] = graph['V']['sentence_id'].replace(to_replace='_(Number|Letter).+', value='',
                                                                          regex=True)
        graph['V']['reg_id'] = graph['V']['sentence_id'].replace(to_replace='_(Article|Section).+', value='',
                                                                 regex=True)
        graph['V']['old_suffix'] = graph['V']['suffix']
        graph['V']['suffix'] = '_Modified_By_' + graph['V']['suffix']

        graph['V'] = graph['V'].merge(modified, left_on='article_section', right_on='source',
                                      how='left').drop_duplicates()
        graph['V']['dup'] = graph['V'].groupby('id')['id'].transform('count')
        graph['V'] = graph['V'][(graph['V']['suffix_y'].isna()) | (graph['V']['suffix_x'] == graph['V']['suffix_y']) |
                                (~(graph['V']['suffix_x'] == graph['V']['suffix_y']) &
                                 (graph['V']['reg_id'] == graph['V']['old_suffix'])) | (graph['V']['dup'] == 1)]
        graph['V']['suffix'] = graph['V']['suffix_y'].combine_first(graph['V']['suffix_x'])
        graph['V'].loc[graph['V']['suffix_y'].isna() | (graph['V']['reg_id'] == graph['V']['old_suffix']),
                       'suffix'] = ''
        graph['V'].loc[~(graph['V']['suffix_y'].isna()) & ~(graph['V']['suffix'] == '') &
                       ~(graph['V']['suffix_x'] == graph['V']['suffix_y']), 'state'] = 0
        graph['V']['sentence_id'] = graph['V']['sentence_id'] + graph['V']['suffix']
        graph['V'] = graph['V'][['sentence_id', 'id', 'text', 'type', 'min', 'max', 'is_root_sentence', 'state',
                                 'suffix']].drop_duplicates()
        graph['E']['article_section'] = graph['E']['sentence_id'].replace(to_replace='_(Number|Letter).+', value='',
                                                                          regex=True)
        graph['E']['reg_id'] = graph['E']['sentence_id'].replace(to_replace='_(Article|Section).+', value='',
                                                                 regex=True)
        graph['E']['old_suffix'] = graph['E']['suffix']
        graph['E']['suffix'] = '_Modified_By_' + graph['E']['suffix']
        graph['E'] = graph['E'].merge(modified, left_on='article_section', right_on='source',
                                      how='left').drop_duplicates()
        graph['E']['dup'] = graph['E'].groupby('id')['id'].transform('count')
        graph['E'] = graph['E'][(graph['E']['suffix_y'].isna()) | (graph['E']['suffix_x'] == graph['E']['suffix_y']) |
                                (~(graph['E']['suffix_x'] == graph['E']['suffix_y']) &
                                 (graph['E']['reg_id'] == graph['E']['old_suffix'])) | (graph['E']['dup'] == 1)]
        graph['E']['suffix'] = graph['E']['suffix_y'].combine_first(graph['E']['suffix_x'])
        graph['E'].loc[graph['E']['suffix_y'].isna() | (graph['E']['reg_id'] == graph['E']['old_suffix']),
                       'suffix'] = ''
        graph['E'].loc[~(graph['E']['suffix_y'].isna()) & ~(graph['E']['suffix'] == '') &
                       ~(graph['E']['suffix_x'] == graph['E']['suffix_y']), 'state'] = 0
        graph['E']['sentence_id'] = graph['E']['sentence_id'] + graph['E']['suffix']
        graph['E'] = graph['E'][['sentence_id', 'id', 'source_x', 'target_x', 'rel_name', 'state',
                                 'is_root_sentence']].drop_duplicates()
        graph['E'] = graph['E'].rename(columns={'source_x': 'source', 'target_x': 'target'}, inplace=False)
        graph['E']['temp'] = graph['E'].groupby('id')['id'].transform('count')
    else:
        graph['V']['suffix'] = ''
        graph['E']['suffix'] = ''
    print('step 1')
    '''merge special phrase'''
    for phrase in special_phrase.values():
        stop, graph = special_merge(graph, phrase[0], phrase[1])
    copy_node = graph['V'].copy()
    copy_edge = graph['E'].copy()
    new_G = {'V': copy_node, 'E': copy_edge}
    print('step 2')
    '''merge compound rels as phrase node'''
    new_G['V'], new_G['E'] = compound_restructure(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 3')
    '''remove punctuation rels'''
    new_G['E'].loc[new_G['E'].rel_name == 'punct', 'state'] = 0
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 4')
    '''reconstruct conjuction rels'''
    new_G['V'], new_G['E'] = clear_conj(new_G['V'], new_G['E'])
    new_G['V'], new_G['E'] = conj_restructure(new_G['V'], new_G['E'])
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 5')
    '''handling cases rels'''
    new_G['V'], new_G['E'] = cases_handler(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 6')
    '''handling nsubj rels'''
    new_G['V'], new_G['E'] = nsubj_handler(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 7')
    '''range conj construction'''
    new_G['V'], new_G['E'] = range_formatter(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 8')
    '''condition handler'''
    new_G['V'], new_G['E'] = condition_construction(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 9')
    '''construct if else'''
    new_G['V'], new_G['E'] = if_else_construction(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 10')
    new_G['V'], new_G['E'] = act_construction(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 11')
    new_G['V'], new_G['E'] = conj_finishing(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 12')
    new_G['V'], new_G['E'] = cases_finishing(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 13')
    """todo : refactor"""
    new_G['V'], new_G['E'] = acts_reformat(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 14')
    new_G['V'], new_G['E'] = modality_handler(new_G, modality)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 15')
    refers = new_G['V'][new_G['V'].text.str.contains(r'^ketentuan REFER\d+', regex=True)]
    for refer in refers.iterrows():
        new_refer_text = re.sub('^ketentuan ', '', refer[1]['text'])
        new_G['V'].loc[new_G['V'].id == refer[1]['id'], 'text'] = new_refer_text.strip()
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 16')
    new_G['V'], new_G['E'] = based_on_formatter(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 17')
    new_G['V'], new_G['E'] = verb_to_verb_handler(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 18')
    new_G['V'], new_G['E'] = numeral_construction(new_G, modality)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 19')
    new_G['V'], new_G['E'] = compound_finishing(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 20')
    new_G['V'], new_G['E'] = condition_finishing(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 21')
    new_G['V'], new_G['E'] = refer_to_finishing(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 22')
    new_G['V'], new_G['E'] = conj_out_finishing(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''

    print('step 23')
    new_G['V'], new_G['E'] = invalid_rels_refinement(new_G)
    new_G['V'].loc[new_G['V']['suffix'].isna(), 'suffix'] = ''
    print('step 24')
    new_G['V'], new_G['E'] = postag_finishing(new_G)
    g_edges = new_G['E']
    g_nodes = new_G['V']
    s3 = sqldf('''
                select
                    g_edges.sentence_id,
                    g_edges.id,
                    sources.text as source,
                    rel_name,
                    targets.text as target,
                    source as id_source,
                    target as id_target,
                    g_edges.state
                from
                    g_edges
                join
                    g_nodes as sources
                on
                    sources.id = g_edges.source
                join
                    g_nodes as targets
                on
                    targets.id = g_edges.target
                where g_edges.state=1
            ''')
    for sentence_id in s3[s3['sentence_id'].str.contains('UU_1983_8_Section_4A_3', regex=True)]['sentence_id'].unique():
        print(sentence_id)
        print(g_nodes[(g_nodes.sentence_id == sentence_id) & (g_nodes.state == 1)])
        print(s3[s3.sentence_id == sentence_id])
    return new_G


def get_expand_range(triples):
    expandRes = []
    startTriples = triples[triples['rel_name'] == schema + 'start']
    endTriples = triples[triples['rel_name'] == schema + 'end']
    triples = startTriples.merge(endTriples.add_suffix('_end'), left_on='source', right_on='source_end')
    triples = triples[['source', 'rel_name', 'target', 'target_end']]
    for triple in triples.iterrows():
        start_funct = triple[1]['target']
        end_funct = triple[1]['target_end']
        try:
            isStartNum = re.match(r'^\d+$', re.search(r'[\dA-Za-z]+$', start_funct).group(0))
        except Exception:
            isStartNum = None
        try:
            isEndNum = re.match(r'^\d+$', re.search(r'[\dA-Za-z]+$', end_funct).group(0))
        except Exception:
            isEndNum = None
        if isStartNum and isEndNum:
            start_funct = int(re.search(r'[\dA-Za-z]+$', start_funct).group(0))
            end_funct = int(re.search(r'[\dA-Za-z]+$', end_funct).group(0))
            for new_el in range(start_funct, end_funct + 1):
                temp = {'source': triple[1]['source'], 'rel_name': schema + 'hasElement'}
                new_target = str(new_el)
                temp['target'] = re.sub(r'[\dA-Za-z]+$', new_target, triple[1]['target'])
                expandRes.append(temp)
        else:
            try:
                start_funct = ord(re.search(r'[\dA-Za-z]+$', start_funct).group(0))
                end_funct = ord(re.search(r'[\dA-Za-z]+$', end_funct).group(0))
                for new_el in range(start_funct, end_funct + 1):
                    temp = {'source': triple[1]['source'], 'rel_name': schema + 'hasElement'}
                    new_target = chr(new_el)
                    temp['target'] = re.sub(r'[\dA-Za-z]+$', new_target, triple[1]['target'])
                    expandRes.append(temp)
            except Exception:
                pass
    expandRes = pandas.DataFrame(expandRes)
    return expandRes


def get_sentence_triple(graph, refer, alias, concept):
    g_nodes = graph['V']
    g_edges = graph['E']
    if len(alias) == 0:
        alias['chapter'] = ''
        alias['object'] = ''
    alias['chapter'] = alias['chapter'].str.title()
    alias['chapter'] = alias['chapter'].replace(to_replace='V', value='Number_', regex=True)
    g_nodes['reg_id'] = g_nodes['sentence_id'].replace(to_replace=r'_(Article|Section|LX).*$', value='', regex=True)
    g_edges['reg_id'] = g_edges['sentence_id'].replace(to_replace=r'_(Article|Section|LX).*$', value='', regex=True)
    g_nodes.loc[g_nodes['uri'].isnull(), 'uri'] = '_'
    ref_node = g_nodes[g_nodes['uri'].str.contains('RX')]
    g_nodes = g_nodes.reset_index()[
        ['sentence_id', 'id', 'text', 'type', 'min', 'max', 'is_root_sentence', 'state', 'uri', 'reg_id']]
    g_edges = g_edges.reset_index()[['sentence_id', 'id', 'source', 'target', 'rel_name', 'state', 'is_root_sentence']]
    if not ref_node.empty:
        ref_node['rx'] = g_nodes['uri'].str.extract(r'(RX\d+[A-Z]{0,2}(X\d+[A-Z]{0,2})?(V\d+)?N\d+)')[0]
        ref_node = ref_node.merge(refer, left_on=['rx', 'reg_id'], right_on=['id', 'reg_id']).sort_values(['id_x'])
        ref_node.loc[ref_node['value'].str.contains('(?i)article|section', regex=True),
                     'value'] = data + ref_node.loc[ref_node['value'].str.contains('(?i)article|section', regex=True),
                                                    'value'].values
        ref_node.loc[~ref_node['value'].str.contains('(?i)article|section', regex=True),
                     'value'] = data + ref_node.loc[~ref_node['value'].str.contains('(?i)article|section', regex=True),
                                                    'value'].values
        ref_node['text'] = ref_node['value'].replace(to_replace='^' + data, value='', regex=True)
        ref_node['stype'] = 'Article_'
        ref_node.loc[g_nodes['sentence_id'].str.contains(r'LX\d+[A-WYZ]*X\d+[A-WYZ]*'), 'stype'] = 'Section_'
        ref_node['sentence_id'] = ref_node.apply(lambda x: x['sentence_id'].replace('LX', x['stype']), axis=1)
        ref_node['sentence_id'] = ref_node['sentence_id'].replace(to_replace=r'[X_]V(?=\d)', value='_Number_',
                                                                  regex=True)
        ref_node['sentence_id'] = ref_node['sentence_id'].replace(to_replace='X(?=[A-Z])', value='_Letter_', regex=True)
        ref_node['sentence_id'] = ref_node['sentence_id'].replace(to_replace='X', value='_', regex=True)
        ref_node = ref_node.drop(columns=['stype'])
        g_nodes = g_nodes.sort_values(['id'])
        try:
            g_nodes.loc[g_nodes['id'].isin(ref_node.id_x.array),
                        ['uri', 'text']] = ref_node.loc[ref_node['id_x'].isin(ref_node.id_x.array),
                                                        ['value', 'text']].values
        except ValueError:
            drop_article = error_handling(g_nodes.loc[g_nodes['id'].isin(ref_node.id_x.array)],
                                          ref_node.loc[ref_node['id_x'].isin(ref_node.id_x.array)], ['id'], ['id_x'])
            g_edges.loc[g_edges['sentence_id'].isin(drop_article), 'state'] = 0
            g_nodes.loc[g_nodes['sentence_id'].isin(drop_article), 'state'] = 0
    all_cx = g_nodes[g_nodes['text'].str.contains('CX')].sort_values(['id'])
    cx_node = all_cx.copy()
    while len(cx_node) > 0:
        cx_node['cx'] = cx_node['text'].str.extract(r'(CX\d+[A-Z]{0,2}(X\d+[A-Z]{0,2})?(V\d+)?N\d+)')[0]
        cx_node = cx_node.merge(concept, left_on=['cx', 'reg_id'], right_on=['id', 'reg_id']).sort_values(['id_x'])
        all_cx = all_cx.sort_values(['id'])
        if len(cx_node) > 0:
            cx_node['count'] = cx_node.groupby(['id_x'])['id_x'].transform('count')
            cx_node['rank'] = cx_node.groupby(['id_x'])['count'].rank(method='first')
            cx_node = cx_node[cx_node['rank'] == 1].sort_values(['id_x'])
            all_cx = all_cx.drop_duplicates().sort_values(['id'])
            cx_node['new_text'] = cx_node.apply(lambda x: x['text_x'].replace(x['cx'], x['text_y']), axis=1)
            all_cx.loc[all_cx['id'].isin(cx_node.id_x.array),
                       ['text']] = cx_node.loc[cx_node['id_x'].isin(cx_node.id_x.array), ['new_text']].values
            cx_node = all_cx[all_cx['text'].str.contains('CX')]
    all_cx['new_uri'] = all_cx['text'].str.title()
    all_cx['new_uri'] = all_cx['new_uri'].replace(to_replace=r'[\s\W]+', value='_', regex=True)
    all_cx['new_uri'] = data + 'Concept_' + all_cx['new_uri']
    g_nodes = g_nodes.sort_values(['id'])
    g_nodes.loc[g_nodes['id'].isin(all_cx.id.array),
                ['text', 'uri']] = all_cx.loc[all_cx['id'].isin(all_cx.id.array), ['text', 'new_uri']].values
    num = g_nodes[(g_nodes['type'] == 'num') & (g_nodes['state'] == 1)]
    num_d = num[num['text'].str.contains(r'\d', regex=True)]
    num_d['uri'] = '"' + num_d['text'].replace(to_replace=r',0+$|\D', value='', regex=True) + '"^^xsd:int'
    num_d.sort_values(['id'])
    g_nodes.loc[g_nodes['id'].isin(num_d.id.array), 'uri'] = num_d.loc[num_d['id'].isin(num_d.id.array), 'uri']
    num_t = num[num['text'].str.contains(r'^[a-z ]+$', regex=True)]
    num_t['uri'] = '"' + num_t['text'] + '"'
    g_nodes.loc[g_nodes['id'].isin(num_t.id.array), 'uri'] = num_t.loc[num_t['id'].isin(num_t.id.array), 'uri']
    g_nodes['uri'] = g_nodes['uri'].replace(to_replace='^refer:', value=data, regex=True)
    roots = g_nodes[(g_nodes['type'] == 'root') &
                    (g_nodes['state'] == 1)]
    if len(alias) > 0:
        alias['sentence_id'] = alias['reg_id'] + '_' + alias['chapter']
        alias = alias.merge(roots, left_on='sentence_id', right_on='sentence_id')
        alias = alias[alias['text'] != 'Norm']
        alias['object'] = alias['object'].str.strip()
        cx_node = alias[alias['object'].str.contains('CX')]
        cx_node['cx'] = cx_node['object'].str.extract(r'(CX[XV0-9]+N\d+)')
        cx_node = cx_node.merge(concept, left_on=['cx', 'reg_id_x'], right_on=['id', 'reg_id']).sort_values(
            ['sentence_id'])
        cx_node['text_y'] = cx_node['text_y'].replace(to_replace=r'\s+', value='_', regex=True)
        alias['uri_old'] = data + 'Concept_' + alias['object'].str.title().replace(to_replace=r'\s+', value='_',
                                                                                   regex=True)
        alias['uri_al'] = data + 'Concept_' + alias['reg_id_x'] + '_' + alias['object'].str.title().replace(
            to_replace=r'\s+',
            value='_', regex=True)
        if len(cx_node) > 0:
            cx_node['new_text'] = cx_node.apply(lambda x: x['object'].replace(x['cx'], x['text_y']), axis=1)
            cx_node['uri_old'] = data + 'Concept_' + cx_node['new_text'].str.title().replace(to_replace=r'\s+',
                                                                                             value='_',
                                                                                             regex=True)
            cx_node['uri_al'] = data + 'Concept_' + cx_node['reg_id_x'] + '_' + cx_node[
                'new_text'].str.title().replace(
                to_replace=r'\s+', value='_', regex=True)
            alias = alias.sort_values(['sentence_id'])
            cx_node = cx_node.sort_values(['sentence_id'])
            alias.loc[alias['sentence_id'].isin(cx_node.sentence_id.array),
                      ['uri_old', 'uri_al',
                       'object']] = cx_node.loc[cx_node['sentence_id'].isin(cx_node.sentence_id.array),
                                                ['uri_old', 'uri_al', 'new_text']].values
        nodes_alias = g_nodes.merge(alias.add_suffix('_alias'), left_on=['uri', 'reg_id'],
                                    right_on=['uri_old_alias', 'reg_id_x_alias'])[
            ['id', 'uri', 'reg_id', 'reg_id_x_alias', 'uri_al_alias']]
        nodes_alias = nodes_alias.drop_duplicates().sort_values(['id'])
        g_nodes.loc[g_nodes['id'].isin(nodes_alias.id.array),
                    'uri'] = nodes_alias.loc[nodes_alias['id'].isin(nodes_alias.id.array), 'uri_al_alias'].values
        alias = alias.rename(columns={'uri': 'source', 'uri_al': 'target'}, inplace=False)
        alias['rel_name'] = sameAs
        aliases_triple = alias[['source', 'rel_name', 'target']]
        aliases_triple1 = aliases_triple.rename(columns={'source': 'target', 'target': 'source'})
        aliases_triple = pandas.concat([aliases_triple, aliases_triple1])
    root_concepts = roots[roots['text'] != 'Norm'][['id', 'text', 'state', 'reg_id', 'uri', 'sentence_id']]
    root_concepts = root_concepts.merge(g_edges.add_suffix('_e'), left_on=['id', 'state'],
                                        right_on=['source_e', 'state_e'])
    root_concepts = root_concepts[root_concepts['rel_name_e'] == schema + 'hasSubject'][
        ['id', 'text', 'state', 'reg_id', 'uri',
         'sentence_id', 'target_e']]
    root_concepts = root_concepts.merge(g_nodes.add_suffix('_subj'), left_on=['target_e', 'state'],
                                        right_on=['id_subj', 'state_subj'])
    root_concepts = root_concepts[(root_concepts['text'].str.lower() == root_concepts['text_subj'].str.lower()) &
                                  ~root_concepts['text'].str.contains('(?i)^concept')][['id', 'uri', 'reg_id',
                                                                                        'sentence_id']]
    if len(root_concepts) > 0:
        root_concepts['new_uri'] = root_concepts.apply(lambda x: x['uri'].replace(data + 'Concept_',
                                                                                  data + 'Concept_' + x[
                                                                                      'reg_id'] + '_'),
                                                       axis=1)
        root_concepts = root_concepts.drop_duplicates().sort_values(['id'])
        g_nodes.loc[g_nodes['id'].isin(root_concepts.id.array),
                    'uri'] = root_concepts.loc[root_concepts['id'].isin(root_concepts.id.array), 'new_uri'].values
        root_concepts = root_concepts.merge(g_nodes.add_suffix('_o'), left_on=['uri', 'reg_id'],
                                            right_on=['uri_o', 'reg_id_o'])
        root_concepts = root_concepts[root_concepts['sentence_id'] != root_concepts['sentence_id_o']][['id_o',
                                                                                                       'new_uri']]
        root_concepts = root_concepts.drop_duplicates().sort_values(['id_o'])
        g_nodes.loc[g_nodes['id'].isin(root_concepts.id_o.array),
                    'uri'] = root_concepts.loc[root_concepts['id_o'].isin(root_concepts.id_o.array), 'new_uri'].values
    false_root = g_nodes[(g_nodes['uri'] == data + 'Concept_Root') & (g_nodes['state'] == 1)]
    if len(false_root) > 0:
        false_root['new_uri'] = false_root.apply(lambda x: x['uri'].replace(data + 'Concept_',
                                                                            data + 'Concept_' + x['sentence_id']
                                                                            + '_'),
                                                 axis=1)
        false_root = false_root.sort_values(['id'])
        g_nodes.loc[g_nodes['id'].isin(false_root.id.array),
                    'uri'] = false_root.loc[false_root['id'].isin(false_root.id.array), 'new_uri'].values
    g_nodes.loc[g_nodes['text'] == 'Norm',
                'text'] = 'Norm dari ' + g_nodes.loc[g_nodes['text'] == 'Norm',
                                                     'sentence_id'].values
    g_nodes.loc[g_nodes['text'].isin(['Act', 'Concept']),
                'text'] = g_nodes.loc[g_nodes['text'].isin(['Act', 'Concept']), 'uri'].values
    g_nodes['text'] = g_nodes['text'].replace(to_replace='act:', value='', regex=True)
    concept_x_lbl = g_nodes[g_nodes['text'].str.contains('(?i)^(concept(s?)|refers|acts)$', regex=True) &
                            (g_nodes['state'] == 1)][['id', 'state', 'text']]
    while len(concept_x_lbl) > 0:
        concept_x_lbl = concept_x_lbl.merge(g_edges.add_suffix('_e'), left_on=['id', 'state'],
                                            right_on=['source_e', 'state_e'])
        concept_x_lbl = concept_x_lbl[(~concept_x_lbl.isnull().any(axis=1)) &
                                      concept_x_lbl['rel_name_e'].str.contains('(?i)concept|refer|act|subject|'
                                                                               'condition|referTo')][['id', 'state',
                                                                                                      'text',
                                                                                                      'target_e',
                                                                                                      'rel_name_e']]
        concept_x_lbl = concept_x_lbl.merge(g_nodes.add_suffix('_det'), left_on=['target_e', 'state'],
                                            right_on=['id_det', 'state_det'])[['id', 'state', 'text', 'rel_name_e',
                                                                               'id_det', 'text_det']]
        concept_x_lbl.loc[concept_x_lbl['rel_name_e'].str.contains('condition'), 'text_det'] = 'with condition'
        concept_x_lbl.loc[concept_x_lbl['rel_name_e'].str.contains('referTo'),
                          'text_det'] = 'merujuk pada ' + concept_x_lbl.loc[
            concept_x_lbl['rel_name_e'].str.contains('referTo'),
            'text_det'].values
        concept_x_lbl['unclear'] = 0
        concept_x_lbl.loc[concept_x_lbl['text_det'].str.contains('(?i)^(concept(s?)|refers|acts)$', regex=True),
                          'unclear'] = 1
        concept_x_lbl['unclear'] = concept_x_lbl.groupby(['id'])['unclear'].transform('sum')
        concept_x_lbl = concept_x_lbl[concept_x_lbl['unclear'] == 0]
        if len(concept_x_lbl) == 0:
            break
        concept_x_lbl['conj'] = ' ' + concept_x_lbl['rel_name_e'].str.extract('(?i)(and|or|xor|conj|end|start)') + ' '
        concept_x_lbl.loc[concept_x_lbl['conj'].isnull(), 'conj'] = ' '
        concept_x_lbl['conj'] = concept_x_lbl['conj'].replace(to_replace='And', value='dan', regex=True)
        concept_x_lbl['conj'] = concept_x_lbl['conj'].replace(to_replace='Or', value='dan/atau', regex=True)
        concept_x_lbl['conj'] = concept_x_lbl['conj'].replace(to_replace='Xor', value='atau', regex=True)
        concept_x_lbl['conj'] = concept_x_lbl['conj'].replace(to_replace='Start', value='', regex=True)
        concept_x_lbl['conj'] = concept_x_lbl['conj'].replace(to_replace='End', value='sampai dengan', regex=True)
        concept_x_lbl = concept_x_lbl.sort_values(['id', 'id_det'])
        concept_x_lbl['text_det'] = concept_x_lbl['conj'] + concept_x_lbl['text_det']
        concept_x_lbl['text_det'] = concept_x_lbl.groupby(['id'])['text_det'].transform(lambda x: ''.join(x))
        concept_x_lbl['text_det'] = concept_x_lbl['text_det'].replace(to_replace=r'^\s*(|dan|atau|dan/atau|conj)\s',
                                                                      value='',
                                                                      regex=True)
        concept_x_lbl['text_det'] = concept_x_lbl['text_det'].replace(to_replace=r'LX[X0-9]+', value='huruf ',
                                                                      regex=True)
        concept_x_lbl['text_det'] = concept_x_lbl['text_det'].str.strip()
        concept_x_lbl = concept_x_lbl[['id', 'text_det']].drop_duplicates().sort_values(['id'])
        g_nodes = g_nodes.sort_values(['id'])
        g_nodes.loc[g_nodes['id'].isin(concept_x_lbl.id.array),
                    'text'] = concept_x_lbl.loc[concept_x_lbl['id'].isin(concept_x_lbl.id.array), 'text_det'].values
        concept_x_lbl = g_nodes[g_nodes['text'].str.contains('(?i)^(concept(s?)|refers|acts)$', regex=True) &
                                (g_nodes['state'] == 1)][['id', 'state', 'text']]
    s3 = sqldf('''
                    select
                        g_edges.sentence_id,
                        g_edges.id,
                        sources.uri as source,
                        rel_name,
                        targets.uri as target,
                        source as id_source,
                        target as id_target,
                        g_edges.state
                    from
                        g_edges
                    join
                        g_nodes as sources
                    on
                        sources.id = g_edges.source
                    and
                        sources.state = g_edges.state
                    join
                        g_nodes as targets
                    on
                        targets.id = g_edges.target
                    and
                        targets.state = g_edges.state
                    where
                        targets.state = 1
                ''')
    for sentence_id in s3[s3['sentence_id'].str.contains('UU_1983_8_Section_10_3', regex=True)]['sentence_id'].unique():
        print(sentence_id)
        print(g_nodes[(g_nodes.sentence_id == sentence_id) & (g_nodes.state == 1)])
        print(s3[s3.sentence_id == sentence_id])
    sentence_triples = s3[['source', 'rel_name', 'target']]
    node_label = g_nodes[(g_nodes['uri'].str.contains('(?i)(Act|Concept|And|Or|Xor|Letter|Number)', regex=True)) & (
                g_nodes['state'] == 1)]
    node_label['rel_name'] = label
    node_label['text'] = node_label['text'].replace(to_replace=r'LX[\dX]+', value='Huruf ', regex=True)
    node_label['text'] = node_label['uri'].replace(to_replace=r'^(lexid:Per|UU).+_Number_', value='Angka ', regex=True)
    node_label['text'] = node_label['text'].replace(to_replace=r'^' + data, value='', regex=True)
    node_label['text'] = node_label['text'].replace(to_replace=r'_+', value=' ', regex=True)
    node_label['text'] = node_label['text'].replace(to_replace=r'((?<=Concept \d).+|(?<=Concept \d\d).+|' +
                                                               r'(?<=Act \d).+|(?<=Act \d\d).+)', value='', regex=True)
    node_label['text'] = '"' + node_label['text'] + '"^^xsd:string'
    node_label = node_label[['uri', 'rel_name', 'text']].rename(columns={'uri': 'source', 'text': 'target'})
    print(node_label[node_label['source'] == 'lexid:Concept_Berbagai_Alternatif'])
    if len(alias) > 0:
        alias_label = alias[['target', 'object']]
        alias_label['object'] = '"' + alias_label['object'] + '"'
        alias_label['rel_name'] = label
        alias_label = alias_label[['target', 'rel_name', 'object']].rename(
            columns={'target': 'source', 'object': 'target'})
        label_triple = pandas.concat([node_label, alias_label]).drop_duplicates()
    else:
        label_triple = node_label
    g_nodes['uri'] = g_nodes['uri'].replace(to_replace='^refer:', value=data, regex=True)
    rule = g_nodes[g_nodes['uri'].str.contains(':(And|Xor|Or)') & (g_nodes['state'] == 1)][['text', 'uri']]
    if len(alias) > 0:
        rule = pandas.concat([rule.rename(columns={'uri': 'source'}), alias_label[['source']]])
    else:
        rule = rule.rename(columns={'uri': 'source'})
    rule['rtype0'] = schema + rule['text'].str.title() + 'Expression'
    rule['rtype1'] = rule['source'].replace(to_replace='lexid:', value='', regex=True)
    rule['rtype1'] = rule['rtype1'].replace(to_replace='(s)?_.+', value='', regex=True)
    rule['rtype1'] = schema + rule['rtype1'].str.title()
    rule.loc[rule['source'].str.contains('^lexid:(And|Xor|Or)', regex=True), 'rtype1'] = CompoundExpression
    temp = g_nodes[~(g_nodes['uri'].str.contains(r':(And|Xor|Or)|xsd|"|(^_$)')) & (g_nodes['state'] == 1)][
        ['text', 'uri']]
    temp['rtype1'] = temp['uri'].replace(to_replace=data, value=schema, regex=True)
    temp['rtype1'] = temp['rtype1'].replace(to_replace='(' + schema + ')?Numeralc.+', value=Concept, regex=True)
    temp['rtype1'] = temp['rtype1'].replace(to_replace=schema + r'Norm.+$', value=Norm, regex=True)
    temp['rtype1'] = temp['rtype1'].replace(to_replace=schema + r'Act.+$', value=RuleAct, regex=True)
    temp['rtype1'] = temp['rtype1'].replace(to_replace=schema + r'.+_(Letter|Number)_[\da-zA-Z]+$', value=Items,
                                            regex=True)
    temp['rtype1'] = temp['rtype1'].replace(to_replace=schema + r'.+_Article_\d+[A-Za-z]*$', value=Article, regex=True)
    temp['rtype1'] = temp['rtype1'].replace(to_replace=schema + r'.+_Section(_\d+[A-Za-z]*)+$', value=Section,
                                            regex=True)
    temp['rtype1'] = temp['rtype1'].replace(to_replace=schema + r'^([A-Za-z]+_)+\d{4}_\d+$', value=LegalDocument,
                                            regex=True)
    temp['rtype1'] = temp['rtype1'].replace(to_replace='_.+$', value='', regex=True)
    temp = temp[temp['rtype1'].isin(
        [Norm, RuleAct, Concept, Article, Section, LegalDocument])]
    rule = pandas.concat([rule, temp.rename(columns={'uri': 'source'})])
    rule[['rtype2', 'rtype3', 'rel_name']] = [RuleExpression, 'owl:Thing', types]
    rule.loc[rule['rtype1'].isin([Article, Section, Items]), 'rtype2'] = LegalDocumentContent
    rule.loc[rule['rtype1'].isin([LegalDocument]), 'rtype2'] = ''
    type_triple = pandas.concat([rule[~rule['rtype0'].isnull()][['source', 'rel_name',
                                                                 'rtype0']].rename(columns={'rtype0': 'target'},
                                                                                   inplace=False),
                                 rule[['source', 'rel_name',
                                       'rtype1']].rename(columns={'rtype1': 'target'}, inplace=False),
                                 rule[['source', 'rel_name',
                                       'rtype2']].rename(columns={'rtype2': 'target'}, inplace=False),
                                 rule[['source', 'rel_name',
                                       'rtype3']].rename(columns={'rtype3': 'target'},
                                                         inplace=False)], ignore_index=True).drop_duplicates()
    g_nodes['sentence_id'] = data + g_nodes['sentence_id']
    LN_node = g_nodes[g_nodes['sentence_id'].str.contains('Number|Letter')][
        ['sentence_id']].drop_duplicates().reset_index()
    LN_node['rtype1'] = Items
    LN_node[['rtype2', 'rtype3', 'rel_name_1', 'rel_name_2', 'rel_name_3']] = [LegalDocumentContent, 'owl:Thing', types,
                                                                               schema + 'isPartOf', schema + 'hasPart']
    LN_type_triple = pandas.concat([LN_node[['sentence_id', 'rel_name_1',
                                             'rtype1']].rename(columns={'sentence_id': 'source',
                                                                        'rel_name_1': 'rel_name', 'rtype1': 'target'},
                                                               inplace=False),
                                    LN_node[['sentence_id', 'rel_name_1',
                                             'rtype2']].rename(columns={'sentence_id': 'source',
                                                                        'rel_name_1': 'rel_name', 'rtype2': 'target'},
                                                               inplace=False),
                                    LN_node[['sentence_id', 'rel_name_1',
                                             'rtype3']].rename(columns={'sentence_id': 'source',
                                                                        'rel_name_1': 'rel_name', 'rtype3': 'target'},
                                                               inplace=False)], ignore_index=True).drop_duplicates()
    has_rule_triple = roots[['sentence_id', 'uri']]
    has_rule_triple['rel_name'] = schema + 'hasRule'
    has_rule_triple['sentence_id'] = data + has_rule_triple['sentence_id']
    has_rule_triple = has_rule_triple.rename(columns={'sentence_id': 'source', 'uri': 'target'}, inplace=False)
    has_source_triple = has_rule_triple.rename(columns={'target': 'source', 'source': 'target'}, inplace=False)
    has_source_triple['rel_name'] = schema + 'isRuleOf'
    letter = g_nodes[(g_nodes['is_root_sentence'] != 1) & (g_nodes['type'] == 'root')]
    letter = letter.merge(g_nodes.add_suffix('_refer'), left_on='sentence_id', right_on='uri_refer')
    letter = letter[letter['uri_refer'].str.contains(r'_Letter_[A-Z]+_?\d*', regex=True)]
    letter_triple = letter[['sentence_id', 'sentence_id_refer']]
    letter_triple['label'] = '"Huruf ' + letter_triple['sentence_id'].str.extract(r'_Letter_([A-Z]+(?:_\d+)?)') + \
                             '"^^xsd:string'
    letter_triple['rel_name_1'] = schema + 'isPartOf'
    letter_triple['rel_name_2'] = schema + 'hasPart'
    letter_triple['rel_name_3'] = label
    letter_triple = pandas.concat([
        letter_triple[['sentence_id', 'rel_name_1',
                       'sentence_id_refer']].rename(columns={'sentence_id': 'source', 'rel_name_1': 'rel_name',
                                                             'sentence_id_refer': 'target'}, inplace=False),
        letter_triple[['sentence_id', 'rel_name_2',
                       'sentence_id_refer']].rename(columns={'sentence_id': 'target', 'rel_name_2': 'rel_name',
                                                             'sentence_id_refer': 'source'}, inplace=False),
        letter_triple[['sentence_id', 'rel_name_3',
                       'label']].rename(columns={'sentence_id': 'source', 'rel_name_3': 'rel_name',
                                                 'label': 'target'}, inplace=False)],
        ignore_index=True)
    letter = letter[['uri', 'id', 'id_refer', 'state']]
    letter = letter.merge(g_edges.add_suffix('_l'), left_on=['id', 'state'], right_on=['source_l', 'state_l'])
    letter = letter.dropna()
    letter = letter[letter['rel_name_l'].str.contains(schema + '(subject|act)$')]
    letter_in_triple = letter.merge(g_edges.add_suffix('_rin'), left_on=['id_refer', 'state'],
                                    right_on=['target_rin', 'state_rin'])
    letter_in_triple = letter_in_triple.merge(g_nodes.add_suffix('_vrin'), left_on='source_rin',
                                              right_on='id_vrin')[['uri_vrin', 'rel_name_rin', 'uri']]
    letter_in_triple = letter_in_triple.rename(columns={'uri_vrin': 'source', 'rel_name_rin': 'rel_name',
                                                        'uri': 'target'})
    version = g_nodes[g_nodes['sentence_id'].str.contains(r'_Number_\d+', regex=True)]
    version = version[['sentence_id']].drop_duplicates()
    version['target'] = version['sentence_id'].replace(to_replace=r'_Number_\d+', value='', regex=True)
    version['rel_name'] = schema + 'isPartOf'
    version1 = version.rename(columns={'sentence_id': 'source'}, inplace=False)
    version2 = version1.copy()
    version2['rel_name'] = schema + 'hasPart'
    version2 = version2.rename(columns={'target': 'source', 'source': 'target'}, inplace=False)
    version3 = version1.copy()
    version3['rel_name'] = label
    version3['target'] = '"Angka ' + version3['source'].str.extract(r'_Number_(\d+(?:_\d+)?)') + '"^^xsd:string'
    version_triple = pandas.concat([version1, version2, version3])
    sentences_triple = g_nodes[
        g_nodes['sentence_id'].str.contains(r'(_Number\d+_\d+|(ARTICLE|SECTION).*_[A-Z]+_\d+|_\d+_\d+_\d+)$',
                                            regex=True)]
    sentences_triple['target'] = sentences_triple['sentence_id'].replace(to_replace=r'_\d+$', value='', regex=True)
    sentences_triple['rel_name'] = schema + 'isPartOf'
    sentences_triple = sentences_triple[['sentence_id', 'rel_name', 'target']].rename(columns={'sentence_id': 'source'},
                                                                                      inplace=False)
    sentences_triple1 = sentences_triple.rename(columns={'source': 'target', 'target': 'source'}, inplace=False)
    sentences_triple1['rel_name'] = schema + 'hasPart'
    sentences_triple = pandas.concat([sentences_triple, sentences_triple1]).drop_duplicates()
    # todo: ini kalau di acc lanjut kalau nggak ya comment aja
    sentence_triples.loc[sentence_triples['rel_name'].str.contains(schema + 'and|or|xor|uconj',
                                                                   regex=True), 'rel_name'] = schema + 'hasElement'
    expanded_range = get_expand_range(sentence_triples[sentence_triples['rel_name'].str.contains('start|end')])
    sentence_triples = pandas.concat([sentence_triples, expanded_range], ignore_index=True)
    sentence_triples = sentence_triples[~sentence_triples['rel_name'].str.contains(schema + 'start|end', regex=True)]
    # todo: sampai sini
    if len(alias) > 0:
        res = pandas.concat([letter_triple, has_rule_triple, sentence_triples, aliases_triple, has_source_triple,
                             letter_in_triple, LN_type_triple, type_triple, label_triple, sentences_triple,
                             version_triple]).drop_duplicates()
    else:
        res = pandas.concat([letter_triple, has_rule_triple, sentence_triples, has_source_triple,
                             letter_in_triple, LN_type_triple, type_triple, label_triple, sentences_triple1,
                             version_triple]).drop_duplicates()
    return res


def get_abstract_triple(abstract_df, modifified, deleted, concept, refer):
    abstract_df['stype'] = 'Article_'
    abstract_df = abstract_df[~(abstract_df['value'].str.contains(r'(?i)^dihapus\W*$', regex=True))]
    abstract_df.loc[abstract_df['id'].str.contains(r'LX\d+[A-WYZ]*X\d+[A-WYZ]*'), 'stype'] = 'Section_'
    abstract_df['id'] = abstract_df.apply(lambda x: x['id'].replace('LX', x['stype']), axis=1)
    abstract_df['id'] = abstract_df['id'].replace(to_replace=r'[X_]V(?=\d)', value='_Number_', regex=True)
    abstract_df['id'] = abstract_df['id'].replace(to_replace='X(?=[A-Z])', value='_Letter_', regex=True)
    abstract_df['id'] = abstract_df['id'].replace(to_replace='X', value='_', regex=True)
    abstract_df['id'] = data + abstract_df['reg_id'] + '_' + abstract_df['id']
    abstract_df['id'] = abstract_df['id'].replace(to_replace='(?i)article', value='Article', regex=True)
    abstract_df['id'] = abstract_df['id'].replace(to_replace='(?i)section', value='Section', regex=True)
    abstract_df['count'] = abstract_df.groupby(['id'])['id'].transform('count')
    abstract_df['rank'] = abstract_df.groupby(['id'])['count'].rank(method='first')
    abstract_df = abstract_df[abstract_df['rank'] == 1]
    all_cx = abstract_df[abstract_df['value'].str.contains('CX')].copy()
    cx_node = all_cx.copy()
    while len(cx_node) > 0:
        cx_node['cx'] = cx_node['value'].str.extract(r'(CX[XV0-9]+N\d+)')
        cx_node = cx_node.merge(concept, left_on=['cx', 'reg_id'], right_on=['id', 'reg_id'])
        if len(cx_node) > 0:
            cx_node['count'] = cx_node.groupby(['id_x'])['id_x'].transform('count')
            cx_node['rank'] = cx_node.groupby(['id_x'])['count'].rank(method='first')
            cx_node = cx_node[cx_node['rank'] == 1].sort_values(['id_x'])
            all_cx = all_cx[all_cx['rank'] == 1].drop_duplicates().sort_values(['id'])
            cx_node['new_value'] = cx_node.apply(lambda x: x['value'].replace(x['cx'], x['text']), axis=1)
            all_cx.loc[all_cx['id'].isin(cx_node.id_x.array) & all_cx['value'].str.contains('CX', regex=True),
                       ['value']] = cx_node.loc[cx_node['id_x'].isin(cx_node.id_x.array), ['new_value']].values
            cx_node = all_cx[all_cx['value'].str.contains('CX')]

    abstract_df = abstract_df.drop_duplicates().sort_values(['id'])
    all_cx = all_cx[['id', 'value', 'reg_id']].drop_duplicates().sort_values(['id'])

    abstract_df.loc[abstract_df['id'].isin(all_cx.id.array) & abstract_df['value'].str.contains('CX', regex=True),
                    ['value']] = all_cx.loc[all_cx['id'].isin(all_cx.id.array), ['value']].values
    all_rx = abstract_df[abstract_df['value'].str.contains('RX')]
    rx_node = all_rx.copy()
    while len(rx_node) > 0:
        rx_node['rx'] = rx_node['value'].str.extract(r'(RX[XV0-9]+N\d+)')
        rx_node = rx_node.merge(refer, left_on=['rx', 'reg_id'], right_on=['id', 'reg_id']).sort_values(['id_x'])
        rx_node['count'] = rx_node.groupby(['id_x'])['id_x'].transform('count')
        rx_node['rank'] = rx_node.groupby(['id_x'])['count'].rank(method='first')
        rx_node = rx_node[rx_node['rank'] == 1].sort_values(['id_x'])
        all_rx = all_rx.sort_values(['id'])
        rx_node = rx_node.drop_duplicates().dropna()
        if len(rx_node) > 0:
            rx_node['new_value'] = rx_node.apply(lambda x: x['value_x'].replace(x['rx'], x['text']), axis=1)
            rx_node = rx_node.drop_duplicates()
            all_rx.loc[all_rx['id'].isin(rx_node.id_x.array),
                       ['value']] = rx_node.loc[rx_node['id_x'].isin(rx_node.id_x.array), ['new_value']].values
            rx_node = all_rx[all_rx['value'].str.contains('RX')]
    abstract_df.loc[abstract_df['id'].isin(all_rx.id.array),
                    ['value']] = all_rx.loc[all_rx['id'].isin(all_rx.id.array), ['value']].values
    abstract_df['value'] = '\"\"\"' + abstract_df['value'] + '\"\"\"^^xsd:string'
    abstract_df['rel_name'] = description
    if len(modifified) > 0:
        abstract_df['article_section'] = abstract_df['id'].replace(to_replace='lexid:|_(Number|Letter).+', value='',
                                                                   regex=True)
        abstract_df['suffix'] = ''
        modifified['suffix'] = modifified['target'].str.extract('(_Modified_By_.*$)')
        abstract_df = abstract_df.merge(modifified, left_on='article_section', right_on='source', how='left')
        abstract_df['suffix'] = abstract_df['suffix_y'].combine_first(abstract_df['suffix_x'])
        abstract_df['id'] = abstract_df['id'] + abstract_df['suffix']
    abstract_df = abstract_df[['id', 'rel_name', 'value']].rename(columns={'id': 'source', 'value': 'target'})
    return abstract_df


def get_opening_part(considerans, law_bases, dictum, reg_id_funct):
    consider = pandas.DataFrame(considerans, columns=['target'])
    consider['source'] = data + reg_id_funct
    consider['target'] = '"' + consider['target'] + '"^^xsd:string'
    consider['rel_name'] = schema + 'considers'
    based_on = pandas.DataFrame(law_bases, columns=['target'])
    based_on['source'] = data + reg_id_funct
    based_on = based_on[based_on['target'].str.contains(r'\w')]
    based_on['target'] = data + based_on['target']
    based_on['rel_name'] = schema + 'hasLegalBasis'
    based_on2 = based_on.rename(columns={'source': 'target', 'target': 'source'}, inplace=False)
    based_on2['rel_name'] = schema + 'LegalBasisOf'
    lbases_type = based_on.rename(columns={'source': 'target', 'target': 'source'}, inplace=False)
    lbases_type['rel_name'] = types
    lbases_type['target'] = LegalDocument
    lbasis_label = based_on2[['source', 'rel_name']]
    lbasis_label['code'] = lbasis_label['source'].str.extract(r'lexid:([a-zA-Z_]+)_\d')
    lbasis_label['year'] = 'Tahun ' + lbasis_label['source'].str.extract(r'_(\d+)_')
    lbasis_label['num'] = 'Nomor ' + lbasis_label['source'].str.extract(r'(\d+)$')
    lbasis_label = lbasis_label.merge(reg_types, left_on='code', right_on='code')
    lbasis_label = lbasis_label[['source', 'rel_name', 'formalTyping', 'num', 'year']]
    lbasis_label['target'] = '"' + lbasis_label['formalTyping'] + ' ' + lbasis_label['num'] + ' ' + lbasis_label['year'] \
                             + '"^^xsd:string'
    lbasis_label['rel_name'] = label
    lbasis_label = lbasis_label[['source', 'rel_name', 'target']].drop_duplicates()

    opening_triples = pandas.concat([consider, based_on, based_on2, lbases_type, lbasis_label])
    opening_triples = opening_triples.append({'source': data + reg_id_funct, 'rel_name': schema + 'hasDictum',
                                              'target': '"' + dictum + '"^^xsd:string'}, ignore_index=True)
    return opening_triples


def text_to_date(text_date):
    months = {'januari': '1', 'februari': '2', 'maret': '3', 'april': '4', 'mei': '5', 'juni': '6', 'juli': '7',
              'agustus': '8', 'september': '9', 'oktober': '10', 'november': '11', 'desember': '12'}
    try:
        text_date = text_date.lower()
        date_f = re.search(r'(\d{1,2})\s+([a-z]+)\s+(\d{4})', text_date)
        day = int(date_f.group(1))
        month = int(months[date_f.group(2)])
        year = int(date_f.group(3))
        return str(datetime(year=year, month=month, day=day).date())
    except Exception:
        return None


def get_label_regulatory(rid):
    try:
        rtype = re.search(r'^([A-Za-z_\d]+)_\d+_\d+$', rid).group(1).strip()
        rtype = reg_types.loc[reg_types['code'] == rtype].reset_index().to_dict()
        year = re.search(r'_(\d+)_', rid).group(1).strip()
        num = re.search(r'(\d+)$', rid).group(1).strip()
        return rtype['formalTyping'][0] + ' Nomor ' + num + ' Tahun ' + year
    except AttributeError:
        rtype = re.search(r'^(UUD)_1945$', rid).group(1).strip()
        rtype = reg_types.loc[reg_types['code'] == rtype].reset_index().to_dict()
        year = re.search(r'_(\d+)$', rid).group(1).strip()
        return rtype['formalTyping'][0] + ' Tahun ' + year


def get_reg_triple(title, wd_cache, promulgation=None, enactment=None):
    rid = title['reg_id']
    reg_type = reg_types.loc[reg_types['type'] == title['type']].reset_index().to_dict()
    res = [{'source': data + rid, 'rel_name': types, 'target': schema + reg_type['type1'][0]},
           {'source': data + rid, 'rel_name': types, 'target': LegalDocument},
           {'source': data + rid, 'rel_name': types, 'target': 'owl:Thing'}, ]
    if reg_type['code'][0] not in ['UUD', 'Amandemen_1_UUD', 'Amandemen_2_UUD', 'Amandemen_3_UUD', 'Amandemen_4_UUD']:
        res.append({'source': data + rid, 'rel_name': label,
                    'target': '"' + reg_type['formalTyping'][0] + ' Nomor ' + title['number'] + ' Tahun ' + title[
                        'year'] + '"^^xsd:string'})
    if reg_type['code'][0] in ['UUD', 'Amandemen_1_UUD', 'Amandemen_2_UUD', 'Amandemen_3_UUD', 'Amandemen_4_UUD']:
        res.append({'source': data + rid, 'rel_name': label,
                    'target': '"' + reg_type['formalTyping'][0] + ' Tahun ' + title['year'] + '"^^xsd:string'})
    print(res)
    if title.keys().__contains__('number'):
        res.append({'source': data + rid, 'rel_name': schema + 'regulationNumber',
                    'target': '"' + title['number'] + '"^^xsd:string'})
    if title.keys().__contains__('change'):
        res.append({'source': data + rid, 'rel_name': 'lexid-s:amends', 'target': 'lexid:' + title['change']})
        res.append({'source': data + title['change'], 'rel_name': 'lexid-s:amendedBy', 'target': 'lexid:' + rid})
        res.append({'source': data + title['change'], 'rel_name': types, 'target': LegalDocument})
        res.append({'source': data + title['change'], 'rel_name': label,
                    'target': '"' + get_label_regulatory(title['change']) + '"^^xsd:string'})
    if title.keys().__contains__('repeal'):
        if len(title['repeal']) > 0:
            if isinstance(title['repeal'], dict):
                for article in title['repeal'].keys():
                    id_article = rid + '_' + re.sub('SECTION', 'Section', re.sub('ARTICLE', 'Article', article.title()))
                    for repeal in title['repeal'][article]:
                        if repeal == '':
                            continue
                        res.append(
                            {'source': data + id_article, 'rel_name': schema + 'repeals', 'target': data + repeal})
                        res.append(
                            {'source': data + repeal, 'rel_name': schema + 'repealedBy', 'target': data + id_article})
                        res.append({'source': data + rid, 'rel_name': schema + 'repeals', 'target': data + repeal})
                        res.append({'source': data + repeal, 'rel_name': schema + 'repealedBy', 'target': data + rid})
                        res.append({'source': data + repeal, 'rel_name': types, 'target': LegalDocument})
                        res.append({'source': data + repeal, 'rel_name': label,
                                    'target': '"' + get_label_regulatory(repeal) + '"^^xsd:string'})
                        if re.search('(?i)article', article):
                            res.append({'source': data + id_article, 'rel_name': types, 'target': Article})
                        else:
                            res.append({'source': data + id_article, 'rel_name': types, 'target': Section})
            else:
                for repeal in title['repeal']:
                    res.append({'source': data + rid, 'rel_name': schema + 'repeals', 'target': data + repeal})
                    res.append({'source': data + repeal, 'rel_name': schema + 'repealedBy', 'target': data + rid})
                    res.append({'source': data + repeal, 'rel_name': types, 'target': LegalDocument})
                    res.append({'source': data + repeal, 'rel_name': label,
                                'target': '"' + get_label_regulatory(repeal) + '"^^xsd:string'})

    if title.keys().__contains__('implement'):
        if isinstance(title['implement'], list):
            implements = title['implement']
        else:
            implements = [title['implement']]
        for implement in implements:
            if implement == '':
                continue
            res.append({'source': data + rid, 'rel_name': 'lexid-s:implements', 'target': data + implement})
            res.append(
                {'source': data + implement, 'rel_name': schema + 'implementedBy', 'target': data + rid})
            if re.search('(?i)article', implement):
                res.append({'source': data + implement, 'rel_name': types, 'target': Article})
            elif re.search('(?i)section', implement):
                res.append({'source': data + implement, 'rel_name': types, 'target': Section})
            else:
                res.append({'source': data + implement, 'rel_name': types, 'target': LegalDocument})
                res.append({'source': data + implement, 'rel_name': label,
                            'target': '"' + get_label_regulatory(implement) + '"^^xsd:string'})
                get_label_regulatory(implement)
    if title.keys().__contains__('year'):
        res.append({'source': data + rid, 'rel_name': schema + 'regulationYear',
                    'target': '"' + title['year'] + '"^^xsd:int'})
    if title.keys().__contains__('name'):
        res.append({'source': data + rid, 'rel_name': schema + 'name',
                    'target': '"' + title['name'].strip().title() + '"^^xsd:string'})
    if title.keys().__contains__('official'):
        temp, wd_cache = wikidata_linking(data + rid, schema + 'hasCreator',
                                          re.sub(r'^\W+|\W+$', '', title['official'].title()),
                                          wd_cache, Position)
        res += temp
    if enactment is not None and len(enactment) > 0:
        if enactment.keys().__contains__('location'):
            try:
                location = re.search(r'^(([A-Z][a-z]+[\W\s\n]*)+)', enactment['location']).group(1).strip()
                temp, wd_cache = wikidata_linking(data + rid, schema + 'hasEnactionLocation', location.title(), wd_cache,
                                                  City)
                res += temp
            except AttributeError:
                pass
        if enactment.keys().__contains__('date'):
            date_str = text_to_date(enactment['date'])
            if date_str is not None:
                res.append({'source': data + rid, 'rel_name': schema + 'hasEnactionDate',
                            'target': '"' + date_str + '"^^xsd:date'})
        if enactment.keys().__contains__('official'):
            temp, wd_cache = wikidata_linking(data + rid, schema + 'hasEnactionOffice', enactment['official'].title(),
                                              wd_cache, Position)
            res += temp
        if enactment.keys().__contains__('official_name'):
            temp, wd_cache = wikidata_linking(data + rid, schema + 'hasEnactionOfficial',
                                              enactment['official_name'].title(),
                                              wd_cache, Person)
            res += temp
    if promulgation is not None and len(promulgation) > 0:
        if promulgation.keys().__contains__('location'):
            try:
                location = re.search(r'^(([A-Z][a-z]+[\W\s\n]*)+)', promulgation['location']).group(1).strip()
                temp, wd_cache = wikidata_linking(data + rid, schema + 'hasPromulgationLocation', location.title(),
                                                  wd_cache, City)
                res += temp
            except AttributeError:
                pass
        if promulgation.keys().__contains__('date'):
            date_str = text_to_date(promulgation['date'])
            if date_str is not None:
                res.append({'source': data + rid, 'rel_name': schema + 'hasPromulgationDate',
                            'target': '"' + date_str + '"^^xsd:date'})
        if promulgation.keys().__contains__('official'):
            temp, wd_cache = wikidata_linking(data + rid, schema + 'hasPromulgationOffice',
                                              promulgation['official'].title(),
                                              wd_cache, Position)
            res += temp
        if promulgation.keys().__contains__('official_name'):
            temp, wd_cache = wikidata_linking(data + rid, schema + 'hasPromulgationOfficial',
                                              promulgation['official_name'].title(),
                                              wd_cache, Person)
            res += temp
    try:
        res += [
            {'source': data + rid, 'rel_name': schema + 'hasPromulgationPlace', 'target': data + reg_type['type2'][0]}]
    except Exception:
        pass
    res = pandas.DataFrame(res).drop_duplicates()
    return res, wd_cache


def wikidata_linking(source, rels, target, wd_cache, target_type):
    res = []
    target_node = re.sub('-s:', ':', target_type) + '_' + re.sub(r'\W+', '_', target.title())
    target_label = re.sub(r'\W+', ' ', target.title())
    res.append({'source': target_node, 'rel_name': label, 'target': '"' + target_label + '"^^xsd:string'})
    res.append({'source': source, 'rel_name': rels, 'target': target_node})
    res.append({'source': target_node, 'rel_name': types, 'target': target_type})
    res.append({'source': target_node, 'rel_name': types, 'target': 'owl:Thing'})
    targetCache = wd_cache.loc[wd_cache['object'] == target]
    if targetCache.size != 0:
        url = 'https://query.wikidata.org/sparql'
        query = '''
            SELECT distinct ?item ?itemLabel WHERE{{
            ?item ?label "{target}"@id.
            ?article schema:about ?item .
            ?article schema:inLanguage "en" .
            ?article schema:isPartOf <https://en.wikipedia.org/>.
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "id". }}
            }}
            '''
        target = re.sub(r'\W+', ' ', target).strip()
        r = requests.get(url, params={'format': 'json', 'query': query.format(target=target)})
        if r.ok:
            response = r.json()['results']['bindings']
            for data_funct in response:
                target_uri = 'wd:' + re.search(r'Q\d+$', str(data_funct['item']['value'])).group(0)
                wd_cache = wd_cache.append([{'object': target, 'uri': target_uri}], ignore_index=True)
                res.append({'source': target_node, 'rel_name': sameAs, 'target': target_uri})
                res.append({'source': target_uri, 'rel_name': sameAs, 'target': target_node})
            return res, wd_cache
        else:
            return [], wd_cache
    else:
        for target_uri in targetCache.iterrows():
            res.append({'source': target_node, 'rel_name': sameAs, 'target': target_uri[1]['uri']})
            res.append({'source': target_uri[1]['uri'], 'rel_name': sameAs, 'target': target_node})
    return res, wd_cache


file_list = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')
global reg_types, prefixs
reg_types = pandas.read_csv('regulatory_type.csv')
reg_types['old_code'] = reg_types['code'].str.upper()
reg_types['old_code'] = reg_types['old_code'].replace(to_replace='^PB', value='PBERSAMA', regex=True)
reg_types['old_code'] = reg_types['old_code'].replace(to_replace='^UUD', value='UUD1945', regex=True)
prefixs = pandas.read_csv('prefixs.csv').fillna('')
resMap = pandas.read_csv('E:/Ninggar/Mgstr/Penelitian/Data/files/sentence_triple/turtle_map.csv').fillna('')
state = True
wdCache = pandas.read_csv('C:/Users/ningg/PycharmProjects/crawling/crawl/wikidata_cache.csv').fillna(
    '').drop_duplicates()
file_num = 0
all_node, all_edge, all_refer, all_alias, all_concept, all_abstract, all_potriple = [], [], [], [], [], [], []
all_modifiedContent, all_deletedContent, titles = [], [], []
deletedContents = []
all_optriple, all_regulatory = [], []
dumps = 0
start = time.time()
reg_ids = []
isForEval = False
isForAmendment = True
targetDir = ''
filetarget = ''
existed_content = None
if isForAmendment:
    existed_content = pandas.read_csv('existedContentAmendment.csv')
else:
    targetDir = 'sentence_triple'
    filterDoc = None
while state:
    file_num += 1
    text = file_list.readline()
    print(file_num, text)
    if text == '':
        if len(all_node) > 0:
            if not isForEval:
                resMap.loc[resMap['regulatory'].isin(reg_ids), 'ttl_file'] = re.sub(
                    'E:/Ninggar/Mgstr/Penelitian/Data/files/{targetDir}/'.format(targetDir=targetDir), '', filetarget)
            else:
                resMap.loc[resMap['regulatory'].isin(reg_ids), 'ttl_file'] = filetarget.format(targetDir=targetDir)
            nodes = pandas.concat(all_node)
            edges = pandas.concat(all_edge)
            refers = pandas.concat(all_refer)
            concepts = pandas.concat(all_concept)
            aliases = pandas.concat(all_alias)
            abstracts = pandas.concat(all_abstract)
            abstracts['id'] = abstracts['id'].replace(to_replace=r'\s+', value='', regex=True)
            G = {'V': nodes, 'E': edges}
            modifiedContents = pandas.concat(all_modifiedContent)
            all_modifiedContent.append(modifiedContents)
            all_deletedContent.append(deletedContents)
            try:
                deletedContents = pandas.concat(all_deletedContent)
            except Exception:
                deletedContents = None
            G = postag_handle(G, modifiedContents, deletedContents)
            sentence_triple = get_sentence_triple(graph=G, concept=concepts, refer=refers, alias=aliases)
            abstract_triple = get_abstract_triple(abstracts, modifiedContents, deletedContents, concept=concepts,
                                                  refer=refers)
            partof_triple = pandas.concat(all_potriple)
            opening_triple = pandas.concat(all_optriple)
            regulatory_triple = pandas.concat(all_regulatory)
            all_triple = pandas.concat([sentence_triple, abstract_triple, partof_triple, opening_triple,
                                        regulatory_triple])
            all_triple['target'] = all_triple['target'] + ' .'
            all_triple = all_triple[all_triple['source'].str.contains(r'^([a-z]+(\-[a-z]+)?:)', regex=True) &
                                    all_triple['target'].str.contains(r'^(\"|\'|[a-z]+(\-[a-z]+)?:)', regex=True)]
            all_triple = pandas.concat([prefixs, all_triple])[['source', 'rel_name', 'target']]
            print(filetarget)
            numpy.savetxt(filetarget, all_triple.values, fmt='%s', delimiter=' ')
            print(filetarget)
            resMap.sort_values('regulatory', ascending=True).to_csv(
                'E:/Ninggar/Mgstr/Penelitian/Data/files/{targetDir}/turtle_map.csv'.format(targetDir=targetDir),
                index=False)
            dumps = 0
            all_node, all_edge, all_refer, all_alias, all_concept, all_abstract = [], [], [], [], [], []
            all_potriple = []
            all_optriple, all_regulatory = [], []
            end = time.time()
            print(end - start)
            start = time.time()
        state = False
    filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_parsed_files/' + (
        re.sub('_pdf', '.json', re.sub(r'(%20)|([\\._]+)', '_',
                                       re.sub(r'^http(s?)://peraturan\\.go\\.id/common/dokumen/', '',
                                              text)))).lower().strip()
    filetarget = 'E:/Ninggar/Mgstr/Penelitian/Data/files/{targetDir}/'.format(targetDir=targetDir) + (
        re.search(r'(.{,100})', re.sub('_pdf', '', re.sub(r'(%20)|([\\._]+)', '_',
                                                          re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                 '', text)))).group(1).lower()) + '.ttl'
    dirName = re.sub('/[^/]+$', '', filetarget)
    try:
        # Create target Directory
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    except Exception:
        pass
    try:
        file = json.load(open(filesource, 'r'))
    except Exception:
        continue
    if re.search('putusan', filesource):
        continue

    try:
        reg_id = file['title']['reg_id']
        reg_ids.append(reg_id)
        print(reg_id)
        print(file['title'])
        if not file['title'].keys().__contains__('change'):
            partof_triple = pandas.DataFrame(get_body_values(file['body'], file['title'], reg_id))
            modifiedContents = pandas.DataFrame([])
            deletedContents = pandas.DataFrame([])
        else:
            segment_table = existed_content[existed_content['reg_id'] == reg_id]
            partof_triple, modifiedContents, deletedContents = get_amend_body_values(file['body'], segment_table,
                                                                                     reg_id,
                                                                                     file['title']['change'],
                                                                                     file['values'])
        opening_triple = get_opening_part(file['considerans'], file['law_based'], file['dictum'], reg_id)
        if file.keys().__contains__('closing_part'):
            regulatory_triple, wdCache = get_reg_triple(file['title'], wdCache, file['closing_part']['promulgation'],
                                                        file['closing_part']['enactment'])
        else:
            regulatory_triple, wdCache = get_reg_triple(file['title'], wdCache)
        if titles.__contains__(reg_id):
            continue
        else:
            titles.append(reg_id)
        nodes = pandas.DataFrame(file['nodes'])
        edges = pandas.DataFrame(file['edges'])
        refers = pandas.DataFrame(file['refers'])
        aliases = pandas.DataFrame(file['aliases'])
        concepts = pandas.DataFrame(file['concepts'])
        abstracts = pandas.DataFrame(file['values'])
        if len(nodes) == 0 or len(edges) == 0:
            continue
    except Exception:
        Exception.with_traceback()
        continue
    if not file['title'].keys().__contains__('change'):
        legal_id = reg_id
    else:
        legal_id = file['title']['change']
    print(reg_id)
    nodes['sentence_id'] = legal_id + '_' + nodes['sentence_id'].replace(to_replace=r'\s+', value='', regex=True)
    nodes['sentence_id'] = nodes['sentence_id'].replace(to_replace=r'(?i)article', value='Article', regex=True)
    nodes['sentence_id'] = nodes['sentence_id'].replace(to_replace=r'(?i)section', value='Section', regex=True)
    edges['sentence_id'] = legal_id + '_' + edges['sentence_id'].replace(to_replace=r'\s+', value='', regex=True)
    edges['sentence_id'] = edges['sentence_id'].replace(to_replace=r'(?i)article', value='Article', regex=True)
    edges['sentence_id'] = edges['sentence_id'].replace(to_replace=r'(?i)section', value='Section', regex=True)
    nodes['suffix'] = reg_id
    edges['suffix'] = reg_id
    refers['reg_id'] = legal_id
    concepts['reg_id'] = legal_id
    aliases['reg_id'] = legal_id
    abstracts['reg_id'] = legal_id
    all_node.append(nodes)
    all_edge.append(edges)
    all_refer.append(refers)
    all_concept.append(concepts)
    all_alias.append(aliases)
    all_abstract.append(abstracts)
    all_potriple.append(partof_triple)
    all_modifiedContent.append(modifiedContents)
    all_deletedContent.append(deletedContents)
    all_optriple.append(opening_triple)
    all_regulatory.append(regulatory_triple)
    dumps += 1
    if resMap is None:
        if not isForEval:
            resMap = pandas.DataFrame([{'regulatory': reg_id,
                                        'ttl_file': re.sub('E:/Ninggar/Mgstr/Penelitian/Data/files/{targetDir}/'.format(
                                            targetDir=targetDir), '',
                                            filetarget)}])
        else:
            resMap = pandas.DataFrame([{'regulatory': reg_id,
                                        'ttl_file': filetarget.format(targetDir=targetDir)}])

    elif resMap[resMap['regulatory'] == reg_id].empty:
        if not isForEval:
            resMap = resMap.append([{'regulatory': reg_id,
                                     'ttl_file': re.sub(
                                         'E:/Ninggar/Mgstr/Penelitian/Data/files/{targetDir}/'.format(
                                             targetDir=targetDir),
                                         '',
                                         filetarget)}], ignore_index=True)
        else:
            resMap = resMap.append([{'regulatory': reg_id,
                                     'ttl_file': filetarget.format(targetDir=targetDir)}], ignore_index=True)
    if dumps == 10:
        if not isForEval:
            resMap.loc[resMap['regulatory'].isin(reg_ids), 'ttl_file'] = re.sub(
                'E:/Ninggar/Mgstr/Penelitian/Data/files/{targetDir}/'.format(targetDir=targetDir), '', filetarget)
        else:
            resMap.loc[resMap['regulatory'].isin(reg_ids),
                       'ttl_file'] = filetarget.format(targetDir=targetDir)
        nodes = pandas.concat(all_node)
        edges = pandas.concat(all_edge)
        refers = pandas.concat(all_refer)
        if not refers.empty:
            refers['chapter'] = refers['value'].str.extract('(?i)(change|chapter|part|paragraph|article|section)')
            refers['temp'] = refers['reg_id'] + '_' + refers['chapter'].str.title() + \
                             refers['value'].replace(to_replace=r'(?i)^.+?(change|chapter|part|paragraph|article|' +
                                                                'section)', value='', regex=True)
            null_chapter = refers[refers['chapter'].isnull()][['value']].drop_duplicates()
            null_chapter['old_code'] = null_chapter['value'].replace(to_replace=r'(_\d+)+', value='', regex=True)
            null_chapter = null_chapter.merge(reg_types, left_on='old_code', right_on='old_code')[
                ['value', 'old_code', 'code']].drop_duplicates()
            refers = refers.merge(null_chapter, how='left', left_on='value', right_on='value')
            refers.loc[refers['temp'].isnull(), 'temp'] = refers['code'] + '_' + refers[refers['temp'].isnull()][
                'value'].replace(to_replace=r'^([^\d_][^_]+_)+', value='', regex=True)
            refers['temp'] = refers['temp'].replace(to_replace=r'_(?=[A-Z](_|$))|_(?=[A-Z][A-Z](_|$))',
                                                    value='_Letter_', regex=True)
            refers['value'] = refers['temp']
            refers = refers[['id', 'text', 'value', 'reg_id']]
            refers.loc[refers['value'].isnull(), 'value'] = ''
        concepts = pandas.concat(all_concept)
        aliases = pandas.concat(all_alias)
        abstracts = pandas.concat(all_abstract)
        abstracts['id'] = abstracts['id'].replace(to_replace=r'\s+', value='', regex=True)
        G = {'V': nodes, 'E': edges}
        modifiedContents = pandas.concat(all_modifiedContent)
        try:
            deletedContents = pandas.concat(all_deletedContent)
        except Exception:
            deletedContents = None
        G = postag_handle(G, modifiedContents, deletedContents)
        sentence_triple = get_sentence_triple(graph=G, concept=concepts, refer=refers, alias=aliases)
        abstract_triple = get_abstract_triple(abstracts, modifiedContents, deletedContents, concept=concepts,
                                              refer=refers)
        partof_triple = pandas.concat(all_potriple)
        opening_triple = pandas.concat(all_optriple)
        regulatory_triple = pandas.concat(all_regulatory)
        all_triple = pandas.concat([sentence_triple, abstract_triple, partof_triple, opening_triple,
                                    regulatory_triple])
        all_triple['target'] = all_triple['target'] + ' .'
        all_triple = all_triple[all_triple['source'].str.contains(r'^([a-z]+(\-[a-z]+)?:)', regex=True) &
                                all_triple['target'].str.contains(r'^(\"|\'|[a-z]+(\-[a-z]+)?:)', regex=True)]
        all_triple = pandas.concat([prefixs, all_triple])[['source', 'rel_name', 'target']]
        numpy.savetxt(filetarget, all_triple.values, fmt='%s', delimiter=' ')
        print(filetarget)
        resMap.sort_values('regulatory', ascending=True).to_csv(
            'E:/Ninggar/Mgstr/Penelitian/Data/files/{targetDir}/turtle_map.csv'.format(targetDir=targetDir),
            index=False)
        wdCache.sort_values('object', ascending=True).to_csv(
            'C:/Users/ningg/PycharmProjects/crawling/crawl/wikidata_cache.csv', index=False)
        dumps = 0
        all_node, all_edge, all_refer, all_alias, all_concept, all_abstract, all_potriple = [], [], [], [], [], [], []
        all_modifiedContent, all_deletedContent, titles = [], [], []
        all_optriple, all_regulatory = [], []
        end = time.time()
        print(end - start)
        start = time.time()
        reg_ids = []
end = time.time()
print(end - start)
