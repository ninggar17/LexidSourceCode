import json
import os
import re

import pandas
import stanza

pandas.set_option('display.max_colwidth', 300)
pandas.set_option('display.width', None)


def get_title(texts, is_doc=True):
    try:
        if is_doc:
            filtered_text = re.search(r'((PERUBAHAN[\s\n]+(PERTAMA|KE(DUA|TIGA|EMPAT))[\s\n]+)?' +
                                      r'(PENETAPAN|PERATURAN|UNDANG-UNDANG|KEPUTUSAN)[A-Z0-9\W\s\n]+' +
                                      r'((?i:menlhk-setjen|MENHUT|MUT|permentan|per))?[A-Z0-9\W\s\n]+' +
                                      r'(DENGAN\sRAHMAT\sTUHAN\sYANG\sMAHA\sESA)?[A-Z0-9\W\s\n]+(,?))[\s\n]*Menimbang',
                                      texts).group(1)
        else:
            filtered_text = texts
    except Exception:
        try:
            filtered_text = re.search(
                r'(PERUBAHAN[\s\n]+(PERTAMA|KE(DUA|TIGA|]\s+IV))[\s\n]+)?UNDANG[\s\n]*-[\s\n]*UNDANG[\s\n]+DASAR[\s\n]+'
                r'NEGARA[\s\n]+REPUBLIK[\s\n]+INDONESIA[\s\n]+TAHUN[\s\n]+1945', texts).group(1)

        except Exception:
            return ''
    res_funct = {}
    try:
        res_funct = {'type': re.sub(r'[\n\s.]+|PM|NOMOR|PER/', ' ', re.search(r'^([A-Z/,.\-\s\n]+)(PM\.|NOMOR|PER/)',
                                                                        filtered_text).group(1)).strip()}
    except Exception:
        try:
            res_funct = {'type': re.sub(r'[\n\s.]+', ' ', re.search(r'^((PERUBAHAN[\s\n]+(PERTAMA|KE(DUA|TIGA|EMPAT))'
                                                                    r'[\s\n]+)?UNDANG[\s\n]*-[\s\n]*UNDANG[\s\n]+' +
                                                                    r'DASAR[\s\n]+NEGARA[\s\n]+REPUBLIK[\s\n]+' +
                                                                    r'INDONESIA[\s\n]+)TAHUN',
                                                                    filtered_text).group(1).strip())}
        except Exception:
            pass

    try:
        res_funct['number'] = re.sub(r'[\n\s]+', ' ', re.search(r'(NOMOR|PM\.|PER/)[\n\s/]*((:|[A-Za-z]+[\s\n]?[-.]?|' +
                                                                r'[^/\d]+)[\n\s]*)?(\d+)',
                                                                filtered_text).group(4).strip())
    except Exception:
        pass
    try:
        res_funct['year'] = re.sub(r'[\n\s]+', ' ',
                             re.search(r'(TAHUN|/)[\n\s]*(\d{4})(([\s\n]+T[\s\n]*E[\s\n]*N[\s\n]*' +
                                       r'T[\s\n]*A[\s\n]*N[\s\n]*G)?)', filtered_text).group(2).strip())
    except Exception:
        pass
    try:
        res_funct['official'] = re.search(
            r'\n\s*((PJS|PRESIDEN|MENTERI|WALI KOTA|BUPATI|WALIKOTA|GUBERNUR|LEMBAGA|KEPALA|KETUA|DEWAN|BADAN|JAKSA)' +
            r'[A-Z/,\s\n]+),?[\s\n]+$',
            filtered_text).group(1).strip()
    except Exception:
        res_funct['official'] = ''
    try:
        res_funct['name'] = re.sub('DENGAN RAHMAT TUHAN YANG MAHA ESA', '',
                                   re.sub(r'[\n\s]+', ' ',
                                          re.search(r'(T[\s\n]*E[\s\n]*N[\s\n]*T[\s\n]*A[\s\n]*N[\s\n]*G)' +
                                                    r'([\w\W\n\s]+)' + res_funct['official'],
                                                    filtered_text).group(2).strip())).strip()
        res_funct['official'] = re.sub(r'[\n\s]+', ' ', res_funct['official']).strip()
    except Exception:
        pass
    return res_funct


def get_considerans(texts):
    try:
        filtered_text = re.search(r'Menimbang\s*:([\w\W\n]+)Mengingat\s*:', texts).group(1).strip()
    except Exception:
        return []
    res_funct = re.split(';', filtered_text)
    for idx in range(len(res_funct)):
        res_funct[idx] = re.sub(r'[\n\s]+', ' ', re.sub(r'([\s\n]+|^)[a-z]\.', ' ', res_funct[idx])).strip()
    try:
        res_funct.remove('')
    except Exception:
        pass
    return res_funct


def get_law_based(texts):
    try:
        filtered_text = re.search(r'(Mengingat|Memperhatikan)\s*:([\w\W\n]+)MEMUTUSKAN', texts).group(2).strip()
    except Exception:
        return []
    res_funct = re.split(';', filtered_text)
    for idx in range(len(res_funct)):
        res_funct[idx] = re.sub(r'[\n\s]+', ' ', re.sub(r'([\s\n]+|^)\d\.', ' ', res_funct[idx])).strip()
        res_funct[idx] = refers_reformat(res_funct[idx])
        res_funct[idx] = re.sub(r'_(ARTICLE|SECTION|CHAPTER).+$', '', res_funct[idx])
    try:
        res_funct.remove('')
    except Exception:
        pass
    return res_funct


def get_dictum(texts):
    try:
        filtered_text = re.sub('(BAB|Pasal).+$', '', re.sub(r'[\n\s]+', ' ',
                               re.search(r'(MEMUTUSKAN[\s\n]*:)?([\s\n]*(Menetapkan|MENETAPKAN)[^.]+\.)',
                                         texts).group(2))).strip()
    except Exception:
        return ''
    return filtered_text


def get_body_part(texts, regulatory_id, aliases, concepts, refers, values, nodes, edges, is_change_reg=False):
    texts = re.sub(r'\n\s*(Agar\s*)?(supaya\s*)?setiap\s*orang\s*(yang\s*)?(dapat\s*)?mengetahui(nya)?[\w\W\n]+', '',
                   texts).strip()
    if is_change_reg:
        added_pattern = r'(\s*[A-Z]{1,2})?'
    else:
        added_pattern = '()'
    filtered_chapters = re.finditer(r'\n\s*(BAB[\s\n]*(([IVXLCDM]+){}))\s*\n'.format(added_pattern), texts)
    chapters = {}
    temp = None
    for chapter in filtered_chapters:
        if len(chapters) > 0:
            chapters[temp]['end'] = chapter.start(1)
        temp = regulatory_id + '_CHAPTER_' + chapter.group(2)
        chapters[temp] = {'start': chapter.start(1), 'key': chapter.end(1) - chapter.start(1)}
    for key in chapters.keys():
        try:
            chapter_text = texts[chapters[key]['start']:chapters[key]['end']]
        except Exception:
            chapter_text = texts[chapters[key]['start']:]
        filtered_part = re.finditer(r'\n\s*(Bagian[\s\n]*([Kk]e(se(puluh|belas|ratus|mbilan)|satu|dua|tiga|empat|' +
                                    r'lima|enam|tujuh|delapan)[ a-zA-Z]*))\s*\n',
                                    chapter_text)
        chapters[key]['parts'] = dict()
        for part in filtered_part:
            if len(chapters[key]['parts']) > 0:
                chapters[key]['parts'][temp]['end'] = part.start(1)
            temp = key.replace('CHAPTER', 'PART') + '_' + part.group(2).upper().strip()
            chapters[key]['parts'][temp] = {'start': part.start(1), 'key': part.end(1) - part.start(1)}

        for part_key in chapters[key]['parts'].keys():
            try:
                part_text = chapter_text[
                            chapters[key]['parts'][part_key]['start']:chapters[key]['parts'][part_key]['end']]
            except Exception:
                part_text = chapter_text[chapters[key]['parts'][part_key]['start']:]
            filtered_pgph = re.finditer(r'\n\s*(Paragraf[\s\n]*(\d+))\s*\n', part_text)
            chapters[key]['parts'][part_key]['paragraphs'] = {}
            for pgph in filtered_pgph:
                if len(chapters[key]['parts'][part_key]['paragraphs']) > 0:
                    chapters[key]['parts'][part_key]['paragraphs'][temp]['end'] = pgph.start(1)
                temp = re.sub('CHAPTER|PART', 'PARAGRAPH', part_key.strip()) + '_' + pgph.group(2).upper()
                chapters[key]['parts'][part_key]['paragraphs'][temp] = {'start': pgph.start(1),
                                                                        'key': pgph.end(1) - pgph.start(1)}
            if len(chapters[key]['parts'][part_key]['paragraphs']) > 0:
                for pgph_key in chapters[key]['parts'][part_key]['paragraphs'].keys():
                    try:
                        pgph_text = part_text[chapters[key]['parts'][part_key]['paragraphs'][pgph_key]['start']:
                                              chapters[key]['parts'][part_key]['paragraphs'][pgph_key]['end']]
                    except Exception:
                        pgph_text = part_text[chapters[key]['parts'][part_key]['paragraphs'][pgph_key]['start']:]
                    chapters[key]['parts'][part_key]['paragraphs'][pgph_key]['articles'] = get_article('Paragraf',
                                                                                                       regulatory_id,
                                                                                                       pgph_text)
            else:
                chapters[key]['parts'][part_key]['articles'] = get_article('Bagian', regulatory_id, part_text)

        if len(chapters[key]['parts']) == 0:
            chapters[key]['articles'] = get_article('BAB', regulatory_id, chapter_text.strip(), added_pattern)

    if len(chapters) == 0:
        chapters = get_article('', regulatory_id, texts, added_pattern)
        get_body_values(chapters, texts, 'articles', regulatory_id, aliases=aliases, concepts=concepts,
                        refers=refers, values=values, nodes=nodes, edges=edges, added_pattern=added_pattern)

    else:
        get_body_values(chapters, texts, 'chapters', regulatory_id, aliases=aliases, concepts=concepts,
                        refers=refers, values=values, nodes=nodes, edges=edges)
    return chapters


def get_article(part_name, regulatory_id, texts, added_pattern='()'):
    temp = None
    pattern = r'(?s)(^{part_name}[\w\W\n]*?|[\.:][\n\s]*)\n\s*(Pasal[\s\n]*(\d+{added_pattern}))\s*\n'
    filtered_pasal = re.finditer(pattern.format(part_name=part_name, added_pattern=added_pattern), texts)
    res = {}
    for pasal in filtered_pasal:
        if len(res) > 0:
            res[temp]['end'] = pasal.start(2)
        temp = refers_reformat(pasal.group(2), regulatory_id=regulatory_id, added_pattern=added_pattern)
        res[temp] = {'start': pasal.start(2), 'name': pasal.group(2)}
    for pasal in res.keys():
        try:
            pasal_text = texts[res[pasal]['start']:res[pasal]['end']]
        except Exception:
            pasal_text = texts[res[pasal]['start']:]
        filtered_ayat = re.finditer(r'(^Pasal[\n\s]+\d+{}[\n\s]+|\.[\n\s]+)(\(\d+{}\))'.format(added_pattern,
                                                                                               added_pattern.lower()),
                                    pasal_text)
        res[pasal]['sections'] = {}
        for ayat in filtered_ayat:
            if len(res[pasal]['sections']) > 0:
                res[pasal]['sections'][temp]['end'] = ayat.start(3)
            temp = refers_reformat(res[pasal]['name']+' ayat '+ayat.group(3), regulatory_id=regulatory_id,
                                   added_pattern=added_pattern)
            res[pasal]['sections'][temp] = {'start': ayat.start(3), 'name': ayat.group(3)}
    return res


def get_body_values(chapters, texts, part, regulatory_id, aliases, concepts, refers, values, nodes, edges,
                    prev_chapter='', added_pattern=''):
    next_part = {'chapters': 'parts', 'parts': 'paragraphs', 'paragraphs': 'articles', 'articles': 'sections'}
    for chapter in chapters.keys():
        try:
            part_text = texts[chapters[chapter]['start']:chapters[chapter]['end']]
        except Exception:
            part_text = texts[chapters[chapter]['start']:]
        if part != 'sections' and len(chapters[chapter][next_part[part]]) > 0:
            if part not in ('articles', 'sections'):
                first_next_key = list(chapters[chapter][next_part[part]].keys())[0]
                first_next_start = chapters[chapter][next_part[part]][first_next_key]['start']
                part_name = re.sub(chapter, '', part_text[:first_next_start])
                chapters[chapter]['name'] = re.sub(r'[\s\n]+', ' ', part_name[chapters[chapter]['key']:]).strip()
                prev_chap = chapter
            else:
                prev_chap = chapters[chapter]['name']
            get_body_values(chapters[chapter][next_part[part]], part_text, next_part[part], regulatory_id,
                            aliases=aliases, concepts=concepts, refers=refers, values=values, nodes=nodes, edges=edges,
                            prev_chapter=prev_chap, added_pattern=added_pattern)
        elif part not in ('articles', 'sections'):
            if len(chapters[chapter]['articles']) > 0:
                first_next_key = list(chapters[chapter]['articles'].keys())[0]
                first_next_start = chapters[chapter]['articles'][first_next_key]['start']
                part_name = re.sub(chapter, '', part_text[:first_next_start])
                chapters[chapter]['name'] = re.sub(r'[\s\n]+', ' ', part_name[chapters[chapter]['key']:]).strip()
                get_body_values(chapters[chapter]['articles'], part_text, 'articles', regulatory_id, aliases=aliases,
                                concepts=concepts, refers=refers, values=values, nodes=nodes, edges=edges,
                                prev_chapter=chapter, added_pattern=added_pattern)
            else:
                part_name = re.sub(chapter, '', part_text)
                chapters[chapter]['name'] = re.sub(r'[\s\n]+', ' ', part_name[chapters[chapter]['key']:]).strip()
        else:
            values_iter = re.sub(r'\s\+', ' ',
                                 re.sub(r'^(Pasal[\s\n]+\d+{}|'
                                        r'\(\d+{}\))[\s\n]+'.format(added_pattern, added_pattern.lower()),
                                        '', part_text)).strip()
            if re.search('(?i)pasal', prev_chapter):
                chapter_text = re.sub(r'(?i)pasal\s+', part[:-1].upper() + '_',
                                      prev_chapter.strip()) + '_' + re.sub(r'\W', '', chapters[chapter]['name']).upper()
            else:
                chapter_text = re.sub(r'(?i)pasal\s+', part[:-1].upper() + '_',
                                      chapters[chapter]['name']).upper().strip()
            values.append({'id': chapter_text, 'value': re.sub(r'\n+', '\n', re.sub(r' +', ' ', values_iter))})

            values_iter = re.split(r'\n\d{1,3}\s?\.\s', values_iter)
            refer_dict = []
            concept_dict = []
            if len(values_iter) > 2:
                values_iter[0] = re.sub(':', '', values_iter[0])
                for idx in range(1, len(values_iter)-1):
                    values_iter[0] += ' V' + str(idx) + ','
                values_iter[0] += ' dan V' + str(len(values_iter)-1)
            elif len(values_iter) > 1:
                values_iter[0] = re.sub(':', '', values_iter[0]) + ' V1'
            for idx in range(len(values_iter)):
                if len(values_iter) > 1:
                    chapter_text += '_V' + str(idx)
                else:
                    chapter_text += ''
                get_detail(values_iter[idx].strip(), values=values, chapter=chapter_text, regulatory_id=regulatory_id,
                           aliases=aliases, concepts=concepts, refers=refers, nodes=nodes, edges=edges)
                refer_dict.append(refers)
                concept_dict.append(concepts)
                values.append({'id': chapter_text, 'value': re.sub(r'\n+', '\n', re.sub(r' +', ' ', values_iter[idx]))})
                chapter_text = re.sub(r'_V\d+$', '', chapter_text)


def get_detail(texts, chapter, values, regulatory_id, aliases, concepts, refers, nodes, edges, ):
    res_funct = {}
    refer_dict = {}
    refer_num = 0
    copy_texts = texts
    refer_iter = re.finditer(r'((((Pasal[\s\n]+\d+[\W\s\n]+)?(ayat[\s\n]+\(\d+\)[\W\s\n]+)?(huruf[\s\n]+' +
                             r'(\(?)[a-z](\)?)[\W\s\n]+)?)?)(Peraturan|Undang-Undang|Keputusan)[\s\W\n]+' +
                             r'([A-Z][A-Za-z]+[\s\W\n]+)+Nomor[\W\s\n]+(\d+[\W\s\n]+Tahun[\W\s\n]+\d+|\d+' +
                             r'(/[a-zA-Z0-9]+)+)[\W\s\n]+tentang[\W\s\n]|Undang[\s\n]*-[\s\n]*Undang[\s\n]+' +
                             r'Dasar[\s\n]+Negara[\s\n]+Republik[\s\n]+Indonesia[\s\n]+Tahun[\s\n]+1945)',
                             texts)
    start = None
    for refer in refer_iter:
        if refer.group(0) != '':
            # print('jsredh')
            if start is None:
                start = refer.start()
                continue
            temp = texts[start: refer.start(0)]
            refer_text = re.search(r'(^(((Pasal[\s\n]+\d+[\W\s\n]+)?(ayat[\s\n]+\(\d+\)[\W\s\n]+)?(huruf[\s\n]+(\(?)' +
                                   r'[a-z](\)?)[\W\s\n]+)?)?)(Peraturan|Undang-Undang|Keputusan)[\s\W\n]+' +
                                   r'([A-Z][A-Za-z]+[\s\W\n]+)+Nomor[\W\s\n]+(\d+[\W\s\n]+Tahun[\W\s\n]+\d+|\d+' +
                                   r'(/[a-zA-Z0-9]+)+)[\W\s\n]+tentang[\W\s\n]+(([A-Z][a-zA-Z0-9]+|\d+|dan)' +
                                   r'[\W\s\n]+)*([A-Z][a-zA-Z0-9]+|\d+)|Undang[\s\n]*-[\s\n]*Undang[\s\n]+' +
                                   r'Dasar[\s\n]+Negara[\s\n]+Republik[\s\n]+Indonesia[\s\n]+Tahun[\s\n]+1945)' +
                                   r'[\W\s\n]+', temp)
            id_dict = 'RX' + re.sub('^[A-Z]+', '', chapter.replace('_', 'X')) + 'N' + str(refer_num)
            try:
                refer_dict[id_dict] = re.sub(r'^[,.\s\n]|[\s\n]+|[.,\s\n]+$', ' ', refer_text.group(1))
                refer_num += 1
            except Exception:
                pass

            start = refer.start(0)
    if start is not None:
        temp = texts[start:]
        id_dict = 'RX' + re.sub('^[A-Z]+', '', chapter.replace('_', 'X')) + 'N' + str(refer_num)
        refer_text = re.search(r'(^(((Pasal[\s\n]+\d+[\W\s\n]+)?(ayat[\s\n]+\(\d+\)[\W\s\n]+)?(huruf[\s\n]+(\(?)[a-z]'
                               r'(\)?)[\W\s\n]+)?)?)(Peraturan|Undang-Undang|Keputusan)[\s\W\n]+'
                               r'([A-Z][A-Za-z]+[\s\W\n]+)+Nomor[\W\s\n]+(\d+[\W\s\n]+Tahun[\W\s\n]+\d+|\d+'
                               r'(/[a-zA-Z0-9]+)+)[\W\s\n]+tentang[\W\s\n]+(([A-Z][a-zA-Z0-9]+|\d+|dan)[\W\s\n]+)*'
                               r'([A-Z][a-zA-Z0-9]+|\d+)|Undang[\s\n]*-[\s\n]*Undang[\s\n]+Dasar[\s\n]+Negara[\s\n]+'
                               r'Republik[\s\n]+Indonesia[\s\n]+Tahun[\s\n]+1945)[\W\s\n]+',
                               temp)
        try:
            refer_dict[id_dict] = re.sub(r'^[,.\s\n]|[\s\n]+|[.,\s\n]+$', ' ', refer_text.group(0)).strip()
            refer_num += 1
        except Exception:
            pass
    texts = copy_texts
    refer_to = re.finditer(r'(Pasal[\s\n]+\d+[\W\s\n]+)?(ayat[\s\n]+\(\d+\)[\W\s\n]+)?'
                           r'(huruf[\s\n]+(\(?)[a-z](\)?)[\W\s\n]+)?',
                           texts)
    for refer in refer_to:
        if refer.group(0) != '':
            id_dict = 'RX' + re.sub('^[A-Z]+', '', chapter.replace('_', 'X')) + 'N' + str(refer_num)
            refer_dict[id_dict] = re.sub(r'^[,.\s\n]|[\s\n]+|[.,\s\n]+$', ' ', refer.group(0)).strip()
            refer_num += 1
    refer_key = list(refer_dict.keys())
    refer_val = list(refer_dict.values())
    refer_val.sort(key=lambda item: (-len(item), item))
    refer_dict = dict(zip(refer_key, refer_val))
    copy_texts = re.sub(r'[\s\n]+', ' ', copy_texts)
    for k in refer_dict.keys():
        copy_texts = copy_texts.replace(refer_dict[k], k)
        refers.append({'id': k, 'text': refer_dict[k],
                       'value': refers_reformat(refer_dict[k], regulatory_id=regulatory_id, chapter=chapter)})
    texts = copy_texts
    concept_iter = re.finditer(
        r'(^([A-Z][A-Za-z]+[\s\n]+)+[A-Z][A-Za-z]+|[\W\s\n]+([A-Z][A-Za-z]+[\s\n]+)+[A-Z][A-Za-z]+)[\W\s\n]+',
        texts)
    concept_num = 0
    for concept in concept_iter:
        concept_text = re.sub(r'[\s\n\W]+', ' ', concept.group(1)).strip()
        id_concept = 'CX' + re.sub('^[A-Z]+', '', chapter.replace('_', 'X')) + 'N' + str(concept_num)
        copy_texts = copy_texts.replace(re.sub(r'^[\s\W\n]+|[\s\W\n]+$', '', concept.group(1)),
                                        id_concept)
        concepts.append({'id': id_concept, 'text': concept_text})
        concept_num += 1
    texts = copy_texts
    details = re.finditer(r'[:;,.]([\s\n]*(dan|atau|dan\s*/\s*atau))?[\s\n]+([a-z]{1,2})\.', texts)
    temp = None
    for detail in details:
        if len(res_funct) > 0:
            res_funct[temp]['end'] = detail.start(3)
        l_key = re.sub('^[A-Z]+', 'LX', chapter.replace('_', 'X') + 'X' + detail.group(3).upper())
        res_funct[l_key] = {'start': detail.start(3)}
        temp = l_key
    detail_text = {}
    for res_k in res_funct.keys():
        try:
            detail_text[res_k] = re.sub(r'(dan\s*/\s*atau|dan|atau)[\s\n]+$', '',
                                        texts[res_funct[res_k]['start']:res_funct[res_k]['end']])
        except Exception:
            detail_text[res_k] = re.sub(r',[\s]+\n[\W\w\n]+|\.$', '', texts[res_funct[res_k]['start']:])
    for res_k in res_funct.keys():
        texts = texts.replace(detail_text[res_k], res_k + ', ')
        texts = re.sub(r',[\s\n]+,', ', ', re.sub(':', '', re.sub(r',[\s\n]+\.[\s\n]*$', '.', texts)))
        detail_text[res_k] = re.sub(r'[\s\n]+', ' ',
                                    re.sub(r'^[a-z]{1,2}\.|[;,.][\s\n]+$', '', detail_text[res_k])).strip() + '.'
        new_text = definition_reformat(detail_text[res_k], res_k, aliases=aliases)
        pos_tag(new_text, chapters=res_k, nodes=nodes, edges=edges)
        values.append({'id': res_k, 'value': re.sub(r'\n+', '\n', re.sub(r' +', ' ', new_text))})
    texts = re.sub(r'[\s\n]+', ' ', texts)
    texts = definition_reformat(texts, chapter, aliases=aliases)
    pos_tag(texts, chapters=chapter, nodes=nodes, edges=edges)


def definition_reformat(texts, chapter, aliases):
    subj_alias = re.search(r'((yang[\s\n]+)?selanjutnya[\s\n]+di(sebut|singkat)[\s\n]+(dengan[\s\n]+)?' +
                           r'([\w\s\n-]+))(adalah)', texts)
    if subj_alias is None:
        subj_alias = re.search(r'((yang[\s\n]+)?selanjutnya[\s\n]+di(sebut|singkat)[\s\n]+(dengan[\s\n]+)?' +
                               r'([\w\s\n-]+))(merupakan)', texts)
    if subj_alias is not None:
        aliases.append({'chapter': chapter, 'object': subj_alias.group(5)})
        texts = re.sub(subj_alias.group(0), 'merupakan', texts)
    texts = re.sub('adalah', 'merupakan', texts)
    return texts


def refers_reformat(ref_text, regulatory_id='', chapter='', added_pattern='', new_type=''):
    rule_part = re.search(r'(?i)(Pasal[\s\n]+\d+{added_pattern}([\W\s]+|'
                          r'[\W\s]*$))?(ayat\s*\(\d+{added_pattern}\)([\W\s]+|[\W\s]*$))?'
                          r'(huruf[\s]+\(?[a-z]\)?([\W\s]+|[\W\s]*$))?'.format(added_pattern=added_pattern), ref_text)
    reg_part = re.search(r'(?i)(Perubahan[\s\n](pertama|ke([a-z]+))?)?(Peraturan|Keputusan|Undang(-?)[\s\n]*Undang).*',
                         ref_text)
    res_rule, res_reg = '', regulatory_id
    if rule_part is not None:
        rule_part = rule_part.group(0)
        if re.search('(?i)^Pasal', rule_part):
            if re.search('(?i)ayat', rule_part):
                pointer = 'SECTION_'
            else:
                pointer = 'ARTICLE_'
            res_rule = re.sub(r'\s+', '_', pointer + re.sub(r'(?i)(pasal|ayat|huruf|\W+)', ' ',
                                                            rule_part).strip()).upper()
        elif re.search('(?i)^ayat', rule_part):
            try:
                res_rule = re.sub(r'\s+', '_', re.search(r'^SECTION_\d+_',
                                                         chapter).group(0) + re.sub(r'ayat|huruf|\W+', ' ',
                                                                                    rule_part).strip()).upper()
            except AttributeError:
                res_rule = re.sub(r'\s+', '_',
                                  'SECTION_X_'+re.sub(r'ayat|huruf|\W+', ' ', rule_part).strip()).upper()
        elif re.search('(?i)^huruf', rule_part):
            res_rule = re.sub(r'\s+', '_', re.search(r'^[A-Z]+_\d+((_\d+)?)',
                                                     chapter).group(0) + '_' + re.sub(r'huruf|\W+', ' ',
                                                                                      rule_part).strip()).upper()
    if reg_part is not None:
        reg_part = reg_part.group(0)
        refer_detail = get_title(reg_part.upper(), is_doc=False)
        try:
            res_reg = reg_types.loc[reg_types['type'] == refer_detail['type'], 'code'].values[0]
            if res_reg in ['Perda', 'Perkot', 'Perkab', 'Peraturan', 'Perprov', 'Permen'] and new_type != '':
                res_reg = reg_types.loc[reg_types['type'] == new_type, 'code'].values[0]
            res_reg = res_reg + '_' + refer_detail['year'] + '_' + refer_detail['number']
        except Exception:
            try:
                res_reg = reg_types.loc[reg_types['type'] == refer_detail['type'], 'code'].values[0]
                res_reg = res_reg + '_' + refer_detail['year']
            except Exception:
                res_reg = ''
    res_funct = re.sub(r'^_|_$', '', res_reg + '_' + res_rule)
    return res_funct


def pos_tag(texts, nodes, edges, chapters=''):
    try:
        if re.search(r'[a-zA-Z\d]', texts):
            doc = nlp(texts)
        else:
            doc = None
    except RuntimeError:
        print(len(texts.split()))
        RuntimeError.with_traceback()
    if doc is not None:
        for idx_sentence in range(len(doc.sentences)):
            if len(doc.sentences) > 1:
                add = '_' + str(idx_sentence)
            else:
                add = ''
            new_chap = chapters + add
            for word in doc.sentences[idx_sentence]._words:
                node = {'sentence_id': new_chap, 'id': word._id, 'text': word._text, 'type': word._upos.lower(),
                        'min': word._id, 'max': word._id, 'is_root_sentence': re.search(r'[A-Z]$', chapters) is None}
                nodes.append(node)
            for key in range(len(doc.sentences[idx_sentence]._dependencies)):
                relation = (doc.sentences[idx_sentence]._dependencies)[key]
                edge = {}
                word_source = relation[0]
                word_target = relation[2]
                edge['sentence_id'] = new_chap
                edge['id'] = key
                edge['source'] = word_source._id
                edge['target'] = word_target._id
                edge['rel_name'] = relation[1]
                edge['state'] = 1
                edge['is_root_sentence'] = re.search(r'[A-Z]$', chapters) is None
                edges.append(edge)


def get_closing_part(texts):
    month = 'januari|februari|maret|april|mei|juni|juli|agustus|september|oktober|november|desember'
    res_funct = {}
    filtered_text = re.search(r'(Agar\s*)?[sS]e(tiap|mua)\s*[oO]rang\s*(yang\s*)?(dapat\s*)?mengetahui(nya)?[\w\W\n]+',
                              texts).group(0).strip()

    res_funct['promulgation'] = {'place': re.search(r'berita[\s\n]*negara|lembaran[\s\n]*negara|' +
                                                    r'lembaran[\s\n]*daerah|berita[\s\n]*daerah',
                                                    filtered_text.lower()).group(0)}
    res_funct['enactment'] = {}
    try:
        parts = {'enactment': re.search(r'Ditetapkan([\w\W\n]+)Diundangkan', filtered_text).group(1).strip(),
                 'promulgation': re.search(r'Diundangkan([\w\W\n]+)', filtered_text).group(1).strip()}
    except Exception:
        return res_funct
    for key in parts.keys():
        locations = [i for i in re.finditer(r'di([\w\W\n]+)pada[\s\n]+((tanggal)?)', parts[key])]
        if len(locations) == 1:
            res_funct[key]['location'] = re.sub(r'[\n\s]+', ' ',
                                                locations[0].group(1)).strip()
        elif len(locations) == 2:
            res_funct['enactment']['location'] = re.sub(r'[\n\s]+', ' ', locations[0].group(1)).strip()
            res_funct['promulgation']['location'] = re.sub(r'[\n\s]+', ' ', locations[1].group(1)).strip()
        dates = [i for i in re.finditer(r'tanggal[\s\n]+(\d+[\s\n]+(' + month + r')[\s\n]+\d+)', parts[key].lower())]
        if len(dates) == 1:
            res_funct[key]['date'] = re.sub(r'[\n\s]+', ' ', dates[0].group(1)).strip()
        elif len(dates) == 2:
            res_funct['enactment']['date'] = re.sub(r'[\n\s]+', ' ', dates[0].group(1)).strip()
            res_funct['promulgation']['date'] = re.sub(r'[\n\s]+', ' ', dates[1].group(1)).strip()
        officials = [i for i in re.finditer(r'([A-Z-\s]+),', parts[key])]
        if len(officials) == 1:
            res_funct[key]['official'] = re.sub(r'[\n\s]+', ' ', officials[0].group(1)).strip()
        elif len(officials) == 2:
            res_funct['enactment']['official'] = re.sub(r'[\n\s]+', ' ', officials[0].group(1)).strip()
            res_funct['promulgation']['official'] = re.sub(r'[\n\s]+', ' ', officials[1].group(1)).strip()
        official_names = [i for i in re.finditer(r'(?i)[tT]td([.,])?[\s\n]+(([A-Z.]|[^\S\n\v\f\r\u2028\u2029])+)',
                                                 parts[key])]
        if len(official_names) == 1:
            res_funct[key]['official_name'] = re.sub(r'[\n\s]+', ' ', official_names[0].group(2)).strip()
        elif len(official_names) == 2:
            res_funct['enactment']['official_name'] = re.sub(r'[\n\s]+', ' ', official_names[0].group(2)).strip()
            res_funct['promulgation']['official_name'] = re.sub(r'[\n\s]+', ' ', official_names[1].group(2)).strip()
    return res_funct


def text_to_num(num_t):
    num_dict = {'pertama': [1, 1], 'satu': [1, 1], 'dua': [2, 1], 'tiga': [3, 1], 'empat': [4, 1], 'lima': [5, 1],
                'enam': [6, 1], 'tujuh': [7, 1], 'delapan': [8, 1], 'sembilan': [9, 1], 'sebelas': [11, 1],
                'sepuluh': [10, 1], 'seratus': [100, 1], 'seribu': [1000, 1], 'belas': [10, 1]}
    time_dict = {'puluh': [10, 1], 'ratus': [100, 2]}
    cubes = {'ribu': [1e3, 1], 'juta': [1e6, 2], 'milyar': [1e9, 3]}
    temp = 0
    res_func = 0
    num_t = num_t.split(' ')
    x = 0
    for t in num_t:
        try:
            x += num_dict[t][0]
        except Exception:
            try:
                temp += x * time_dict[t][0]
                x = 0
            except Exception:
                temp = (temp + x) * cubes[t][0]
                res_func += temp
                temp = 0
                x = 0
    res_func += temp + x
    return res_func


def changer_reg(reg_name, rtype_doc):
    change_dict = get_title(reg_name, is_doc=False)
    rtype = reg_types.loc[reg_types['type'] == change_dict['type'], 'code'].values[0]
    if rtype not in ['Perda', 'Perkot', 'Perkab', 'Peraturan', 'Perprov', 'Permen']:
        change_id = rtype + '_' + change_dict['year'] + '_' + change_dict['number']
    else:
        change_id = reg_types.loc[reg_types['type'] == rtype_doc, 'code'].values[0] +\
                    '_' + change_dict['year'] + '_' + change_dict['number']
    return change_id


def get_change_body_part(texts, regulatory_id, aliases, concepts, refers, values, nodes, edges):
    deletedMatches = re.finditer(r'(Pasal(\s|\n)+\d+[A-Z]*)(\s|\n)+dihapus\.', texts)
    for deletedMatch in deletedMatches:
        new_text = 'Ketentuan\n' + deletedMatch.group(1)+'\n'+'Dihapus.'
        texts = re.sub(deletedMatch.group(0), new_text, texts)
    filtered_changes = re.search(r'\n\s*(Pasal[\s\n]*I([\w\W]*))Pasal[\s\n]*II', texts).group(1)
    change_iter = re.finditer(r'(Pasal I[^:]+:|\.)\s*\n(\n|\s)*((\d+)[\s\n]*\.)[\s\n]*(.|\n)*?\n+\s*(BAB|Pasal)(.|\n)',
                              filtered_changes)

    changes = {}
    temp = ''
    n = 0
    for change in change_iter:
        n += 1
        if len(changes) > 0:
            changes[temp]['end'] = change.start(3)
        temp = 'CHANGE_' + re.sub(r'[^\d]+', '', change.group(3)).strip()
        # print(temp)
        changes[temp] = {'start': change.start(3), 'key': change.end(3) - change.start(3)}
        # print(changes[temp])
    if n == 0:
        changes['CHANGE_1'] = {'start': 0, 'key': 0}
    for num_c in changes.keys():
        try:
            change_text = filtered_changes[changes[num_c]['start']:changes[num_c]['end']]
        except Exception:
            change_text = filtered_changes[changes[num_c]['start']:]
        changes[num_c] = get_body_part(change_text, regulatory_id=regulatory_id, aliases=aliases, concepts=concepts,
                                       refers=refers, values=values, nodes=nodes, edges=edges, is_change_reg=True)
    return changes


def get_repealed_doc(repealed_df, new_type):
    repealedRes = {}
    for repealedText in repealed_df.iterrows():
        docs = re.finditer(r'(?i)(Peraturan|Undang|Keputusan)([^;,]|\n)+(tentang)', repealedText[1]['value'])
        temp = []
        for doc in docs:
            repealed_text = re.sub(r'[\s\n]+', ' ', doc.group(0).upper())
            repealDoc = refers_reformat(repealed_text, new_type=new_type)
            if repealDoc is not None or repealDoc.strip() != '':
                temp.append(repealDoc)
        if len(temp) > 0:
            repealedRes[repealedText[1]['id']] = temp
    return repealedRes


def get_implementing_doc(considerans_text):
    implementRes = []
    if re.search('bahwa untuk melaksanakan ketentuan (Peraturan|Undang|Pasal)', considerans_text):
        try:
            implementing_text = re.search(r'Pasal([^,]|\n)\d+[A-Z]?([^,]|\n)([Aa]yat([^,]|\n)\(\d+[a-z]?\))?' +
                                          r'([^,]|\n)+(Peraturan|Undang)([^,]|\n)+(tentang)', considerans_text).group(0)
            print(implementing_text)
            implementArticle = refers_reformat(implementing_text)
            if implementArticle is not None and implementArticle != '':
                implementRes.append(implementArticle)
        except AttributeError:
            pass
        implementing_text = re.search(r'(Peraturan|Undang)([^,]|\n)+(tentang)', considerans_text).group(0)
        implementDoc = refers_reformat(implementing_text)
        if implementDoc is not None and implementDoc != '':
            implementRes.append(implementDoc)
        return implementRes
    else:
        return None


nlp = stanza.Pipeline(lang='id', use_gpu=True)
file_list = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')
global reg_types
reg_types = pandas.read_csv('regulatory_type.csv')
last_error_file = open('last_error.txt', 'r+')
last_error = last_error_file.readline()
last_error_file.close()
file_num = 0
state = True
next = True
failed = []
types = []
stop = False
filterDoc = None
while state:
    text = file_list.readline()
    if text == '':
        state = False
    print(file_num, text)
    text = text.strip()
    filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_1_text_files/' + (
        re.sub('_pdf', '.txt', re.sub(r'(%20)|([\\._]+)', '_',
                                      re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
    filetarget = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_parsed_files/' + (
        re.search(r'(.{,100})', re.sub('_pdf', '', re.sub('(%20)|([\\._]+)', '_',
                                                          re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                 '', text)))).group(1).lower()) + '.json'
    dirName = re.sub('/[^/]+$', '', filetarget)
    try:
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    except Exception:
        None
    try:
        file = open(filesource, encoding="utf8")
    except Exception:
        file_num += 1
        continue
    res = None
    # Uncomment codes bellow if don't want to reparse the legal clause
    # try:
    #     res = json.load(open(filetarget, 'r'))
    # except Exception:
    #     pass
    if re.search('putusan', filesource):
        file_num += 1
        continue
    text = file.read()
    text = re.sub('\n+\s*\d+[^\w;]*([Nn][Oo]\\.\s*(-?\s*\d+\s*-?\s*)+)?\n+', '\n',
                  re.sub(r'\n+(\s*\d+\s*\n+)?', '\n',
                         re.sub('\\nwww.djpp.(depkumham|kemenkumham).go.id|https://jdih.bandung.go.id/|' +
                                r'www.peraturan.go.id\\n', '\n',
                                re.sub(r'[^a-zA-Z\d\n./:(\-,);]', ' ',
                                       re.sub(r'\n+', '\n',
                                              re.sub(r'\n\s*\n', '\n\n', text)))))).strip()

    if re.match(r'^[\s\n\W]*$', text):
        file_num += 1
        continue
    if re.search('(?i)/[^/]*tln|lmp[^/]*$', filesource):
        file_num += 1
        continue
    try:
        result = {'title': get_title(text)}
        reg_type = reg_types.loc[reg_types['type'] == result['title']['type'].strip(), 'code'].values[0]
        if reg_type not in ['UUD', 'Amandemen_1_UUD', 'Amandemen_2_UUD', 'Amandemen_3_UUD', 'Amandemen_4_UUD']:
            reg_id = reg_type + '_' + result['title']['year'] + '_' + result['title']['number']
        else:
            reg_id = reg_type + '_' + result['title']['year']
        if filterDoc is not None and reg_id.upper() not in filterDoc:
            file_num += 1
            continue

        if reg_type in ['Amandemen_1_UUD', 'Amandemen_2_UUD', 'Amandemen_3_UUD', 'Amandemen_4_UUD']:
            result['title']['change'] = 'UUD_1945'
            result['title']['change_num'] = re.sub(r'[^\d]+', '', reg_type)
        result['title']['reg_id'] = reg_id
        print(result['title']['reg_id'])
        if re.search('^PERUBAHAN', result['title']['reg_id']):
            # print('jshsh')
            result['title']['change'] = re.sub(r'PERUBAHAN(_\d+)?_', '', result['title']['reg_id'])
            try:
                result['title']['change_num'] = re.search(r'PERUBAHAN(_\d+)?_', result['title']['reg_id']).group(1)
            except Exception:
                result['title']['change_num'] = 1
        elif reg_type not in ['UUD', 'Amandemen_1_UUD', 'Amandemen_2_UUD', 'Amandemen_3_UUD', 'Amandemen_4_UUD'] and\
                re.search(r'^PERUBAHAN(\s+(PERTAMA|KE([A-Z ]+)))? (ATAS )?(PERATURAN|UNDANG)', result['title']['name']):
            result['title']['change'] = changer_reg(re.sub(r'PERUBAHAN(\s+(PERTAMA|KE([A-Z ]+)))? (ATAS )?'+
                                                           r'(?=PERATURAN|UNDANG)', '', result['title']['name']),
                                                    result['title']['type'])
            try:
                change_num = re.sub(r'^KE|\W+', ' ', re.search('PERUBAHAN (PERTAMA|KE([A-Z ]+))? (ATAS )?',
                                                               result['title']['name']).group(1)).strip().lower()
                result['title']['change_num'] = text_to_num(re.search('^[a-z]+', change_num).group(0))
            except Exception:
                result['title']['change_num'] = 1
        result['considerans'] = get_considerans(text)
        result['law_based'] = [e for e in get_law_based(text) if e != '']
        result['dictum'] = get_dictum(text)
        if reg_type not in ['UUD', 'Amandemen_1_UUD', 'Amandemen_2_UUD', 'Amandemen_3_UUD', 'Amandemen_4_UUD']:
            result['closing_part'] = get_closing_part(text)
        if res is None or result['title'].keys().__contains__('change'):
            result['aliases'] = []
            result['concepts'] = []
            result['refers'] = []
            result['values'] = []
            result['nodes'] = []
            result['edges'] = []
            if result['title'].keys().__contains__('change'):
                result['body'] = get_change_body_part(text,
                                                      result['title']['change'],
                                                      aliases=result['aliases'],
                                                      concepts=result['concepts'],
                                                      refers=result['refers'],
                                                      values=result['values'],
                                                      nodes=result['nodes'],
                                                      edges=result['edges'])
                stop = True
            else:
                result['body'] = get_body_part(text,
                                               reg_id,
                                               aliases=result['aliases'],
                                               concepts=result['concepts'],
                                               refers=result['refers'],
                                               values=result['values'],
                                               nodes=result['nodes'],
                                               edges=result['edges'])

        elif isinstance(res, dict):
            result['body'] = res['body']
            result['aliases'] = res['aliases']
            result['concepts'] = res['concepts']
            result['refers'] = res['refers']
            result['values'] = res['values']
            result['nodes'] = res['nodes']
            result['edges'] = res['edges']
        if reg_type not in ['UUD', 'Amandemen_1_UUD', 'Amandemen_2_UUD', 'Amandemen_3_UUD', 'Amandemen_4_UUD']:
            valdf = pandas.DataFrame(result['values'])
            if len(valdf) > 0:
                valdf = valdf[valdf['value'].str.contains(r'(?i)(mencabut(\s|\n)(.|\n)*(Peraturan|Undang)(.|\n)*' +
                                                          r'tentang(.|\n)*|(Peraturan|Undang)(.|\n)*tentang(.|\n)*' +
                                                          r'dicabut(\s|\n)(.|\n)*)', regex=True)]
                print(valdf)
                repealed = valdf[valdf['value'].str.contains(r'(?i)(mencabut(\s|\n)(.|\n)*(Peraturan|Undang)(.|\n)*' +
                                                             r'tentang(.|\n)*|(Peraturan|Undang)(.|\n)*tentang(.|\n)*' +
                                                             r'dicabut(\s|\n)(.|\n)*)', regex=True)].drop_duplicates()
                repealed_doc = get_repealed_doc(repealed, result['title']['type'])
                result['title']['repeal'] = repealed_doc
            if len(result['considerans']) > 0:
                implementing_doc = get_implementing_doc(result['considerans'][0])
                if implementing_doc is not None:
                    result['title']['implement'] = implementing_doc
        with open(filetarget, 'w') as json_file:
            json.dump(result, json_file, indent=4)
        file_num += 1
    except RuntimeError:
        file_num += 1
        print('failed', filesource)
        with open('last_error.txt', 'w') as text_file:
            text_file.write(filesource)
        RuntimeError.with_traceback()
    except Exception:
        file_num += 1
        failed.append(filesource)
        if stop:
            stop = False
        print('failed', filesource)
