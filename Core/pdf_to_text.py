import os
import re
import warnings

import pdfbox

warnings.filterwarnings('ignore')
if __name__ == "__main__":
    file = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')
    state = True
    failed = []
    file_num = 0
    global ERRORS_BAD_CONTEXT
    p = pdfbox.PDFBox()
    while state:
        text = file.readline()
        if text == '':
            state = False
        text = text.strip()
        filesource = 'E:/Ninggar/Mgstr/penelitian/Data/files/files/' + (
            re.sub('_pdf', '.pdf', re.sub('(%20)|([\\._]+)', '_',
                                          re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
        filetarget = 'E:/Ninggar/Mgstr/penelitian/Data/files/new_1_text_files/' + (
            re.sub('_pdf', '.txt', re.sub('(%20)|([\\._]+)', '_',
                                          re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
        dirName = re.sub('/[^/]+$', '', filetarget)
        try:
            # Create target Directory
            os.makedirs(dirName)
            print("Directory ", dirName, " Created ")
        except Exception:
            None
        if not re.search('lamp(|iran)|pjl', filesource):
            try:
                f = open(filesource)
                f.close()
                p.extract_text(filesource, filetarget)
                # acrobat_extract_text(filesource, filetarget)
            except IOError:
                print("File not accessible : {}".format(filesource))
                continue
            except Exception:
                print('failed_extract', filesource)
                Exception.with_traceback()
        file_num += 1
        print(file_num)
    file.close()
