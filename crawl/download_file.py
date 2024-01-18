import os
import re
import urllib.request

file = open('failed_filename', 'r+')
state = True
failed = []
file_num = 0
while state:
    text = file.readline()
    if text == '':
        state = False
    text = text.strip()
    filename = 'files/' + (
        re.sub('_pdf', '.pdf', re.sub('(%20)|([\\._]+)', '_',
                                      re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
    dirName = re.sub('/[^/]+$', '', filename)
    try:
        # Create target Directory
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    except FileExistsError:
        None
    try:
        urllib.request.urlretrieve(text, filename)
    except Exception:
        failed.append(text)
    print(file_num)
    file_num += 1
print(failed)
file.close()
