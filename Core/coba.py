# from pdfminer.high_level import extract_text
#
# filesource='D:/Ninggar/Mgstr/Semester 2/Data/files/files/bn/2014/bn387-2014.pdf'
#
# res = extract_text(filesource)
# print(res)
#
import re
import nltk
file = open('E:/Ninggar/Mgstr/Semester 4/Perolehan Informasi Lanjut/text_dataset/S08_dataset/S08_set3_a4.txt.clean', "r", encoding="utf8")
# nltk.download('punkt')
text = file.read()
# print(text)
start = re.search('1776', text).span()[0]
print(len(nltk.word_tokenize(text)))
# print(text[start:])

from transformers import Trainer
x = Trainer()
from transformers import BertForQuestionAnswering, Bert
finetune_model = BertForQuestionAnswering.from_pretrained('qna-task-3')
finetune_model.base_model.