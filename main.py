
from types import SimpleNamespace
import uvicorn

from typing import Union

from fastapi import FastAPI

import nltk
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search
from collections import Counter
import pymorphy2
#import elastico
from zinc_search import Zincsearch
from sphinx_search import Sphinxsearch
import sphinxapi
from urllib.parse import urlparse

API_PORT = 8001

app = FastAPI()
with open('stopwords_ext.txt', 'r', encoding='utf-8') as sw_file:
    sw_list = [word.strip() for word in sw_file]
stop_words = stopwords.words('russian') + sw_list # подключаем словарь из nltk.
print (stop_words)


class ISearcher:
    def __init__(self, addr): pass
    def Fuzz(self, index, name): pass
    def Exact(self, index, name): pass


class ElasticSearcher(ISearcher):

    def __init__(self, addr):
        self.client = Elasticsearch(addr)

    def Fuzz(self, index, name):
        return Search(using=self.client, index=index).query("fuzzy", name=name).execute() #Нечеткий поиск

    def Exact(self, index, name):
        return Search(using=self.client, index=index).query('match', name=name).execute() #Точный поиск


class ZincSearcher(ISearcher):

    def __init__(self, addr):
        self.client = Zincsearch(addr)

    def Fuzz(self, index, name):
        return self.client.Search(index=index, search_type="fuzzy", name=name) #Нечеткий поиск

    def Exact(self, index, name):
        return self.client.Search(index=index, search_type="match", name=name) #Точный поиск

class SphinxSearcher(ISearcher):

    def __init__(self, addr):
        self.client = Sphinxsearch(addr)

    def Fuzz(self, index, name):
        return []#self.client.Search(index=index, search_type="fuzzy", name=name) #Нечеткий поиск

    def Exact(self, index, name):
        return self.client.Search(index=index, search_type='', name=name) #Точный поиск

class SphinxSearcher2(ISearcher):

    def __init__(self, addr):
        self.client = sphinxapi.SphinxClient()
        parsed = urlparse(addr)
        self.client.SetServer(parsed.hostname, parsed.port)
        self.client.SetConnectTimeout(1.0)
        self.client.SetMaxQueryTime(5000)
        self.client.SetLimits(0, 9999)
        # self.client.SetMatchMode(sphinxapi.SPH_MATCH_ANY)
        # self.client.SetMatchMode(sphinxapi.SPH_MATCH_FULLSCAN)
        self.client.SetMatchMode(sphinxapi.SPH_MATCH_EXTENDED2)

    def Fuzz(self, index, name):
        return []#self.client.Search(index=index, search_type="fuzzy", name=name) #Нечеткий поиск

    def Exact(self, index, name):
        res = self.client.Query(name, index)
        return [SimpleNamespace(cat=i['attrs']['cat']) for i in res['matches']]


client = ElasticSearcher(addr="http://127.0.0.1:9200") #Подключение к ноде эластика
# client = ZincSearcher(addr="http://127.0.0.1:4080") #Подключение к ноде цинка
# client = SphinxSearcher(addr="http://127.0.0.1:36307") #Подключение к ноде сфинкса
# client = SphinxSearcher2(addr="http://127.0.0.1:36307") #Подключение к ноде сфинкса


@app.get("/cat_search")
def elastico_cat(sentence: str, search_index:str ='ok_test', Full_Answer:bool =False, lemmatyze:bool =True, Just_full_sentence: bool = False, search_request = None):
    if search_request is None:
        search_request = sent_shorter(sentence, lemma=lemmatyze, Only_Full_sent=Just_full_sentence) #предобработка запроса
    elastic_resp_list = [] #Инициализация списка
    for word in search_request:
        response_for_fuzz = client.Fuzz(index=search_index, name=word) #Нечеткий поиск
        exact_resp = client.Exact(index=search_index, name=word) #Точный поиск
        elastic_fuzz_resp_list = [hit.cat for hit in response_for_fuzz] #Формирование списка по ответу нечеткого поиска
        elastic_match_resp_list = [hit.cat for hit in exact_resp] #Формирование списка по ответу точного поиска
        # elastic_resp_list = elastic_fuzz_resp_list + elastic_match_resp_list #Объединение списков
        elastic_resp_list.extend(elastic_fuzz_resp_list + elastic_match_resp_list) #Объединение списков
    if Full_Answer: #Если требуется полный список ответов
        suggested_cat_count = Counter(elastic_resp_list)
        suggested_cat = sorted(suggested_cat_count, key=suggested_cat_count.get, reverse=True)
        if len(elastic_resp_list) != 0:
            cats = suggested_cat
            scats = suggested_cat_count
            # return {"categories":suggested_cat,
            # "sorted_cat": suggested_cat_count}
        else:
            cats = []
            scats = []
    else: #Если требуется вывод одного результата
        suggested_cat = Counter(elastic_resp_list).most_common(1)
        if len(elastic_resp_list) != 0:
            cats = suggested_cat[0][0]
            scats = None
        else:
            cats = None
            scats = None
    return {"categories": cats, **(scats if scats else {})}
    
@app.get("/name_search")
def elastico_name(sentence: str, search_index:str ='ok_test', Full_Answer:bool =False, lemmatyze:bool =True, Just_full_sentence: bool = False, search_request = None):
    if search_request is None:
        search_request = sent_shorter(sentence, lemma=lemmatyze, Only_Full_sent=Just_full_sentence) #предобработка запроса
    elastic_resp_list = []
    for word in search_request:
        response_for_fuzz = client.Fuzz(index=search_index, name=word)
        exact_resp = client.Exact(index=search_index, name=word)
        elastic_fuzz_resp_list = [hit.name for hit in response_for_fuzz]
        elastic_match_resp_list = [hit.name for hit in exact_resp]
        elastic_resp_list = elastic_fuzz_resp_list + elastic_match_resp_list
    if Full_Answer:
        suggested_name = Counter(elastic_resp_list)
        if len(list(suggested_name)) != 0: 
            return {"names" : sorted(suggested_name, key=suggested_name.get, reverse=True),
            "names_unsorted": suggested_name}
        else:
            return {"null"}
    else:
        suggested_name = Counter(elastic_resp_list)
        if len(suggested_name) != 0:
            return {"names" :sorted(suggested_name, key=suggested_name.get, reverse=True)[0]}
        else:
            return {"null"}

@app.get("/req_prep")
def sent_shorter(sentence: str, lemma: bool = True, Only_Full_sent: bool = False):
    words = word_tokenize(sentence) # токенизируем предложение на слова
    morph = pymorphy2.MorphAnalyzer()
    lem_list = [morph.parse(lexem)[0].normal_form for lexem in words] #формируем список из лемматизированных слов
    lem_res_list = []
    in_list =[]
    for i in range(len(lem_list)):
        if lem_list[i] not in stop_words:
            lem_res_list.append(lem_list[i])
            in_list.append(words[i])
    if lemma and not Only_Full_sent: #если используем лемматизацию
        pured_list = lem_res_list + in_list #формируем список очищенный от мусора из стоп-листа
        if len(in_list) > 1: #проверяем, чтоб не задваивать слово, если оно одно в очищенном списке
            pureSent = ' '.join(in_list) #собираем очищенное предложение
            pured_list.append(pureSent) #добавляем в конец очищенное чистое предложение
            return pured_list
        else:
            return pured_list
    elif not lemma and not Only_Full_sent:
        if len(in_list) > 1: #проверяем, чтоб не задваивать слово, если оно одно в очищенном списке
            pureSent = ' '.join(in_list) #собираем очищенное предложение
            in_list.append(pureSent) #добавляем в конец очищенное чистое предложение
            return in_list
        else:
            return in_list
    elif not lemma and Only_Full_sent:
        if len(in_list) > 1: #проверяем, чтоб не задваивать слово, если оно одно в очищенном списке
            pureSent = ' '.join(in_list) #собираем очищенное предложение
            return [pureSent]
        else:
            return in_list
    elif lemma and Only_Full_sent:
        Both_Sents = []
        if len(in_list) > 1: #проверяем, чтоб не задваивать слово, если оно одно в очищенном списке
            pureSent = ' '.join(in_list) #собираем очищенное предложение
            pureLemSent = ' '.join(lem_res_list) #собираем очищенное предложение
            Both_Sents.append(pureSent)
            Both_Sents.append(pureLemSent)
            return Both_Sents
        else:
            Both_Sents.extend(in_list)
            return Both_Sents
    #return result_list

@app.get("/both")
def elas_search(sentence: str, lemma:bool = True, search_index:str ='ok_test', Full_Answer:bool =False, Just_full_sent: bool = False, NeedSuggestNameOfItem: bool = False):
    search_request = sent_shorter(sentence=sentence, lemma=lemma, Only_Full_sent=Just_full_sent)
    result = {"Suggest Category": elastico_cat (sentence, search_index, Full_Answer, search_request=search_request),
        "Shortened sentence": search_request,
        "Request": sentence}
    if NeedSuggestNameOfItem:
        result["Suggest name of item"] = elastico_name(sentence, search_index, Full_Answer, search_request=search_request)
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=API_PORT)
