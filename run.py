from scrapy import cmdline
from twisted.internet import reactor
import scrapy
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerRunner
from wealth_x.spiders.test import YourScrapingSpider
import pandas as pd
import logging
from scrapy.utils.log import configure_logging
import unicodedata
import os
from nltk.tag import StanfordNERTagger
import string
import nltk
import sys

def getNER(article):
    Temp = []
    for l in article:
        temp = nltk.sent_tokenize(l)
        Temp.extend(temp)
    Result = st.tag_sents([nltk.word_tokenize(l) for l in Temp])
    
    Info = []
    for L in Result:
        result = []
        for l in L:
            if l[0] == 'Bio':
                result.append((u'Bio',u'O'))
            elif l[0] == 'Americas':
                result.append((u'Americas',u'LOCATION'))
            else:
                result.append(l)
        final = []
        temp = []
        for i in range(len(result)-1):
            if result[i][1] == result[i+1][1]:
                temp.append(result[i][0])
            else:
                temp.append(result[i][0])
                final.append((temp,result[i][1]))
                temp = []
        
        if len(temp) == 1:
            temp.append(result[len(result)-1][0])
            final.append((temp,result[len(result)-1][1]))
        
        Final = []
        for l in final:
            if l[1] != 'O':
                Final.append(l)
        Info.append(Final)
    return Info

if __name__=='__main__':
    
    #cmdline.execute("scrapy crawl MyScrapingSpider -o result.csv --logfile log1.txt".split())
    
    settings = get_project_settings()
    configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})

    runner = CrawlerRunner(settings)
    runner.crawl('MyScrapingSpider')

    d = runner.join()
    d.addBoth(lambda _: reactor.stop())

    reactor.run()  # the script will block here until all crawling jobs are finished

    result = pd.read_csv('output.csv')
    
    text = list(result['Text'])
    links = list(result['Links'])
    
    Sentence = []
    for I in text:
        final_text = []
        temp = I.splitlines()
        for J in temp:
            i = J.strip()
            i = unicodedata.normalize('NFKD', i.decode('utf-8')).encode('ascii','ignore')
            final_text.append(i.strip('\t'))
        indices = [i for i, x in enumerate(final_text) if x == '']
        Final_text = []
        if indices[0] != 0:
            Final_text.append(' '.join(final_text[:indices[0]]))
        for i in range(len(indices)-1):
            if indices[i+1] - indices[i] > 1:
                Final_text.append(' '.join(final_text[indices[i]+1:indices[i+1]]))
        Sentence.append(Final_text)
    
    os.environ['CLASSPATH'] = '/Users/wangyong199207/stanford-ner-2015-04-20/stanford-ner.jar'
    st = StanfordNERTagger('/Users/wangyong199207/stanford-ner-2015-04-20/classifiers/english.all.3class.distsim.crf.ser.gz') 

    A = map(getNER,Sentence)

    Info = {}
    for I in range(len(A)):
        Final = []
        for l in A[I]:
            temp = 1
            for j in l:
                if j[1] == 'PERSON':
                    temp = 0
            if temp == 0:
                Final.append(l)
        Info[links[I]] = Final 
    
    UniqueSingle = []
    UniqueFull = []
    NoUnique = []
    All = []
    Name = []
    for l in links:
        J = []
        name = {}
        nounique = []
        uniquesingle = []
        uniquefull = []
        for i in Info[l]:
            for j in i:
                if j[1] == 'PERSON': #and len(j[0]) > 1:
                    if j not in J:
                        J.append(j)
                        if len(j[0]) > 1:
                            name[(j[0][-1])] = name.get((j[0][-1]),0) + 1
                            name[(j[0][0])] = name.get((j[0][0]),0) + 1
        for j in J:
            if len(j[0]) == 1 and (name.get(j[0][0]) > 1 or name.get(j[0][-1]) > 1):
                nounique.append(j)
            elif len(j[0]) > 1 or j[0][0] not in name:
                uniquefull.append(j)
            else:
                uniquesingle.append(j)
        All.append(J)
        UniqueFull.append(uniquefull)
        UniqueSingle.append(uniquesingle)
        NoUnique.append(nounique)
        Name.append(name)
    

    Dossier = []
    for d in range(len(All)):
        Name = {}
        for l in All[d]:
            name = []
            if l in UniqueFull[d]:
                for j in Info[links[d]]:
                    if ([l[0][0]],'PERSON') in j or ([l[0][-1]],'PERSON') in j or l in j:
                        name.extend(j)
            elif l in NoUnique[d]:
                for j in Info[links[d]]:
                    if l in j:
                        name.extend(j)
    
            final = []
    
            for i in name:
                if i not in final:
                    final.append(i)

            if len(final) > 1:
                Name[' '.join(l[0])] = final
            
        Dossier.append(Name)
        
    File = []
    for i in range(len(Dossier)):
        for v,w in Dossier[i].items():
            File.append((links[i],v,w))
    
    Final = pd.DataFrame(File,columns = ['Link','name','info'])
    Final.to_csv(sys.argv[1],encoding = 'utf-8')
    
    