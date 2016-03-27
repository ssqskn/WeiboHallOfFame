#coding:utf8
import re
import time
import jieba
import threading, multiprocessing
import pandas as pd
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer
from mongodb_connector import MongodbConnector


def load_stopwords(path):
    stopwords = {}
    with open(path, 'r+') as f:
        for line in f:
            stopwords[line.strip().decode('utf8')] = 1
    return stopwords

def get_weibo_content(n_users, save_path):
    t0 = time.time()
    mongo = MongodbConnector()
    weibo_collection = mongo.get_collection("weibo_hall_of_fame", "microblogs")
    weibo_content = []
    for weibo in weibo_collection.find().limit(n_users):
        weibo_list = weibo['microblogs']
        for i in weibo_list:
            weibo_content.append([i['uid'],i['mid'],i['text']])
    weibo_df = pd.DataFrame(weibo_content, columns=['uid','mid','text'])
    weibo_df.to_csv(save_path, index=False, encoding='utf8')
    print "Totally %s microblogs have been fetched. %s secs" % (len(weibo_df), round((time.time()-t0),1))
    return weibo_df

def get_weibo_content_from_disk(path):    
    return pd.read_csv(path)

def weibo_text_cleansing(text):
    if isinstance(text, list):   #针对用[]引用的微博
        text = ""
    elif isinstance(text, int) or isinstance(text, float):
        text = str(text)    
    text = text.strip().replace(',','')
    for rp in regex_pattern:
        text = rp.sub('',text)
    return text

def remove_stopwords(word_sequence):
    if len(word_sequence) == 0:
        return ""
    word_list_cleaned = [word for word in word_sequence.split(" ") if not stop_words.has_key(word)]
    return " ".join(word_list_cleaned)


class TagsGenerator():
    def __init__(self, weibo_df):
        self.text_tags_save_path = "tmp/weibo_content_tags_added.csv"    
        self.weibo_df = weibo_df # a dataframe containing "uid, mid, text"
        self.weibo_count = len(self.weibo_df)
        self.weibo_segment = []  # 储存分词后的微博
        self.tfidf_words = [] #储存所有参与tfidf计算的单词(即tfidf矩阵的列向量)
        self.tfidf = None       #储存tfidf的计算结果, sklearn计算结果是一个scipy.sparse.csr的稀疏矩阵
        
    def _clean_text(self, weibo_list, n_proc=1):
        pool = multiprocessing.Pool(n_proc)
        results = pool.map(weibo_text_cleansing, weibo_list)
        pool.close()
        pool.join()
        return results
    
    def _remove_stopwords(self):
        t0 = time.time()
        pool = multiprocessing.Pool(30)
        results = pool.map(remove_stopwords, self.weibo_segment)
        pool.close()
        pool.join()
        self.weibo_segment = results
        print "Remove stop words. %s secs" % round((time.time()-t0),1)
    
    def get_tfidf_words(self):
        return self.tfidf_words
    
    def get_tfidf_result(self, index):
        return self.tfidf[index]
        
    def segment_word(self):
        t0 = time.time()
        n_err = 0
        weibo_list = self._clean_text(list(self.weibo_df['text']), n_proc=30) #微博文本清洗
        pd.DataFrame(weibo_list,columns=['text']).to_csv("tmp/clean_weibo_text.csv", index=False, encoding='utf8')

        for i in range(self.weibo_count):
            try:
                self.weibo_segment.append(" ".join(list(jieba.cut(weibo_list[i]))))
            except Exception, e:
                n_err += 1
                self.weibo_segment.append("")
                continue
            
        self._remove_stopwords()
        print "Totally %s microblogs have been segmented, with %s errors while decoding" % (self.weibo_count, n_err)    
        print "Weibo text segmented to words list. %s secs" % round((time.time()-t0),1)
        
    def calculateTFIDF(self):
        if len(self.weibo_segment) == 0:
            self.segment_word()
        t0 = time.time()
        vectorizer = CountVectorizer() #转换为词频矩阵
        transformer = TfidfTransformer() #转换为tfidf矩阵
        self.tfidf = transformer.fit_transform(vectorizer.fit_transform(self.weibo_segment))
        print "Calculate tfidf vector. %s secs" % round((time.time()-t0),1)
        """Tf-idf的生成结果"""
        self.tfidf_words = vectorizer.get_feature_names() # tfidf的纵坐标-即所有单词
        
    def _get_tags(self, row):
        """ 从tfidf计算得到的稀疏矩阵中获取某个微博的非0词和tfidf值"""
        if len(self.tfidf_words) == 0:
            self.calculateTFIDF()
        tags = []
        for idx in self.tfidf[row].nonzero()[1]:
            tfidf_val = self.tfidf[row].getcol(idx).toarray()[0][0]
            if tfidf_val > tfidf_threshold_low and tfidf_val < tfidf_threshold_high:  ## tfidf在阈值范围内纳入tags中
                tags.append({idx: {self.tfidf_words[idx]: round(tfidf_val,3)}})
        self.tags.append((row, tags))
        
    def generate_tags(self, n_proc=1):
        if len(self.tfidf_words) == 0:
            self.calculateTFIDF()
        t0 = time.time()
        self.tags = []
        
        if n_proc > 1:
            """ 多线程获取tags """
            for i in range(self.weibo_count / n_proc):
                threads = []
                for j in range(n_proc):
                    t = threading.Thread(target=TagsGenerator._get_tags, args=(self, i*n_proc+j))
                    threads.append(t)
                for j in range(n_proc):
                    threads[j].start()
                for j in range(n_proc):
                    threads[j].join()
            # 剩余的text
            threads = []
            for j in range((i+1)*n_proc, self.weibo_count):
                t = threading.Thread(target=TagsGenerator._get_tags, args=(self, j))
                threads.append(t)
            for j in range(len(threads)):
                threads[j].start()
            for j in range(len(threads)):
                threads[j].join()
                
            self.tags.sort(key=lambda x: x[0])
            
        else:
            """ 单线程获取tags，默认采用 """
            for i in range(self.weibo_count):
                self._get_tags(i)
        
        self.tags = [i[1] for i in self.tags]
        self.weibo_df['text_tags'] = self.tags
        self.weibo_df.to_csv(self.text_tags_save_path, index=False, encoding='utf8')
        print "Generate tags and add to dataframe. %s secs" % round((time.time()-t0),1)
        

if __name__ == '__main__':
    n_users = 1000
    tfidf_threshold_low = 0.3
    tfidf_threshold_high = 0.99
    stop_words = load_stopwords(path="stopwords/stopwords.txt")
    regex_pattern = [
                     re.compile("http\:\/\/[\S]+"), #清除网址 
                     re.compile("\/\/.+"),          #清除引用
                     re.compile(u"（分享自.+?）"),    #清除分享来源信息
                     re.compile(u"（来自.+?）"),     #清除分享来源信息
                     re.compile("@[\S]+?[\s]"),     #清除@
                     re.compile("@[\S]+?"),         #清除结尾处的@
                     re.compile("\[.+?\]"),         #清除表情
                     re.compile(u"回复@.+?\:"),     #清除回复标头
                     re.compile("\d"),              #清除数字
                    ]
   
    weibo_df = get_weibo_content(n_users, "tmp/weibo_content.csv")
    #weibo_df = get_weibo_content_from_disk("tmp/weibo_content.csv")
    tags_generator = TagsGenerator(weibo_df)
    tags_generator.generate_tags(n_proc=1)

    