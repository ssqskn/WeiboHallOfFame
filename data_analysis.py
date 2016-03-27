#coding:utf8
import time
from mongodb_connector import MongodbConnector

class DataAnalyzer():
    def __init__(self):
        self.client = MongodbConnector()
        self.db_weibo = self.client.get_database('weibo_hall_of_fame')
        self.collection_tags = self.client.get_collection('weibo_hall_of_fame', 'tags')
        self.collection_intro = self.client.get_collection('weibo_hall_of_fame', 'introduction')
        self.collection_verified_text = self.client.get_collection('weibo_hall_of_fame', 'verifiedText')
        self.collection_weibo = self.client.get_collection('weibo_hall_of_fame', 'microblogs')
        
    def print_sample_record(self):
        weibo_sample_record = self.collection_weibo.find_one()
        for i in weibo_sample_record['microblogs'][0:5]:
            print i
        print self.collection_intro.find_one()
        print self.collection_verified_text.find_one()
        print self.collection_tags.find_one()
    
    def find_personally(self, collection_name, statement_dict):
        collection = self.client.get_collection('weibo_hall_of_fame', collection_name)
        results = collection.find(statement_dict)
        print "Get %s records by condition of %s" % (results.count(), str(statement_dict))
        return results
        

def DB_introduction_analyzer():
    n_sample = 1   ##打印样本数
    data_analyzer = DataAnalyzer()
    print "(1) Introduction表中所有记录数: ",data_analyzer.collection_intro.count() #id: 66288
    print "(2) Introduction表中的前%d个样本：" % n_sample  ## id, profession, introduction三个字段
    for rec in data_analyzer.collection_intro.find()[0:n_sample]:
        for k,v in rec.iteritems():
            print "Key:",k, " --- Value:",v
        print ""
    print "(3) Introduction表中profession的类目："  ## 共30类职业
    for i in data_analyzer.collection_intro.distinct("profession"):
        print i,"(%d)" % data_analyzer.collection_intro.find({"profession":i}).count()
    
    """
    data_analyzer.find_personally("introduction", statement_dict={'profession':u'\u5a31\u4e50'})
    ##三种查找方法
    data_analyzer.find_personally("introduction", {'profession':{'$size':1}}).count()
    data_analyzer.find_personally("introduction", {'$where':"this.profession.length<2"}).count() #where语句较慢
    data_analyzer.find_personally("introduction", {'profession.0':{'$exists':1}}).count()
    """

def DB_tags_analyzer():
    n_sample=1
    tag_count_list = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
    data_analyzer = DataAnalyzer()
    print "(1) Tags表中所有记录数: ",data_analyzer.collection_tags.count()  #41217
    print "(2) Tags表中的前%d个样本：" % n_sample  ##id, tags两个标签 每个tag=>{tag_id:tag_name, 权重:xxx}
    for rec in data_analyzer.collection_tags.find()[0:n_sample]:
        for k,v in rec.iteritems():
            print "Key:",k, " --- Value:",v
        print ""
    print "(3) Tags表中的tag数目："
    for i in tag_count_list:
        print "    Tag数目大于等于%d - %d" %(i,data_analyzer.collection_tags.find({'tags.'+str(i):{"$exists":1}}).count())    

def DB_verified_text_analyzer():
    n_sample=1
    data_analyzer = DataAnalyzer()
    print "(1) VerifiedText表中所有记录数: ",data_analyzer.collection_verified_text.count()  #66288
    print "(2) VerifiedText表中的前%d个样本：" % n_sample  ## id, profession, verifiedText三个字段
    for rec in data_analyzer.collection_verified_text.find()[0:n_sample]:
        for k,v in rec.iteritems():
            print "Key:",k, " --- Value:",v
        print ""
    print "(3) VerifiedText表中profession的类目："  ## 共30类职业
    for i in data_analyzer.collection_verified_text.distinct("profession"):
        print i,"(%d)" % data_analyzer.collection_verified_text.find({"profession":i}).count()
    
def DB_microblogs_analyzer():
    n_sample=1
    #microblogs_count_list = [50,100,200,300,400,500]
    microblogs_count_list = []
    
    data_analyzer = DataAnalyzer()
    print "(1) microblogs表中所有记录数: ",data_analyzer.collection_weibo.count()  #61867
    print "(2) microblogs表中microblogs的数目："
    for i in microblogs_count_list:
        t0 = time.time()
        print "    weibo数目大于等于%d - %d， " %(i,data_analyzer.collection_weibo.find({'microblogs.'+str(i):{"$exists":1}}).count()),
        print round((time.time() - t0),1), "secs"
    """
        microblogs表中microblogs的数目：
        weibo数目大于等于1 - 60373，  328.7 secs
        weibo数目大于等于20 - 56588，  237.7 secs
        weibo数目大于等于50 - 54215，  233.2 secs
        weibo数目大于等于100 - 51575，  232.6 secs
        weibo数目大于等于200 - 47638，  231.1 secs
        weibo数目大于等于300 - 44410，  236.6 secs
        weibo数目大于等于400 - 41492，  237.1 secs
        weibo数目大于等于450 - 40118，  235.0 secs
        weibo数目大于等于480 - 38548，  233.5 secs
        weibo数目大于等于500 - 109，  239.6 secs
    """    
    print "(3) microblogs表的字段结构"
    for k,v in data_analyzer.collection_weibo.find_one()['microblogs'][0].iteritems():
        print k
    """
        uid  用户id
        mid 微博id
        id 
        idstr 字符串id
        text 微博文本
        reposts_count 转发数
        comments_count 评论数
        attitudes_count 赞数
        truncated  是否被截断
        visible 是否可见 {字典}
        source 创建来源
        in_reply_to_status_id 回复状态id
        in_reply_to_screen_name 
        in_reply_to_user_id 回复某人的id
        pic_urls 图片url [数组]， 如果有图片，则有下面3个字段
            thumbnail_pic 索引图片地址
            bmiddle_pic  缩小的图片地址
            original_pic  原始图片地址
        favorited 是否收藏
        geo 地理位置
        created_at 创建时间
        mlevel
    """

if __name__ == '__main__':
    DB_microblogs_analyzer()
    
        