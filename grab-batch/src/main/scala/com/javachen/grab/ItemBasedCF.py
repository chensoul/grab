import math
import random

class ItemBasedCF:
    def __init__(self, datafile = None):
        self.datafile = datafile
        self.readData()
        self.splitData(3,47)

    def readData(self,datafile = None):
        self.datafile = datafile or self.datafile
        self.data = []
        file = open(self.datafile,'r')
        for line in file.readlines()[0:100*1000]:
            userid, itemid, record,_ = line.split()
            self.data.append((userid,itemid,int(record)))

    def splitData(self,k,seed,data=None,M = 8):
        self.testdata = {}
        self.traindata = {}
        data = data or self.data
        random.seed(seed)
        for user,item,record in self.data:
            if random.randint(0,7) == k:
                self.testdata.setdefault(item,{})
                self.testdata[item][user] = record
            else:
                self.traindata.setdefault(item,{})
                self.traindata[item][user] = record

    def ItemSimilarity(self, train = None):
        train = train or self.traindata
        self.itemSim = dict()
        #user_items = dict()
        item_user_count = dict() #item_user_count{item: likeCount} the number of users who like the item
        count = dict() #count{i:{j:value}} the number of users who both like item i and j
        for user, item in train.items(): #initialize the user_items{user: items}
            for i in item.keys():
                item_user_count.setdefault(i,0)
                item_user_count[i] += 1
                for j in item.keys():
                    if i == j:
                        continue
            count.setdefault(i,{})
            count[i].setdefault(j,0)
            count[i][j] += 1
        for i, related_items in count.items():
            self.itemSim.setdefault(i,dict())
            for j, cuv in related_items.items():
                self.itemSim[i].setdefault(j,0)
                self.itemSim[i][j] = cuv / math.sqrt(item_user_count[i] * item_user_count[j] * 1.0)

    def recommend(self,user,train = None, k = 8, nitem = 40):
        train = train or self.traindata
        rank = dict()
        ru = train.get(user,{})
        for i,pi in ru.items():
            for j,wj in sorted(self.itemSim[i].items(), key = lambda x:x[1], reverse = True)[0:k]:
                if j in ru:
                    continue
            rank.setdefault(j,0)
            rank[j] += wj
        #print dict(sorted(rank.items(), key = lambda x:x[1], reverse = True)[0:nitem])
        return dict(sorted(rank.items(), key = lambda x:x[1], reverse = True)[0:nitem])

    #train为训练集合，test为验证集合，给每个用户推荐N个物品
    #召回率和准确率
    def RecallAndPrecision(self,train=None,test=None,K=3,N=10):
        train = train or self.train
        test = test or self.test
        hit = 0
        recall = 0
        precision = 0
        for user in train.keys():
            tu = test.get(user,{})
            rank = self.Recommend(user,K=K,N=N)
            for i,_ in rank.items():
                if i in tu:
                    hit += 1
            recall += len(tu)
            precision += N
        recall = hit / (recall * 1.0)
        precision = hit / (precision * 1.0)
        return (recall,precision)

    #覆盖率
    def Coverage(self,train=None,test=None,K=3,N=10):
        train = train or self.train
        recommend_items = set()
        all_items = set()
        for user,items in train.items():
            for i in items.keys():
                all_items.add(i)
            rank = self.Recommend(user,K)
            for i,_ in rank.items():
                recommend_items.add(i)
        return len(recommend_items) / (len(all_items) * 1.0)

    #新颖度
    def Popularity(self,train=None,test=None,K=3,N=10):
        train = train or self.train
        item_popularity = dict()
        #计算物品流行度
        for user,items in train.items():
            for i in items.keys():
                item_popularity.setdefault(i,0)
                item_popularity[i] += 1

        ret = 0     #新颖度结果
        n = 0       #推荐的总个数
        for user in train.keys():
            rank = self.Recommend(user,K=K,N=N)    #获得推荐结果
            for item,_ in rank.items():
                ret += math.log(1 + item_popularity[item])
                n += 1
        ret /= n * 1.0
        return ret

    def testRecommend():
        ubcf = ItemBasedCF('u.data')
        ubcf.readData()
        ubcf.splitData(4,100)
        ubcf.ItemSimilarity()
        user = "345"
        rank = ubcf.recommend(user,k = 3)
        for i,rvi in rank.items():
            items = ubcf.testdata.get(user,{})
            record = items.get(i,0)
            print "%5s: %.4f--%.4f" %(i,rvi,record)

    def testItemBasedCF():
        cf = ItemBasedCF('u.data')
        cf.ItemSimilarity()
        print "%3s%20s%20s%20s%20s" % ('K',"recall",'precision','coverage','popularity')
        for k in [5,10,20,40,80,160]:
            recall,precision = cf.recallAndPrecision( k = k)
        coverage = cf.coverage(k = k)
        popularity = cf.popularity(k = k)
        print "%3d%19.3f%%%19.3f%%%19.3f%%%20.3f" % (k,recall * 100,precision * 100,coverage * 100,popularity)

    if __name__ == "__main__":
        testItemBasedCF()