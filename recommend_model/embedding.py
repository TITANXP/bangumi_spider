import numpy as np
from gensim.models.word2vec import Word2Vec
import pymongo
import datetime
import collections
import redis

def load_data():
    """
    从mongodb加载用户收藏记录
    :return: 用户的收藏记录
    """
    print('加载数据...')
    client = pymongo.MongoClient('localhost')
    db = client['bangumi_test']
    user_collection = db['user1']
    user_collects = user_collection.find({}, {'_id': 0, 'user_id': 1, 'collects': 1})
    return user_collects

def parse_usercollect_to_sequences(user_collects):
    """
    将用户收藏记录转换为只保留动画id的序列，用于模型的训练
    :param user_collects:
    :return:
    """
    collect_sequences = []
    for user_collect in user_collects:
        # 如果用户收藏的动画不为空
        if len(user_collect['collects']) > 0:
            collect_sequences.append([collect[0] for collect in user_collect['collects']])
    return collect_sequences

def train_model(data):
    print('训练模型...')
    model = Word2Vec(data,
                     workers=4,
                     vector_size=30,
                     min_count=2,
                     window=5,
                     epochs=50)
    return model

def get_user_emb(user_collects, all_anime_embs):
    """
    根据动画embdding 计算用和embedding
    :param user_collects:
    :param all_anime_embs:
    :return:
    """
    print('计算用户Embedding...')
    user_collects.rewind()
    user_embs = {}
    for user_collect in user_collects:
        # 取出当前用户收藏的所以动画id
        anime_ids = [i[0] for i in user_collect['collects']]
        anime_embs = np.array([all_anime_embs.get(anime_id) for anime_id in anime_ids])
        # 过滤掉没有embedding的动画
        mask = [i is not None for i in anime_embs]
        anime_embs = anime_embs[mask]
        collects = np.array(user_collect['collects'])[mask]
        # 计算每个动画的权重
        weights = [ calc_weight(i) for i in collects]
        # 计算加权平均，得到用户的Embedding
        if len(anime_embs) > 0:
            user_embs[user_collect['user_id']] = np.average(anime_embs.tolist(), weights=weights, axis=0)
    return user_embs



def calc_weight(collect):
    """
    计算一条收藏记录的权重
    :param collect:
    :return:
    """
    # 时间衰减系数
    alpha1 = 0.0001
    # 评分系数
    alpha2 = 0.1
    event_time = datetime.datetime.strptime(collect[2], '%Y-%m-%d')
    # 时间权重
    weight1 = 1 / (1 + alpha1 * ( datetime.datetime.now() - event_time).days)
    # 评分权重
    score = int(collect[1])
    weight2 = 1 if score == 0 else 1 / (1 + alpha2 * (5 - score))
    return weight1 * weight2

def save_as_csv(data, path):
    print('保存'+path , len(data))
    with open(path, 'w') as f:
        for k,v in data.items():
            f.write(str(k))
            for i in v:
                f.write(',' + str(i))
            f.write('\n')

def save_to_redis(data, key_prefix):
    """
    将Embedding保存到redis
    :param data:
    :param key_prefix:
    :return:
    """
    print('保存'+key_prefix+'到redis' , len(data))
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    for k,v in data.items():
        emb = ','.join([str(i) for i in v])
        r.set(key_prefix + str(k), emb)


if __name__ == '__main__':
    user_collects = load_data()
    collect_sequences = parse_usercollect_to_sequences(user_collects)
    model = train_model(collect_sequences)
    # 保存embedding向量
    # recommend_model.wv.save_word2vec_format('emb.csv')
    anime_embs = collections.defaultdict(None)
    for i in model.wv.key_to_index:
        anime_embs[i] = model.wv.get_vector(i)
    save_as_csv(anime_embs, '../bangumi_spider/animeEmb.csv')
    # 计算用户Embedding
    user_embs = get_user_emb(user_collects,anime_embs)
    save_as_csv(user_embs, '../bangumi_spider/userEmb.csv')
    # 写入redis
    save_to_redis(anime_embs, 'animeEmb:')
    save_to_redis(user_embs, 'userEmb:')
