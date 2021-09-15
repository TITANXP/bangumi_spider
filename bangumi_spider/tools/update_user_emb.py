import numpy as np
import pymongo
import datetime
import logging
import redis



def update_user_emb_by_id(user_id):
    """
    更新Redis中的用户Embedding
    :param user_id:
    :return:
    """
    # 获取当前用户的所有收藏动画
    user = user_collection.find_one({'user_id': int(user_id)})
    # 取出当前用户收藏的所以动画id
    anime_ids = [i[0] for i in user['collects']]
    # 从redis 查询动画的Embdding
    anime_embs = np.array([get_emb_from_redis('animeEmb:' + str(anime_id)) for anime_id in anime_ids])
    # 过滤掉没有embedding的动画
    mask = [i is not None for i in anime_embs]
    anime_embs = anime_embs[mask]
    collects = np.array(user['collects'])[mask]
    # 计算每个动画的权重
    weights = [calc_weight(i) for i in collects]
    # 计算加权平均，得到用户的Embedding
    user_emb = None
    if len(anime_embs) > 0:
        # self.logger.info('重新计算用户' + str(user_id) + 'Embedding...')
        user_emb = np.average(anime_embs.tolist(), weights=weights, axis=0)
        # 更新redis中的用户Embedding
        emb = ','.join([str(i) for i in user_emb])
        r.set('userEmb:' + str(user_id), emb)
        logger.info('Redis更新用户' + str(user_id) + 'Embedding:' + str(emb))
    return user_emb


def get_emb_from_redis(key):
    """
    从redis查询embedding字符串，并转换成list
    :param key:
    :return:
    """
    emb = r.get(key)
    if emb is not None:
        return [float(i) for i in emb.split(',')]
    return None


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
    weight1 = 1 / (1 + alpha1 * (datetime.datetime.now() - event_time).days)
    # 评分权重
    score = int(collect[1])
    weight2 = 1 if score == 0 else 1 / (1 + alpha2 * (5 - score))
    return weight1 * weight2



client = pymongo.MongoClient('localhost')
db = client['bangumi_test']
user_collection = db['user1']
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
# 创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("[%(asctime)s]:%(levelname)s:%(message)s"))
logger.addHandler(ch)

if __name__ == '__main__':
    update_user_emb_by_id(590763)

