import numpy as np
import pymongo
import pandas as pd
import redis
import logging


def get_rating_df(user_id):
    """
    读取用户评分数据
    :return:
    """
    # logger.info('加载用户评分数据...')
    client = pymongo.MongoClient('localhost', 27017)
    db = client['bangumi_test']
    collection = db['user1']
    ratings = collection.find({'user_id': user_id},{'_id': 0, 'user_id': 1, 'collects': 1, 'tags': 1})
    rating_df =  pd.DataFrame(list(ratings))
    rating_df.rename(columns={'tags': 'user_tags', 'user_id': 'userId'}, inplace=True)
    return rating_df

def get_anime_df(anime_ids):
    """
    读取动画数据
    :return:
    """
    # logger.info('加载动画数据...')
    client = pymongo.MongoClient('localhost', 27017)
    db = client['bangumi_test']
    collection = db['anime1']
    animes = collection.find({'id': {'$in' : anime_ids}},{'_id': 0, 'id': 1, 'api.rating.total': 1, 'api.rating.score': 1, 'tags': 1})
    anime_df = pd.DataFrame([i for i in animes if 'rating' in i['api'] ])
    anime_df.rename(columns={'id':'animeId', 'tags': 'anime_tags', 'api': 'anime_api'}, inplace=True)
    return anime_df


def explode_collects(rating_df):
    """
    将用户的收藏序列展开
    :param rating_df:
    :return:
    """
    # logger.info('将用户收藏序列展开...')
    # 将评分为0的替换为6
    rating_df['collects'] = rating_df['collects'].apply(lambda collects: [item if item[1] != 0 else [item[0], 6, item[2]] for item in collects])
    # 保存用户收藏的动画序列，用于后面添加用户特征
    rating_df['collect_seq'] = rating_df['collects']
    rating_df['like_seq'] = rating_df['collects'].apply(lambda items: [item for item in items if item[1] > 5 or item[1] == 0])

    # 按收藏记录进行展开
    rating_df = rating_df.explode('collects')
    rating_df = rating_df[pd.notna(rating_df['collects'])]
    # 将收藏记录这一列拆成3列
    rating_df['animeId'] = rating_df['collects'].apply(lambda x: x[0])
    rating_df['score'] = rating_df['collects'].apply(lambda x: x[1])
    rating_df['timestamp'] = rating_df['collects'].apply(lambda x: x[2])
    rating_df = rating_df.drop('collects', axis=1)
    return rating_df

def add_anime_feature(rating_df, anime_df):
    """
    添加动画特征
    :param rating_df:
    :param anime_df:
    :return:
    """
    # logger.info('添加动画特征...')
    # 为了使用anime_id进行join，设置索引
    rating_df.set_index('animeId', inplace=True)
    anime_df.set_index('animeId', inplace=True)
    rating_df = rating_df.join(anime_df, on='animeId', how='inner')
    # 取消索引
    rating_df.reset_index(inplace=True)
    anime_df.reset_index(inplace=True)
    rating_df['score'] = rating_df['anime_api'].apply(lambda x: x['rating']['score'])
    rating_df['votes'] = rating_df['anime_api'].apply(lambda x: x['rating']['total'])
    rating_df['animeTag1'] = rating_df['anime_tags'].apply(lambda tags: tags[0]['name'] if len(tags) > 0 else '')
    rating_df['animeTag2'] = rating_df['anime_tags'].apply(lambda tags: tags[1]['name'] if len(tags) > 1 else '')
    rating_df['animeTag3'] = rating_df['anime_tags'].apply(lambda tags: tags[2]['name'] if len(tags) > 2 else '')
    rating_df = rating_df.drop(['anime_api', 'anime_tags'], axis=1)
    return rating_df

def add_user_feature(rating_df):
    """
    添加用户特征
    :param rating_df:
    :return:
    """
    # logger.info('添加用户特征...')
    # 用户好评动画id
    rating_df['userRatedAnime1'] = rating_df['like_seq'].apply(lambda item: item[0][0] if len(item) > 0 else 0)
    rating_df['userRatedAnime2'] = rating_df['like_seq'].apply(lambda item: item[1][0] if len(item) > 1 else 0)
    rating_df['userRatedAnime3'] = rating_df['like_seq'].apply(lambda item: item[2][0] if len(item) > 2 else 0)
    rating_df['userRatedAnime4'] = rating_df['like_seq'].apply(lambda item: item[3][0] if len(item) > 3 else 0)
    rating_df['userRatedAnime5'] = rating_df['like_seq'].apply(lambda item: item[4][0] if len(item) > 4 else 0)
    # 用户平均评分
    rating_df['userAvgRating'] = rating_df['collect_seq'].apply(lambda items: np.mean([item[1] for item in items]))
    rating_df['userRatingStddev'] = rating_df['collect_seq'].apply(lambda items: np.std([item[1] for item in items]))
    rating_df['userRatingCount'] = rating_df['collect_seq'].apply(lambda items: len(items))

    rating_df['userTag1'] = rating_df['user_tags'].apply(lambda tags: tags[0]['name'] if len(tags) > 0 else '')
    rating_df['userTag2'] = rating_df['user_tags'].apply(lambda tags: tags[1]['name'] if len(tags) > 1 else '')
    rating_df['userTag3'] = rating_df['user_tags'].apply(lambda tags: tags[2]['name'] if len(tags) > 2 else '')
    rating_df['userTag4'] = rating_df['user_tags'].apply(lambda tags: tags[3]['name'] if len(tags) > 3 else '')
    rating_df['userTag5'] = rating_df['user_tags'].apply(lambda tags: tags[4]['name'] if len(tags) > 4 else '')
    rating_df = rating_df.drop(['user_tags', 'collect_seq', 'like_seq'], axis=1)
    return rating_df

def save_user_feature_to_redis(rating_df):
    """
    提取并保存用户特征到redis
    :param rating_df:
    :return:
    """
    r = redis.Redis(host="localhost", port=6379, db=0)
    rating_df = rating_df.drop_duplicates(subset=
                                          ['userId',
                                           'userRatedAnime1',
                                           'userRatedAnime2',
                                           'userRatedAnime3',
                                           'userRatedAnime4',
                                           'userRatedAnime5',
                                           'userRatingCount',
                                           'userAvgRating',
                                           'userRatingStddev',
                                           'userTag1',
                                           'userTag2',
                                           'userTag3',
                                           'userTag4',
                                           'userTag5'
                                           ]
                                          , keep='first')
    print(rating_df)
    for row in rating_df.itertuples():
        user = {
            'userId' : getattr(row, 'userId'),
            'userRatedAnime1': int(getattr(row, 'userRatedAnime1')),
            'userRatedAnime2': int(getattr(row, 'userRatedAnime2')),
            'userRatedAnime3': int(getattr(row, 'userRatedAnime3')),
            'userRatedAnime4': int(getattr(row, 'userRatedAnime4')),
            'userRatedAnime5': int(getattr(row, 'userRatedAnime5')),
            'userRatingCount': getattr(row, 'userRatingCount'),
            'userAvgRating': getattr(row, 'userAvgRating'),
            'userRatingStddev': getattr(row, 'userRatingStddev'),
            'userTag1': getattr(row, 'userTag1'),
            'userTag2': getattr(row, 'userTag2'),
            'userTag3': getattr(row, 'userTag3'),
            'userTag4': getattr(row, 'userTag4'),
            'userTag5': getattr(row, 'userTag5'),

        }
        r.set('userF:'+str(user['userId']), str(user))
        logger.info('Redis更新用户' + str(user['userId']) + '特征:' + str(user))


def update_user_feature(user_id):
    pd.set_option('display.width', None)
    pd.set_option('max_colwidth', 100)
    # 1.读取用户评分序列
    rating_df = get_rating_df(user_id)
    # 2.展开评分序列
    rating_df = explode_collects(rating_df)
    # 3.添加动画特征
    # anime_df = get_anime_df(rating_df['animeId'].values.tolist())
    # rating_df = add_anime_feature(rating_df, anime_df)
    # 4. 添加用户特征
    rating_df = add_user_feature(rating_df)
    # 5. 提取动画特征保存到redis
    save_user_feature_to_redis(rating_df)

# 创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("[%(asctime)s]:%(levelname)s:%(message)s"))
logger.addHandler(ch)

if __name__ == '__main__':
    # update_user_feature(594436)
    update_user_feature(590763)

