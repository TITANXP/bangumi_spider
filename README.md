# 1. 技术栈

## 1.1 bangumi_spider

**爬虫**

- Scrapy
- Requests
- Xpath
- MongoDB
- ElasticSearch

## 1.2 recommend_model

**特征工程、召回层模型、排序层模型**

- Pandas
- Numpy
- Matplotlib
- TensorFlow2
- Gensim
- TesorFlow Serving
- Docker

# 2. 运行

## 2.1 bangumi_spider

1. 启动MongoDB

2. 启动Redis

   ```sh
   redis-server "D:\Program Files\Redis-x64-5.0.9\redis.windows.conf"
   ```

3. 启动ElasticSearch

4. 全量爬取动画数据

   ```sh
   scrapy crawl anime
   ```

5. 将所有的动画数据导入ElasticSearch
   ```sh
   mongodb-to-elasticsearch.py
   ```

6. 全量爬取用户“看过”的动画

   ```sh
   scrapy crawl user
   ```

7. 全量爬取用户“抛弃”的动画
   ```sh
   scrapy crawl user_dropped
   ```

8. 全量爬取网站的动画标签
   ```sh
   scrapy crawl tag
   ```

9. 实时增量爬取用户用户“看过”的动画
   ```sh
   scrapy crawl timeline
   ```

## 2.2 recommend_model

1. 特征工程，生成训练集、测试集到CSV文件，生成用户特征、动画特征到Redis
	```sh
	FeatureEng.py
	```

2.  召回层，生成用户和动画的Embedding向量
	```sh
	embedding.py
	```

3. 排序层，训练模型
	```sh
	WideNDeep.py
	```
	
	使用TensorBoard
	
	[tensorflow2 tensorboard可视化使用](https://blog.csdn.net/u010554381/article/details/106261115)

    ```shell
    tensorboard.exe --logdir=D:\python_project\bangumi_spider\bangumi_spider\resources\tensorboardlog
   
    http://localhost:6006/ 
    ```

4. 模型上线

    ```sh
    # 1. 拉取镜像
    docker pull tensorflow/serving
    # 2. 运行容器
    docker run -t --rm -p 8501:8501 -v "D:\IDEA\bangumi-recommend-system\src\main\resources\model\neuralcf:/models/recmodel" -e MODEL_NAME=recmodel tensorflow/serving
