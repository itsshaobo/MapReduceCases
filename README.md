# MapReduceCases
给出了MapReduce的应用实例
# 1. FindSimiliarUsers 
给出用户和该用户对电影的评分，通过用户的喜好找到每个用户的top3相似用户
输入为 <userId, movieId, rating> 
输出为 <userId: similarUserId1, similarUserId2, similarUserId3>


# 2.合并hdfs中的小文件
通过重写RecordReader的方式，讲一个文件作为一个输入，一个小文件构建一个mapper处理他，
产生的sequenceFile的格式为 <key:filePath, value:fileContent>

# 3.找到共同好友，给出一个用户和他的好友列表，找到两两用户的共同好友
