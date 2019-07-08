package com.javachen.grab

// 需要的数据源是电影内容信息
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

// 基于评分数据的LFM，只需要rating数据
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int )


// 定义基于预测评分的用户推荐列表
case class UserRecs( uid: Int, recs: Seq[Recommendation] )

// 定义电影内容信息提取出的特征向量的电影相似度列表
case class MovieRecs( mid: Int, recs: Seq[Recommendation] )


// 定义一个基准推荐对象
case class Recommendation( mid: Int, score: Double )

// 定义电影类别top10推荐对象
case class GenresRecommendation( genres: String, recs: Seq[Recommendation] )

case class MongoConfig(uri:String, db:String)