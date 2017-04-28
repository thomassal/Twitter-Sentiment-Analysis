import org.apache.spark._
import org.apache.spark.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import twitter4j.TwitterFactory
import twitter4j.auth.AccessToken
import org.apache.spark.storage.StorageLevel

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import java.io.StringReader
import org.apache.lucene.analysis._
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.util.StopwordAnalyzerBase
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable
import twitter4j.Status

import scala.collection.mutable.MutableList

object auth {
      val config = new twitter4j.conf.ConfigurationBuilder()
        .setOAuthConsumerKey("***")
        .setOAuthConsumerSecret("***")
        .setOAuthAccessToken("***")
        .setOAuthAccessTokenSecret("***")
        .build
}


/*Streaming tweets from Twitter API 
 * and detect sentiment of them*/           
object TwitterSentimentAnalysis {
	
	def createNBModel(sqlContext: SQLContext): NaiveBayesModel = {
		val customSchema = StructType(Array(
			StructField("Sentiment", StringType, true),
			StructField("Id", StringType, true),
			StructField("Time", StringType, true),
			StructField("Query", StringType, true),
			StructField("Username",StringType, true),
			StructField("SentimentText", StringType, true)))
			
		val data = sqlContext
            .read.format("com.databricks.spark.csv")
            .option("header", "true")
            .schema(customSchema)
            .load("data/training.1600000.processed.noemoticon.csv")
		
		val source2 = data.registerTempTable("sentiment_data")
		val df2 = sqlContext.sql("select Id, (case when Sentiment=4 then Sentiment-3 else Sentiment end)"+
			"as Sentiment, Query, SentimentText from sentiment_data")
          
		val hashingTF = new HashingTF()
		val labelAndTweet = df2.map(t => (t.getString(1), (t.getString(3))))
		val documents = labelAndTweet.map{case(sentiment, tweet) => (sentiment.toFloat.toInt, tokenize(tweet))}
		val featurized = documents.map{case(label, words) => LabeledPoint(label, hashingTF.transform(words))}
		val Array(train, test) = featurized.randomSplit(Array(0.7, 0.3))
		val model = NaiveBayes.train(featurized)
		
		val predictionAndLabels = test.map(point => (model.predict(point.features), point.label))
		val correct = predictionAndLabels.filter{case(predicted, actual) => predicted == actual}
		
		model
	}
	
	def predictSentiment(model: NaiveBayesModel, tokens:Seq[String], hashingTF: HashingTF) : Int = {
		val score = model.predict(hashingTF.transform(tokens))
		return score.toInt
	}
	
	def tokenize(content: String): Seq[String] = {
        val tReader = new StringReader(content)
        val analyzer = new EnglishAnalyzer()
        val tStream = analyzer.tokenStream("contents", tReader)
        val term = tStream.addAttribute(classOf[CharTermAttribute])
        tStream.reset()

        val result = mutable.ArrayBuffer.empty[String]
        while(tStream.incrementToken()) {
            result += term.toString
        }
        result
    }
    //add hashtags to dictionary
    def addToDict(tags: String) = tags.split("\\W+").distinct.zipWithIndex.toMap


    /*loopy belief propagation*/
    def LBP(hashtagGraph: Graph [String,String], numIter: Int): Graph [String,String] = {
    	initialize
    	val intialmsg = hashtagGraph.aggregateMessages[Double] ( 
    		tripletFields => {
    			tripletFields.sendToSrc(1.0) 
    			tripletFields.sendToDst(1.0)},
    			(a, b) => a)
    	// Run BP for numIter iterations.
    	for (iter <- Range(0,numIter)) {
    		// For each sentiment, have that sentiment receive messages from neighbors.
      		for (color <- Range(0, 1)) {
      			val msgs: VertexRDD[Double] = hashtagGraph.aggregateMessages(
      				tripletFields => {
      					tripletFields.sendToDst(tripletFields.srcId.numVertices + tripletFields.dstId.numVertices)
      				},
      				(_+_))
      			// Receive messages, and update beliefs for vertices of the current color.
      			hGraph = hashtagGraph.outerJoinVertices(msgs) {
      				case (vID, vAttr, optMsg) =>
      				if (vAttr.color == color) {
      					val x = vAttr.a + optMsg.getOrElse(0.0)
      					val newBelief = math.exp(-log1pExp(-x))
      					VertexAttr(vAttr.a, newBelief, color)
      				} else {
      					vAttr
      				}
      			}

      		}

    	}
    	hGraph.mapVertices((_, attr) => attr.belief).mapEdges(_ => ()) 
    }
    def log1pExp(x: Double): Double = {
	    if (x > 0) {
	    	x + math.log1p(math.exp(-x))
	    } else {
	    	math.log1p(math.exp(x))
	    }
	}
    case class VertexAttr(a: Double, belief: Double, color: Int)
    case class EdgeAttr(b: Double)
	
	/****************main function*********************/
	def main (args: Array[String]){
		if (args.length <1){
			System.err.println("Usage: TwitterSentimentAnalysis <filters>")
			System.exit(1)
		}
		//tweet filters
		val filters = args
		
		//keys for twitter api
		val consumerKey = "***"
		val consumerSecret = "***"
		val accessToken = "***"
		val accessTokenSecret = "***"
		// Set the system properties so that Twitter4j library used by twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)*/
		
		
		// Create a local StreamingContext with two working thread and batch interval of 1 second.
		// The master requires 2 cores to prevent from a starvation scenario.
		val conf = new SparkConf().setMaster("local[4]").setAppName("TwitterSentimentAnalysis")
		//naiveBayes classifier
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val broadcastedModel = createNBModel(sqlContext)
		
		val ssc = new StreamingContext(sc, Seconds(5))
		//twitter api authorization
		val twitter_auth = new TwitterFactory(auth.config)
		val a = new twitter4j.auth.OAuthAuthorization(auth.config)
		val atwitter : Option[twitter4j.auth.Authorization] =  Some(twitter_auth.getInstance(a).getAuthorization())
		
		val tweets = TwitterUtils.createStream(ssc, atwitter, filters, StorageLevel.DISK_ONLY_2)
		//english tweets
		val tweets_en = tweets.filter(s => s.getLang == "en")
		
		//filter text from twitter4j response 
		val statuses = tweets_en.map(status =>status.getText())
		
		//filter hashtags only
		val words = statuses.flatMap(status => status.split(" "))
		val hashtags = words.filter(word => word.startsWith("#"))
		
		tweets_en.foreachRDD{(rdd, time) =>
			 val hashingTF =new HashingTF()
       rdd.map(t => {
         Map(
           "user"-> t.getUser.getScreenName,
           "created_at" -> t.getCreatedAt,//.toInstant.toString,
           "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
           "text" -> t.getText,
           //"hashtags" -> t.getHashtagEntities.map(_.getText),
           //"retweet" -> t.getRetweetCount
           //"language" -> detectLanguage(t.getText),
           //"sentiment" -> detectSentiment(t.getText).toString
           "sentiment" -> predictSentiment(broadcastedModel, tokenize(t.getText()), hashingTF).toString()
         )
       })
     }
     
     val vertexList = MutableList.empty[Array[(Long,String)]]
     val edgeList = MutableList.empty[Array[Edge[String]]]
     val tagsize = MutableList.empty[Int]
     tagsize ++= MutableList(0)
     hashtags.foreachRDD(hashtagsRdd => {
		 //make a collection of hashtag in order to create the graph
		 //(id, hashtag, links)
		 //ex. 1L, #iphone, (1L,2L)
		
		 //remove duplicates
		 val tagList = hashtagsRdd.collect.distinct //tagList = Array("#tag1","#tag2")
		 
		 //count the index of vertex
		 val counter =tagsize.foldLeft(0)(_+_)
		 tagList.foreach(println)
		 val numtag = tagList.zipWithIndex.map{ case (s,i)=>((i+counter+1).longValue,s)} //numtag = Array((1,#tag1), (2,#tag2))
		 vertexList ++= MutableList(numtag)
		 tagsize ++= MutableList(tagList.size)
		 //single array of hashtags
		 val v = vertexList.flatten.toArray
		 
		 //take the keys
		 val keys = numtag.map(t => t._1)
		 
		 //make the links of hashtag
		 val links = keys.combinations(2).toArray
		 //convert to Array(Edge(1L, 2L, ""), Edge(1L, 4L, ""))
		 val e = links.map(x => Edge(x(0),x(1),""))
		 //convert an array of lists
		 
		 edgeList ++= MutableList(e)
		 //join all arrays
		 val ed = edgeList.flatten.toArray
		 
		 val vertices: RDD[(VertexId, String)] = sc.parallelize(v)
		 val edges: RDD[Edge[String]] = sc.parallelize(ed)
		 //define the graph
		 val inputGraph = Graph(vertices, edges)
		 // graph vertices
		 inputGraph.vertices.collect.foreach(println)
		 // graph edges
		 inputGraph.edges.collect.foreach(println)
		 val lg = LBP_Pregel(inputGraph)
		 lg.vertices.collect.foreach(println)
	 })
	 
	 
     ssc.start()
     ssc.awaitTermination()
	}
	
}
