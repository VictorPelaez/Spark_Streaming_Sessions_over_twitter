package com.vp.sessionization_engine

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.util.Calendar 
import java.lang.Math

/** Sessionization engine over twitter **/


object sessionization_over_twitter {
  
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source
    
    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
  /** where the magic happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "sessionization_twitter"
    val ssc = new StreamingContext("local[*]", "sessionization_twitter", Seconds(10))
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // extrac tinfo into DStreams using map()
    val tweets_lan_time = tweets.map(x => (x.getLang, x.getCreatedAt.getTime()))

    val init_timestamp = System.currentTimeMillis()   
    
    def convert_datetime(x:Long):String = {
      new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new java.util.Date (x)) }
    
    println(convert_datetime(init_timestamp))
    tweets_lan_time.count.print()


    type sessionParams = (Long,Long,Int)
        
    def gcd(x:sessionParams, y:sessionParams): sessionParams = {
          
      val date_in  = Math.min(x._1, y._1)
      val date_out = Math.max(x._2, y._2)
      val num_events = x._3 + y._3
          
      (date_in, date_out, num_events)
      }

    
   // Same types as reduceByKeyAndWindow() function returns, try to change in a future
   val preactive_sessions = tweets_lan_time.map( a =>{ (a._1, (a._2, a._2, 1 )) })
   
   val sessions = preactive_sessions.reduceByKeyAndWindow( (x:sessionParams, y:sessionParams) => gcd(x,y)
        , Seconds(120), Seconds(10))
        

   case class Session (
        date_in_str : String,
        date_out_str : String,

        date_in : Long, 
        //date_out: Long,
        num_events: Int,
        session_time: Long
        )
        
      
   sessions.map(r => (r._1, (convert_datetime(r._2._1),
                             convert_datetime(r._2._2),
                             r._2._3,
                             (r._2._2-r._2._1)/1000, //session time in seconds
                             (System.currentTimeMillis() - r._2._2)/1000
                             )
                             )).print(100)   
   
                             
    //.transform(rdd=> rdd.sortByKey(ascending=false)) 
    //val onlyActiveSessions = latestSessionInfo.filter(t => System.currentTimeMillis() - t._2._2 < SESSION_TIMEOUT)   
    //active_sessions.print(100)
    
    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
  
  
  
}