package eu.fakod

import eu.fakod.twitter.{Tweet, TwitterApiTweets, TweetConsumer}
import org.vertx.scala.core.json.Json
import org.vertx.scala.platform.Verticle
import rx.lang.scala.schedulers.ComputationScheduler

class TwitterVerticle extends Verticle with TweetConsumer with TwitterApiTweets {

  override def start() = {
    val ts = tweets
    ts.subscribe(this, ComputationScheduler())
    ts.toBlockingObservable.foreach(_ => ())
  }


  def OAuthConsumerKey = container.config.getString("OAuthConsumerKey")

  def OAuthConsumerSecret = container.config.getString("OAuthConsumerSecret")

  def OAuthAccessToken = container.config.getString("OAuthAccessToken")

  def OAuthAccessTokenSecret = container.config.getString("OAuthAccessTokenSecret")


  def onTweet(tweet: Tweet): Unit = {

    val jsonTweet = Json.obj(
      "language" -> tweet.getLang,
      "username" -> tweet.getUser.getName
    )
    vertx.eventBus.publish("tweet-feed", jsonTweet)
  }


  def handleException(exception: Throwable): Unit = exception.printStackTrace()
}