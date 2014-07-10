package eu.fakod.twitter

import rx.lang.scala.Observable
import rx.lang.scala.schedulers.{ComputationScheduler, IOScheduler}
import twitter4j._
import scala.collection.immutable.Iterable
import rx.lang.scala.Subject

/**
 * Provides Tweets.
 *
 * You should create a trait or a class, implementing this Trait.
 * The only method to provide is `tweets`, that must return an [[rx.lang.scala.Observable]] of `[[Tweet]]s`.
 */
trait TweetProvider {

  /**
   * Tweets are provided as an [[rx.lang.scala.Observable]].
   *
   * You can implement this in any way you like, though you might want to have
   * a look at the companions object's helper traits.
   *
   * This `Observable` will be consumed by a [[eu.fakod.twitter.TweetProvider.TweetConsumer]]
   *
   * @return an `Observable` of `Tweets`
   */
  def tweets: TweetObservable
}

/**
 * Factory methods for [[TweetProvider]]
 */
object TweetProvider {

  /**
   * Created a `TweetProvider` from an existing [[TweetObservable]]
   *
   * @param os the Observable
   * @return a TweetProvider from the given Observable
   */
  def apply(os: TweetObservable): TweetProvider = new TweetProvider {
    def tweets: TweetObservable = os
  }

  /**
   * Create a `TweetProvider` from an [[scala.collection.immutable.Iterable]] of `String`s
   * where every item of the Iterable is a Tweet JSON.
   *
   * @param it the string iterable
   * @return a TweetProvider from the given string iterable
   */
  def apply(it: Iterable[String]): TweetProvider = new FromStringIterable {
    def iterable: Iterable[String] = it
  }
}

/**
 * Helper trait to create a TweetProvider from a [[scala.collection.immutable.Iterable]] of `String`s.
 */
trait FromStringIterable extends TweetProvider {

  /**
   * @return an Iterable of Strings, where every String is the JSON representation on a [[Tweet]]
   */
  def iterable: Iterable[String]

  final def tweets: TweetObservable = {
    val it = iterable
    Observable.from(it, IOScheduler()).
      map(createTweet).
      flatMap(s => Observable.from(s))
  }
}

/**
 * Consumes tweets of a stream.
 *
 * The user should extend this trait and implement `onTweet`
 * as well as `handleException`
 */
trait TweetConsumer extends TweetObserver {

  final override def onNext(value: Tweet): Unit = onTweet(value)

  final override def onError(error: Throwable): Unit = handleException(error)

  final override def onCompleted(): Unit = ()

  /**
   * Gets called for every tweet that is pulled from the [[TweetProvider]].
   * This method should not block and complete quickly.
   *
   * @param tweet a new [[Tweet]]
   */
  def onTweet(tweet: Tweet): Unit

  /**
   * Gets called when an exception occurred during providing the Tweets.
   * This will be a fatal exception, and tweets will not continue to flow after
   * this exception occurred.
   *
   * @param exception the fatal exception
   */
  def handleException(exception: Throwable): Unit
}

/**
 * Entry point for the app.
 *
 * 1. create an `object`, extending this (`TweetStreaming`)
 * 2. mix-in a proper [[TweetProvider]]
 * 3. provide all desired settings according to the `TweetProvider`
 * 4. implement the methods from [[TweetConsumer]]
 *
 * Example, will scan an Elasticsearch index and print all found tweets.
 *
 * {{{
 * object TweetPrinter extends TweetStreaming with ElasticsearchScanTweets {
 *   def host = "localhost"
 *   def port = 9200
 *   def index = "tweets"
 *
 *   def onTweet(tweet: Tweet) = println(tweet.getText)
 *   def handleException(exception: Throwable) = exception.printStackTrace()
 * }
 * }}}
 *
 * This object's main method can be used to run the app.
 */
abstract class TweetStreaming extends TweetConsumer {
  this: TweetProvider =>

  final def main(args: Array[String]) {
    val ts = tweets
    ts.subscribe(this, ComputationScheduler())
    ts.toBlockingObservable.foreach(_ => ())
  }
}

/**
 * Provides Tweets by streaming from the Twitter API.
 *
 * You should obtain developer keys from Twitter.
 * The steps are roughly
 * 1. create an App at [[https://apps.twitter.com/]]
 * 2. create access tokens for your app as per [[https://dev.twitter.com/docs/auth/tokens-devtwittercom]]
 * 3. follow the steps at [[https://dev.twitter.com/docs/auth/authorizing-request]] to obtain a consumer key/secret pair.
 *
 * This provider will use a sampled stream from the Firehose ([[https://dev.twitter.com/docs/api/1.1/get/statuses/sample Twitter docs]])
 *
 * You could also extend or modify this to use a filtered stream.
 */
trait TwitterApiTweets extends TweetProvider {

  /**
   * @return the OAuth Consumer Key (per authenticated request)
   */
  def OAuthConsumerKey: String

  /**
   * @return the OAuth Consumer Secret (per authenticated request)
   */
  def OAuthConsumerSecret: String

  /**
   * @return the OAuth Access Token (per authorized app)
   */
  def OAuthAccessToken: String

  /**
   * @return the OAuth Access Token Secret (per authorized app)
   */
  def OAuthAccessTokenSecret: String


  private lazy val config = new twitter4j.conf.ConfigurationBuilder().
    setOAuthConsumerKey(OAuthConsumerKey).
    setOAuthConsumerSecret(OAuthConsumerSecret).
    setOAuthAccessToken(OAuthAccessToken).
    setOAuthAccessTokenSecret(OAuthAccessTokenSecret).
    setJSONStoreEnabled(true).
    build

  private def statusListener(o: TweetObserver) = new StatusListener() {
    def onStatus(status: Status) { o.onNext(status) }
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
    def onException(ex: Exception) { o.onError(ex) }
    def onScrubGeo(arg0: Long, arg1: Long) {}
    def onStallWarning(warning: StallWarning) {}
  }

  private def openStream(sl: StatusListener): TwitterStream = {
    val twitterStream = new TwitterStreamFactory(config).getInstance

    sys.addShutdownHook {
      twitterStream.cleanUp()
      twitterStream.shutdown()
    }

    twitterStream.addListener(sl)
    twitterStream
  }

  /**
   * Hooks into the sampled public stream and pushed the Tweets down to the [[TweetConsumer]]
   *
   * @return an `Observable` of `Tweets`
   */
  final def tweets: TweetObservable = {
    val p = Subject[Tweet]()
    val sl = statusListener(p)

    val twitterStream = openStream(sl)
    twitterStream.sample()

    p
  }
}

