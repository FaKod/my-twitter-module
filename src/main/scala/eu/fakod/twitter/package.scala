package eu.fakod

import rx.lang.scala.{Observable, Observer}
import twitter4j.{TwitterObjectFactory, Status}
import scala.collection.immutable.Iterable
import scala.util.Try


package object twitter {
  type Tweet = Status

  type TweetObserver = Observer[Tweet]

  type TweetObservable = Observable[Tweet]

  /**
   * Try to parse a Tweet JSON into an [[scala.collection.immutable.Iterable]].
   *
   * If the json could be parses successfully into a Tweet, the iterable
   * has one element (the Tweet), otherwise it's empty.
   * Exceptions during parsing will be silently swallowed.
   *
   * @param json a string representing a Tweet in JSON.
   * @return an iterable.
   */
  def createTweet(json: String): Iterable[Tweet] =
    tryCreateTweet(json).toOption.toList

  /**
   * Try to parse a Tweet JSON into an [[scala.util.Try]].
   *
   * If the json could be parses successfully into a Tweet, the result will be
   * a [[scala.util.Success]], otherwise it's a [[scala.util.Failure]].
   * Exceptions during parsing will not be thrown, but are available from the `Try`.
   *
   * @param json a string representing a Tweet in JSON
   * @return a `Try` of the parsed JSON
   */
  def tryCreateTweet(json: String): Try[Tweet] =
    Try(TwitterObjectFactory.createStatus(json))

}
