package eu.fakod

import com.clearspring.analytics.stream.{Counter, StreamSummary}
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}
import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.platform.Verticle
import scala.collection.JavaConversions._


class LanguageCountVerticle extends Verticle {

  val topk = new StreamSummary[String](200)

  override def start() = {
    vertx.eventBus.registerHandler("tweet-feed", { message: Message[JsonObject] =>
      topk.offer(message.body().getField("language"))
    })

    vertx.setPeriodic(2000, { timerID: Long =>
      val js = new JsonArray()
      topk.topK(50).foreach(e => js.add(Json.obj(
        "count" -> e.getCount,
        "item" -> e.getItem
      )))
      vertx.eventBus.publish("language-topk", js)
    })
  }
}
