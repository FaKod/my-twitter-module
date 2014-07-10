package eu.fakod

import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.http.ServerWebSocket
import org.vertx.scala.core.json.JsonArray
import org.vertx.scala.platform.Verticle


class WebSocketVerticle extends Verticle {

  override def start() = {
    vertx.createHttpServer().websocketHandler({ ws: ServerWebSocket =>
      if (ws.path().equals("/language-topk")) {

        val id = ws.textHandlerID()
        vertx.sharedData.getSet("language-topk").add(id)

        ws.closeHandler({
          vertx.sharedData.getSet("language-topk").remove(id)
        })
      } else {
        ws.reject()
      }
    }).listen(8080, "localhost")

    vertx.eventBus.registerHandler("language-topk", { message: Message[JsonArray] =>
      vertx.sharedData.getSet("language-topk").foreach {
        e: String =>
          vertx.eventBus.publish(e, message.body().toString)
      }
    })
  }

}
