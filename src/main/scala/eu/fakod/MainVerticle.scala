package eu.fakod

import org.vertx.scala.platform.Verticle


class MainVerticle extends Verticle {

  override def start() = {
    val appConfig = container.config()
    val twitterVerticleConfig = appConfig.getObject("TwitterVerticle")

    container.deployVerticle("scala:eu.fakod.TwitterVerticle", twitterVerticleConfig)
    container.deployVerticle("scala:eu.fakod.LanguageCountVerticle")
    container.deployVerticle("scala:eu.fakod.WebSocketVerticle")
  }
}
