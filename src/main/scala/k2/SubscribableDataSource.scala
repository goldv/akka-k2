package k2

import akka.actor.{Terminated, ActorRef, Actor}
import akka.event.Logging
import akka.util.Subclassification
import k2.util.SubclassifiedIndex
import k2.SubscribableDataSource._

class SubscribableDataSource(_parent: Option[ActorRef]) extends Actor{

  lazy val log = Logging.getLogger(context.system, this)

  /**
   * The logic to form sub-class hierarchy
   */
  implicit val subclassification: Subclassification[String] = new StartsWithSubclassification

  val parent = _parent.getOrElse(context.parent)

  // must be lazy to avoid initialization order problem with subclassification
  private lazy val subscriptions = new SubclassifiedIndex[String, ActorRef]()

  private var cache = Map.empty[String, Set[ActorRef]]

  def receive: Receive = {
    case s:Subscribe =>
      val subscriber = s.subscriber.getOrElse(sender)
      subscriber ! handelSubscription(subscriber, s)

    case u:UnSubscribe =>
      val subscriber = u.subscriber.getOrElse(sender)
      subscriber ! handleUnsubscribe(subscriber, u)

    case p:PublishableEvent => publish(p)

    case Terminated(service) => handleServiceTerminated(service)
  }

  def publish(p:PublishableEvent): Unit = {
    val recv =
        if (cache contains p.topic) cache(p.topic)
        else {
          addToCache(subscriptions.addKey(p.topic))
          cache(p.topic)
        }

    recv foreach (_ ! p.payload)
  }

  def handleServiceTerminated(service: ActorRef) = {
    val diff = subscriptions.removeValue(service)

    removeFromCache(diff)

    // send count to the parent
    diff.map(_._1).foreach( key => parent ! SubscriptionCount(key, cache(key).size) )
  }

  def handleUnsubscribe(subscriber: ActorRef, unsub: UnSubscribe) = {
    log.info(s"unsub: ${unsub.topic}")
    val diff = subscriptions.removeValue(unsub.topic, subscriber)
    // removeValue(K, V) does not return the diff to remove from or add to the cache
    // but instead the whole set of keys and values that should be updated in the cache
    cache ++= diff

    // send the counts to the parent
    diff.map(subscriptionCount).foreach( parent ! _)

    if(diff.nonEmpty){
      context.unwatch(subscriber)
      UnSubscribeAck(unsub.topic)
    }
    else{
      UnSubscribeNack(unsub.topic, "not subscribed")
    }
  }

  def handelSubscription(subscriber: ActorRef, sub: Subscribe) = {
    log.info(s"sub: ${sub.topic}")
    val diff = subscriptions.addValue(sub.topic, subscriber)
    addToCache(diff)

    // send count to the parent
    diff.map(_._1).foreach( key => parent ! SubscriptionCount(key, cache(key).size) )

    if(diff.nonEmpty){
      context.watch(subscriber)
      SubscribeAck(sub.topic)
    }
    else{
      SubscribeNack(sub.topic, "already subscribed")
    }
  }

  private def subscriptionCount(changes: (String, Set[ActorRef])) = SubscriptionCount(changes._1, changes._2.size)

  private def removeFromCache(changes: Seq[(String, Set[ActorRef])]): Unit =
    cache = (cache /: changes) {
      case (m, (c, cs)) ⇒ m.updated(c, m.getOrElse(c, Set.empty[ActorRef]) -- cs)
  }

  private def addToCache(changes: Seq[(String, Set[ActorRef])]): Unit =
    cache = (cache /: changes) {
      case (m, (c, cs)) ⇒ m.updated(c, m.getOrElse(c, Set.empty[ActorRef]) ++ cs)
  }
}

object SubscribableDataSource{
  case class Subscribe(topic: String, subscriber: Option[ActorRef] = None)
  case class SubscribeAck(topic: String)
  case class SubscribeNack(topic: String, reason: String)

  case class UnSubscribe(topic: String, subscriber: Option[ActorRef] = None)
  case class UnSubscribeAck(topic: String)
  case class UnSubscribeNack(topic: String, reason: String)

  case class SubscriptionCount(topic: String, count: Int)

  case class PublishableEvent(topic: String, payload: Any)
}

class StartsWithSubclassification extends Subclassification[String] {
  override def isEqual(x: String, y: String) = x == y
  override def isSubclass(x: String, y: String) = x.startsWith(y)
}

