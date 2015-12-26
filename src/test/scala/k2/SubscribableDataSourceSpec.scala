package k2

import java.util.concurrent.Executors

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import k2.SubscribableDataSource._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext

class SubscribableDataSourceSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this( ActorSystem.apply("ReceptionistSpec", None, None, Some(ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor() )) ) )

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "a subscribable data source" must{
    "successfully subscribe for a new topic" in{
      val service = TestProbe()
      val client = system.actorOf(Props(new SubscribableDataSource(Some(service.ref))))

      client ! Subscribe("marketdata")
      expectMsg(SubscribeAck("marketdata"))
    }
    "receive nack for duplicate subscription" in{
      val service = TestProbe()
      val client = system.actorOf(Props(new SubscribableDataSource(Some(service.ref))))

      client ! Subscribe("marketdata")
      expectMsg(SubscribeAck("marketdata"))

      client ! Subscribe("marketdata")
      expectMsg(SubscribeNack("marketdata", "already subscribed"))
    }
    "allow sub topic subscriptions" in{
      val service = TestProbe()
      val client = system.actorOf(Props(new SubscribableDataSource(Some(service.ref))))

      client ! Subscribe("marketdata")
      expectMsg(SubscribeAck("marketdata"))

      client ! Subscribe("marketdata/eurusd")
      expectMsg(SubscribeAck("marketdata/eurusd"))
    }
    "receive nack for duplicate sub topic subscription" in{
      val service = TestProbe()
      val client = system.actorOf(Props(new SubscribableDataSource(Some(service.ref))))

      client ! Subscribe("marketdata")
      expectMsg(SubscribeAck("marketdata"))

      client ! Subscribe("marketdata/eurusd")
      expectMsg(SubscribeAck("marketdata/eurusd"))

      client ! Subscribe("marketdata/eurusd")
      expectMsg(SubscribeNack("marketdata/eurusd", "already subscribed"))
    }
    "receive subscription count for new topic" in{
      val service = TestProbe()
      val client = system.actorOf(Props(new SubscribableDataSource(Some(service.ref))))

      client ! Subscribe("marketdata")
      expectMsg(SubscribeAck("marketdata"))

      service.expectMsg(SubscriptionCount("marketdata", 1))
    }
    "receive decremented subscription count for un-subscribed topic" in{
      val service = TestProbe()
      val client = system.actorOf(Props(new SubscribableDataSource(Some(service.ref))))

      client ! Subscribe("marketdata")
      expectMsg(SubscribeAck("marketdata"))

      service.expectMsg(SubscriptionCount("marketdata", 1))

      client ! UnSubscribe("marketdata")
      expectMsg(UnSubscribeAck("marketdata"))

      service.expectMsg(SubscriptionCount("marketdata", 0))
    }
    "handle subscriptions from multiple subscribers" in{
      val service = TestProbe()
      val subscriber1 = TestProbe()
      val subscriber2 = TestProbe()
      val client = system.actorOf(Props(new SubscribableDataSource(Some(service.ref))))

      subscriber1.send(client, Subscribe("marketdata"))
      subscriber1.expectMsg(SubscribeAck("marketdata"))
      service.expectMsg(SubscriptionCount("marketdata", 1))

      subscriber2.send(client, Subscribe("marketdata"))
      subscriber2.expectMsg(SubscribeAck("marketdata"))
      service.expectMsg(SubscriptionCount("marketdata", 2))
    }
    "handle termination of a subscriber" in{
      val service = TestProbe()
      val subscriber1 = TestProbe()
      val subscriber2 = TestProbe()
      val client = system.actorOf(Props(new SubscribableDataSource(Some(service.ref))))

      subscriber1.send(client, Subscribe("marketdata"))
      subscriber1.expectMsg(SubscribeAck("marketdata"))
      service.expectMsg(SubscriptionCount("marketdata", 1))

      subscriber2.send(client, Subscribe("marketdata"))
      subscriber2.expectMsg(SubscribeAck("marketdata"))
      service.expectMsg(SubscriptionCount("marketdata", 2))

      subscriber2.send(client, Subscribe("orders"))
      subscriber2.expectMsg(SubscribeAck("orders"))
      service.expectMsg(SubscriptionCount("orders", 1))

      subscriber2.ref ! PoisonPill

      service.expectMsgAllOf(SubscriptionCount("orders", 0), SubscriptionCount("marketdata", 1))
    }

  }

}
