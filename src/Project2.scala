import akka.actor._
import scala.math._
import scala.util.Random
import scala.util.control.Breaks
import akka.actor.Actor
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent._
import akka.actor.Scheduler

case class nodeInitialization(adjacency: ArrayBuffer[Int], listOfActors: ArrayBuffer[ActorRef],topology: String)
case class pushsum(s: Double, w: Double)

object Project2
{
  def main(args: Array[String]) {    
    //check the argument length
    if (args.length != 3)
      println("Number of arguments is not correct.Please enter three arguments.");
    else
    {
      //create an actor system
      val system = ActorSystem("SystemOfGossipAndPushSum")
      var numNodes: Int = args(0).toInt
      var topology: String = args(1).toLowerCase()
      var algorithm: String = args(2).toLowerCase()
     
      val mainActor = system.actorOf(Props(new mainActor(numNodes: Int)), name = "mainActor")
      var listOfActors: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]
      var i= 0
      
      if (topology.equals("2d") || topology.equals("imp2d"))
       while(!isPerfectSquare(numNodes))
        numNodes -=1
      for(i<-0 until numNodes)
      {
        listOfActors += system.actorOf(Props(new Node(mainActor: ActorRef, numNodes: Int, algorithm: String)), name = i.toString)        
      }

   //check for the topology that matches the argument provided
      topology match
      {
        case "full" =>
          { 
          fullTopology(numNodes: Int, listOfActors: ArrayBuffer[ActorRef]) 
          }
        case "2d" =>	
          {
          topology2D(numNodes: Int, listOfActors: ArrayBuffer[ActorRef])
          }
        case "line" =>
          { 
          lineTopology(numNodes: Int, listOfActors: ArrayBuffer[ActorRef]) 
          }
        case "imp2d" =>
          {
          imp2Dtopology(numNodes: Int, listOfActors: ArrayBuffer[ActorRef])
          }
      }
      
      mainActor!"start"
      if (algorithm.equals("gossip")) 
        listOfActors(Random.nextInt(numNodes)) ! "Gossip" 
      else if (algorithm.equals("push-sum"))
        listOfActors(Random.nextInt(numNodes)) ! pushsum(0, 0)
      else println("Please provide the correct name for the algorithm.The options are \n1.push-sum 2.gossip.")

      def lineTopology(numNodes: Int, listOfActors: ArrayBuffer[ActorRef]) = 
      {
        var i: Int = 0
        var j: Int = 0
        for (i <- 0 until numNodes)
        {
         var adjacency: ArrayBuffer[Int] = new ArrayBuffer[Int];
          if (i > 0)
            adjacency += (i - 1)
          if (i < numNodes - 1) 
            adjacency += (i + 1)            
            listOfActors(i) ! nodeInitialization(adjacency: ArrayBuffer[Int], listOfActors: ArrayBuffer[ActorRef],topology: String)
        }
      }
      def imp2Dtopology(numNodes: Int, listOfActors: ArrayBuffer[ActorRef]) = {
        var i: Int = 0
        var k: Int = 0 
        for (i <- 0 until numNodes) {
          var adjacency: ArrayBuffer[Int] = new ArrayBuffer[Int]
          if (!hasBottom(i, numNodes)) {
            adjacency += (i + math.sqrt(numNodes).toInt).toInt
          }
          if (!hasTop(i, numNodes)) {
            adjacency += (i - math.sqrt(numNodes).toInt).toInt
          }
          if (!hasLeft(i, numNodes)) {
            adjacency += (i - 1).toInt
          }
          if (!hasRight(i, numNodes)) {
            adjacency += (i + 1).toInt
          }
          var left: ArrayBuffer[Int] = new ArrayBuffer[Int]
          for (k <- 0 until listOfActors.length) {
            left += listOfActors(k).path.name.toInt
          }

          for (k <- 0 until adjacency.length)
          {
            left = left - (adjacency(k))
          }
          left = left - (i)
          adjacency += left(Random.nextInt(left.length))
          listOfActors(i) ! nodeInitialization(adjacency: ArrayBuffer[Int], listOfActors: ArrayBuffer[ActorRef],topology: String)
        }
      }
      def topology2D(numNodes: Int, listOfActors: ArrayBuffer[ActorRef]) = {
        var i: Int = 0
        for (i <- 0 until numNodes)
        {
          var adjacency: ArrayBuffer[Int] = new ArrayBuffer[Int]
          if (!hasBottom(i, numNodes)) {
            adjacency += (i + math.sqrt(numNodes).toInt).toInt
          }
          if (!hasTop(i, numNodes)) {
            adjacency += (i - math.sqrt(numNodes).toInt).toInt
          }
          if (!hasLeft(i, numNodes)) {
            adjacency += (i - 1).toInt
          }
          if (!hasRight(i, numNodes)) {
            adjacency += (i + 1).toInt
          }
          listOfActors(i) ! nodeInitialization(adjacency: ArrayBuffer[Int], listOfActors: ArrayBuffer[ActorRef],topology: String)
        }
      }
      
      def hasTop(i: Int, numNodes: Int): Boolean = {
        if (i.toDouble < math.sqrt(numNodes.toDouble).toDouble) return true
        return false;
      }
      def hasRight(i: Int, numNodes: Int): Boolean = {
        if ((i + 1).toDouble % math.sqrt(numNodes).toDouble == 0) return true
        return false;
      }
      def hasLeft(i: Int, numNodes: Int): Boolean = {
        if (i.toDouble % math.sqrt(numNodes).toDouble == 0) return true
        return false;
      }
      def hasBottom(i: Int, numNodes: Int): Boolean = {
        if (i.toDouble >= numNodes - math.sqrt(numNodes).toDouble)
          return true
        return false;
      }

      //calculating the next perfect square of a given number of nodes if it is not a perfect square in itself
      def isPerfectSquare(number: Int): Boolean = {
         val sqaureRoot = Math.sqrt(number)
          if((sqaureRoot%1)>0)
           return false
          else
           return true
      }
      def fullTopology(numNodes: Int, listOfActors: ArrayBuffer[ActorRef]) =
      {
        var i: Int = 0
        
        for (i <- 0 until numNodes)
        {
          var adjacency: ArrayBuffer[Int] = new ArrayBuffer[Int]
          var j: Int = 0
          for (j <- 0 until numNodes)
          {
            if (i != j)
            {
              adjacency += j;             
            }
           }
          listOfActors(i) ! nodeInitialization(adjacency: ArrayBuffer[Int], listOfActors: ArrayBuffer[ActorRef],topology: String)
        }
      }      
    }
  }
}
class mainActor(numNodes: Int) extends Actor
{
  var b: Long = 0;
  var responseCount: Int = 0;
  def receive = {
    case "start" =>{
      b = System.currentTimeMillis
    }
    case "completed" => {
      responseCount += 1
      if (responseCount == numNodes)
      {
        println("The message has been passed to all the nodes.");
        println("Total time taken is: " + (System.currentTimeMillis - b) + "ms")
        context.system.shutdown()
      }
    }
  }
}
class Node(mainActor: ActorRef, numNodes: Int, algorithm: String) extends Actor {
  import context._
  var sum: Double = 0
  var weight: Double = 1
  var oldValueSW: Double = 0
  var newValueSW: Double = 0
  var terminate: Double = math.pow(10, -10)
  var pushcount: Int = -1
 
  val main: ActorRef = mainActor
  var rumorcount: Int = 0
  var limit = 10
  var neighbors: ArrayBuffer[Int] = new ArrayBuffer[Int]
  var listOfActors: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]
  private var scheduler: Cancellable = _

  override def preStart()= 
  {
    import scala.concurrent.duration._

      scheduler = context.system.scheduler.schedule(Duration.create(1000, TimeUnit.MILLISECONDS),
      Duration.create(50, TimeUnit.MILLISECONDS), self, "trigger")

  }

  override def postStop(): Unit =
  {    
    scheduler.cancel()
  }
  
  def receive = {
    case pushsum(s: Double, w: Double) =>
      {
      oldValueSW = sum / weight
      sum += s;
      weight += w;
      newValueSW = sum / weight
      sum = sum / 2
      weight = weight / 2
      if (math.abs(newValueSW - oldValueSW) <= terminate) 
      {
        pushcount += 1
        if (pushcount < 3)
        {          
          listOfActors(neighbors(Random.nextInt(neighbors.length))) ! pushsum(sum, weight)
        }
        else if (pushcount == 3)
        {
          println(self.path.name + " completedleted execution  " + " pushcount" + newValueSW)
          main!"completed"
          }
      }
          else if(pushcount<3)
          { 
            pushcount = 0
            listOfActors(neighbors(Random.nextInt(neighbors.length))) ! pushsum(sum, weight)
          }
    }
    case nodeInitialization(adjacency: ArrayBuffer[Int], arrayList: ArrayBuffer[ActorRef],topology: String) => 
      {     
      sum = self.path.name.toDouble
      neighbors = adjacency
      var i = 0     
      listOfActors = arrayList
      pushcount = 0
      if(topology.equals("line")) { 
        limit = 20 
      } else {
         limit = 10 
      }
      }      
      case "Gossip" => {
      if (rumorcount < limit)
      {
        rumorcount += 1        
        listOfActors(neighbors(Random.nextInt(neighbors.length))) ! "Gossip"
      } 
      else if (rumorcount == limit)
      {
        println("exit node" + self.path.name + " total message count is:" + rumorcount)
        rumorcount += 1
        main ! "completed"
      }
    }
    case "trigger" =>
      {      
      if (algorithm.equals("push-sum"))
      {
        if(pushcount<3 && pushcount != (-1)) {           
          self ! pushsum(0 ,0)
        }
      }
      else
      {
        if (rumorcount != 0 && rumorcount <= limit) {          
        self ! "Gossip"
      }
      }
    }
  }
}