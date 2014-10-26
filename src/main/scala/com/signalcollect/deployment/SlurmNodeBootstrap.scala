/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *
 *  Copyright 2014 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.signalcollect.deployment

import java.net.InetAddress

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global

import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.AkkaConfig
import com.signalcollect.node.DefaultNodeActor

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
import akka.util.Timeout

object SlurmNodeBootstrap {

  def putZeroPrefix(number: String, targetSize: Int): String = {
    if (targetSize < number.size)
      throw new Error("targetSize of the number with prefix should be greater than or equal to the size of the initial number.")
    val prefix = new StringBuilder("0").*(targetSize - number.size)
    prefix + number
  }

  /**
   * nodeNames should look like minion[01,02-03,05] OR minion01
   * @return list of minion names, resolvable with DNS, like: List("minion01", "minion02")
   */
  def buildNodeNameList(nodeNames: String): List[String] = {
    println("Minions: " + nodeNames)
    if (nodeNames.contains("[")) {
      var nodes: List[String] = List.empty
      val name = nodeNames.split("\\[")(0)
      val intermediateNodeList = nodeNames.split("\\[")(1).split("\\]")(0).split(",")
      for (betweenCommas <- intermediateNodeList) {
        if (betweenCommas.contains("-")) {
          val sizeOfNumber = betweenCommas.split("-")(1).size
          val first = betweenCommas.split("-")(0).replaceFirst("^0+(?!$)", "").toInt
          val last = betweenCommas.split("-")(1).replaceFirst("^0+(?!$)", "").toInt
          val sublist = (first to last).map(x => name + putZeroPrefix(x.toString, sizeOfNumber)).toList
          nodes = sublist ::: nodes
        } else {
          nodes = (name + betweenCommas) :: nodes
        }
      }
      nodes = nodes.sorted
      println("Resolved to: " + nodes.mkString(","))
      nodes
    } else {
      List(nodeNames).sorted
    }
  }
}

/**
 * A class that gets serialized and contains the code required to bootstrap
 * an actor system on a Slurm cluster. The code in 'slurmExecutable()'
 * is run on each Slurm cluster node. A node actor is started on all nodes,
 * and the leader additionally bootstraps a Signal/Collect computation
 * defined by the class 'slurmDeployableAlgorithmClassName'.
 */
case class SlurmNodeBootstrap[Id, Signal](
  startSc: Boolean,
  infiniband: Boolean,
  actorNamePrefix: String,
  slurmDeployableAlgorithmClassName: String,
  parameters: Map[String, String],
  numberOfNodes: Int,
  fixedNumberOfWorkersPerNode: Option[Int],
  idleDetectionPropagationDelayInMilliseconds: Int,
  akkaPort: Int,
  kryoRegistrations: List[String],
  kryoInitializer: String) {

  def akkaConfig(akkaHostname: String, akkaPort: Int, kryoRegistrations: List[String], kryoInitializer: String) = AkkaConfig.get(
    serializeMessages = false,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInitializer,
    hostname = akkaHostname,
    port = akkaPort)

  def ipAndIdToActorRef(ip: String, id: Int, system: ActorSystem, akkaPort: Int): ActorRef = {
    val translatedIp = normalIpToInfinibandIp(ip)
    val address = s"""akka.tcp://SignalCollect@$translatedIp:$akkaPort/user/${actorNamePrefix}DefaultNodeActor$id"""
    implicit val timeout = Timeout(30.seconds)
    println(s"Resolving node $id")
    var attempts = 0
    def getSelectionFuture = {
      attempts += 1
      system.actorSelection(address).resolveOne
    }
    var future = getSelectionFuture
    var result: Option[ActorRef] = None
    val maxAttempts = 20
    while (result.isEmpty && attempts < maxAttempts) {
      try {
        result = Some(Await.result(getSelectionFuture, 30 seconds))
      } catch {
        case t: Throwable =>
          println(s"Failed during actor resolving: ${t.getMessage}, attempts so far: $attempts")
          Thread.sleep(2000)
      }
    }
    result match {
      case None => throw new Exception(s"Leader could not resolve address $address after $attempts attempts.")
      case Some(r) =>
        println(s"Leader successfully resolved address $address after $attempts attempts.")
        r
    }
  }

  def normalIpToInfinibandIp(ip: String): String = {
    if (infiniband) {
      try {
        val parsed = ip.split("\\.").map(_.toInt)
        if (parsed(0) == 130 && parsed(1) == 60 && parsed(2) == 75) {
          println("Switched address to Infiniband address.")
          s"192.168.32.${parsed(3)}"
        } else if (parsed(0) == 192 && parsed(1) == 168 && parsed(2) == 32) {
          println("Already using Infiniband!")
          ip
        } else {
          println(s"Got address $ip, no Infiniband translation available.")
          ip
        }
      } catch {
        case t: Throwable =>
          println(s"Error during Infiniband IP mapping: ${t.getMessage}")
          t.printStackTrace
          ip
      }
    } else {
      println("Infiniband disabled.")
      ip
    }
  }

  def slurmExecutable {
    val nodeRelativeId = System.getenv("SLURM_NODEID").toInt //TODO SLURM_NODEID it says relative id. might need to use SLURM_NODELIST to get same result as PBS_NODENUM
    val isLeader = nodeRelativeId == 0
    val nodeNames = System.getenv("SLURM_NODELIST")
    val nodeNameList = SlurmNodeBootstrap.buildNodeNameList(nodeNames)
    val currentNodeName = nodeNameList(nodeRelativeId)
    println("Node: " + currentNodeName + " with rel Id " + nodeRelativeId + " isLeader = " + isLeader)
    if (startSc) {
      println(s"numberOfNodes = $numberOfNodes, akkaPort = $akkaPort")
      println(s"Starting the actor system and node actor ...")
      val akkaHostname = normalIpToInfinibandIp(InetAddress.getLocalHost.getHostAddress)
      println(s"akkaHostname: $akkaHostname")
      val system: ActorSystem = ActorSystem("SignalCollect", akkaConfig(akkaHostname, akkaPort, kryoRegistrations, kryoInitializer))
      println(s"$akkaHostname : actor system has been started.")
      ActorSystemRegistry.register(system)
      println(s"$akkaHostname : isLeader: $isLeader")
      println("Node" + currentNodeName + " with relId: " + nodeRelativeId + " is not leader or workers on coordinator node")
      val nodeActorId = nodeRelativeId
      val numberOfWorkerNodes = numberOfNodes
      val nodeController = system.actorOf(
        Props(classOf[DefaultNodeActor[Id, Signal]],
          actorNamePrefix,
          nodeActorId,
          numberOfWorkerNodes,
          fixedNumberOfWorkersPerNode,
          idleDetectionPropagationDelayInMilliseconds,
          None),
        name = "DefaultNodeActor" + nodeActorId.toString)
      println(s"Node ID = $nodeRelativeId")
      println(s"$nodeRelativeId has started its actor system.")
      if (isLeader) {
        println(s"Node $nodeRelativeId is leader.")
        /**
         * To avoid some weird Akka error:
         * akka.remote.RemoteTransportException: Startup timed out
         * at akka.remote.Remoting.akka$remote$Remoting$$notifyError(Remoting.scala:136)
         * at akka.remote.Remoting.start(Remoting.scala:198)
         * at akka.remote.RemoteActorRefProvider.init(RemoteActorRefProvider.scala:184)
         * at akka.actor.ActorSystemImpl.liftedTree2$1(ActorSystem.scala:618)
         * ...
         */
        Thread.sleep(10000)

        println(s"Leader nodeNameList: $nodeNameList")
        println("Leader is waiting for node actors to start ...")
        println("Leader is generating the node actor references ...")
        println(s"Nodes = ${nodeNameList.map(InetAddress.getByName(_).getHostAddress)}")
        println(s"Coordinator = ${InetAddress.getLocalHost.getHostAddress}")
        val nodeIps = nodeNameList.map(InetAddress.getByName(_).getHostAddress)
        val nodeActors = nodeIps.zipWithIndex.par.map {
          case (ip, i) => ipAndIdToActorRef(ip, i, system, akkaPort)
        }.toArray
        println("Leader is passing the nodes and graph builder on to the user code ...")
        val algorithmObject = Class.forName(slurmDeployableAlgorithmClassName).newInstance.asInstanceOf[DeployableAlgorithm]
        algorithmObject.execute(parameters, nodeActors)
      }
    } else { //!startSc
      if (isLeader) {
        val algorithmObject = Class.forName(slurmDeployableAlgorithmClassName).newInstance.asInstanceOf[DeployableAlgorithm]
        algorithmObject.execute(parameters, Array())
      }
    }
  }
}
