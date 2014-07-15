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

import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.AkkaConfig
import com.signalcollect.node.DefaultNodeActor

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
import akka.util.Timeout

/**
 * A class that gets serialized and contains the code required to bootstrap
 * an actor system on a Slurm cluster. The code in 'slurmExecutable()'
 * is run on each Slurm cluster node. A node actor is started on all nodes,
 * and the leader additionally bootstraps a Signal/Collect computation
 * defined by the class 'slurmDeployableAlgorithmClassName'.
 */
case class SlurmNodeBootstrap(
  actorNamePrefix: String,
  slurmDeployableAlgorithmClassName: String,
  parameters: Map[String, String],
  numberOfNodes: Int,
  akkaPort: Int,
  workersOnCoordinatorNode: Boolean,
  kryoRegistrations: List[String],
  kryoInitializer: String) {

  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String], kryoInitializer: String) = AkkaConfig.get(
    serializeMessages = false,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInitializer,
    port = akkaPort)

  def ipAndIdToActorRef(ip: String, id: Int, system: ActorSystem, akkaPort: Int): ActorRef = {
    val address = s"""akka.tcp://SignalCollect@$ip:$akkaPort/user/${actorNamePrefix}DefaultNodeActor$id"""
    implicit val timeout = Timeout(30.seconds)
    println(s"Resolving node $id")
    val selection = system.actorSelection(address)
    val actorRef = Await.result(selection.resolveOne, 30 seconds)
    println(s"Done resolving node $id")
    actorRef
  }

  def putZeroPrefix(number: String, targetSize: Int): String = {
    if (targetSize < number.size)
      throw new Error("targetSize of the number with prefix should be greater than or equal to the size of the initial number.")
    val prefix = new StringBuilder("0").*(targetSize - number.size)
    prefix + number
  }

  /**
   * nodeNames should look like minion[01,02-03,05] OR minion01
   */
  def buildNodeNameList(nodeNames: String): List[String] = {
    //TODO: Rename entities and clean up code

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
      nodes
    } else {
      List(nodeNames)
    }
  }

  def slurmExecutable {
    println(s"numberOfNodes = $numberOfNodes, akkaPort = $akkaPort")
    println(s"Starting the actor system and node actor ...")
    val nodeId = System.getenv("SLURM_NODEID").toInt //TODO SLURM_NODEID it says relative id. might need to use SLURM_NODELIST to get same result as PBS_NODENUM
    val system: ActorSystem = ActorSystem("SignalCollect", akkaConfig(akkaPort, kryoRegistrations, kryoInitializer))
    ActorSystemRegistry.register(system)
    //    val leaderExtractor = "\\d+".r
    //    val nodeNames = System.getenv("SLURM_NODELIST")
    //    val leaderId = leaderExtractor.findFirstIn(nodeNames).get.toInt
    //    val nodeId = System.getenv("SLURM_NODEID").toInt
    val isLeader = nodeId == 0
    if (!isLeader || workersOnCoordinatorNode) {
      //      val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
      //      val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
      //        nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)
      val nodeActorId = {
        if (workersOnCoordinatorNode) {
          nodeId
        } else {
          nodeId - 1
        }
      }
      val numberOfWorkerNodes = {
        if (workersOnCoordinatorNode) {
          numberOfNodes
        } else {
          numberOfNodes - 1
        }
      }
      val nodeController = system.actorOf(
        Props(classOf[DefaultNodeActor], actorNamePrefix, nodeActorId, numberOfWorkerNodes, None),
        name = "DefaultNodeActor" + nodeActorId.toString)
      println(s"Node ID = $nodeId")
    }
    if (isLeader) {
      println(s"Node $nodeId is leader.")
      val nodeNames = System.getenv("SLURM_NODELIST")
      val nodeNameList = buildNodeNameList(nodeNames)
      println(s"Leader nodeNameList: $nodeNameList")
      println("Leader is waiting for node actors to start ...")
      Thread.sleep(10000)
      println("Leader is generating the node actor references ...")
      println(s"workersOnCoordinatorNode = $workersOnCoordinatorNode")
      println(s"Nodes = ${nodeNameList.map(InetAddress.getByName(_).getHostAddress).toList}")
      println(s"Coordinator = ${InetAddress.getLocalHost.getHostAddress}")
      val nodeIps = nodeNameList.map(InetAddress.getByName(_).getHostAddress)
      val nodeActors = nodeIps.zipWithIndex.par.flatMap {
        case (ip, i) =>
          if (workersOnCoordinatorNode) {
            Some(ipAndIdToActorRef(ip, i, system, akkaPort))
          } else if (ip != InetAddress.getLocalHost.getHostAddress) {
            Some(ipAndIdToActorRef(ip, i - 1, system, akkaPort))
          } else {
            None
          }
      }.toArray
      println("Leader is passing the nodes and graph builder on to the user code ...")
      val algorithmObject = Class.forName(slurmDeployableAlgorithmClassName).newInstance.asInstanceOf[TorqueDeployableAlgorithm]
      algorithmObject.execute(parameters, nodeActors)
    } else {
      println(s"$nodeId has started its actor system.")
    }
  }
}
