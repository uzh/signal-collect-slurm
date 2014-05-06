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
import com.signalcollect.configuration.AkkaConfig
import com.signalcollect.nodeprovisioning.NodeActorCreator
import com.signalcollect.configuration.ActorSystemRegistry
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Props
import akka.event.Logging
import com.signalcollect.nodeprovisioning.DefaultNodeActor

/**
 * A class that gets serialized and contains the code required to bootstrap
 * an actor system on a Slurm cluster. The code in 'slurmExecutable()'
 * is run on each Slurm cluster node. A node actor is started on all nodes,
 * and the leader additionally bootstraps a Signal/Collect computation
 * defined by the class 'slurmDeployableAlgorithmClassName'.
 */
case class SlurmNodeBootstrap(
  slurmDeployableAlgorithmClassName: String,
  parameters: Map[String, String],
  numberOfNodes: Int,
  akkaPort: Int,
  kryoRegistrations: List[String],
  kryoInitializer: String) {

  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String], kryoInitializer: String) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = false,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInitializer,
    port = akkaPort)

  def ipAndIdToActorRef(ip: String, id: Int, system: ActorSystem, akkaPort: Int): ActorRef = {
    val address = s"""akka://SignalCollect@$ip:$akkaPort/user/DefaultNodeActor$id"""
    val actorRef = system.actorFor(address)
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

    val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
    val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
      nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)
    //val nodesFilePath = System.getenv("PBS_NODEFILE") //TODO here PBS_NODEFILE the name of the file containing the list of nodes assigned to the job
      val nodesFilePath = System.getenv("SLURM_JOB_NODELIST")
      val isLeader1 = nodesFilePath != null
      val isLeader2 = nodeId == 0
    
    val nodeNames = System.getenv("SLURM_NODELIST") //io.Source.fromFile(nodesFilePath).getLines.toList.distinct

    val isLeader = (nodeNames != null) && (!nodeNames.isEmpty) //nodesFilePath != null
    
    //if (isLeader) {
    if (isLeader2) {
      val nodeNameList = buildNodeNameList(nodeNames)
      println(s"I am leader, nodeNameList: $nodeNameList")
      println("Leader is waiting for node actors to start ...")
      Thread.sleep(500)
      println("Leader is generating the node actor references ...")

      val nodeIps = nodeNameList.map(InetAddress.getByName(_).getHostAddress)
      val nodeActors = nodeIps.zipWithIndex.map { case (ip, i) => ipAndIdToActorRef(ip, i, system, akkaPort) }.toArray
      println("Leader is passing the nodes and graph builder on to the user code ...")
      val algorithmObject = Class.forName(slurmDeployableAlgorithmClassName).newInstance.asInstanceOf[TorqueDeployableAlgorithm]
      algorithmObject.execute(parameters, nodeActors)
    }
  }
}
