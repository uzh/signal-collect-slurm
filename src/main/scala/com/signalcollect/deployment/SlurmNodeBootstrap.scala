/*
 *  @author Philip Stutz
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
  kryoRegistrations: List[String]) {

  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = false,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,
    kryoRegistrations = kryoRegistrations,
    useJavaSerialization = false,
    port = akkaPort)

  def ipAndIdToActorRef(ip: String, id: Int, system: ActorSystem, akkaPort: Int): ActorRef = {
    val address = s"""akka://SignalCollect@$ip:$akkaPort/user/DefaultNodeActor$id"""
    val actorRef = system.actorFor(address)
    actorRef
  }

  def slurmExecutable {
    println(s"numberOfNodes = $numberOfNodes, akkaPort = $akkaPort")
    println(s"Starting the actor system and node actor ...")
    val nodeId = System.getenv("PBS_NODENUM").toInt //TODO here
    val system: ActorSystem = ActorSystem("SignalCollect", akkaConfig(akkaPort, kryoRegistrations))
    ActorSystemRegistry.register(system)
    val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
    val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
      nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)
    val nodesFilePath = System.getenv("PBS_NODEFILE") //TODO here
    val isLeader = nodesFilePath != null
    if (isLeader) {
      println("Leader is waiting for node actors to start ...")
      Thread.sleep(500)
      println("Leader is generating the node actor references ...")
      val nodeNames = io.Source.fromFile(nodesFilePath).getLines.toList.distinct
      val nodeIps = nodeNames.map(InetAddress.getByName(_).getHostAddress)
      val nodeActors = nodeIps.zipWithIndex.map { case (ip, i) => ipAndIdToActorRef(ip, i, system, akkaPort) }.toArray
      println("Leader is passing the nodes and graph builder on to the user code ...")
      val algorithmObject = Class.forName(slurmDeployableAlgorithmClassName).newInstance.asInstanceOf[SlurmDeployableAlgorithm]
      algorithmObject.execute(parameters, nodeActors)
    }
  }
}
