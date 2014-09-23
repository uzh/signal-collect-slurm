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

import com.signalcollect.util.RandomString
import com.typesafe.config.Config
import com.signalcollect.nodeprovisioning.torque.TorqueHost
import com.signalcollect.nodeprovisioning.torque.Job
import scala.collection.JavaConversions._
import com.signalcollect.nodeprovisioning.slurm._

object SlurmDeployer extends App {

  def deploy(config: Config) {
    val serverAddress = config.getString("deployment.torque.server.address")
    val serverUsername = config.getString("deployment.torque.server.username")
    val jobRepetitions = if (config.hasPath("deployment.torque.job.repetitions")) {
      config.getInt("deployment.torque.job.repetitions")
    } else {
      1
    }
    val startSc = if (config.hasPath("deployment.torque.job.start-sc")) {
      config.getBoolean("deployment.torque.job.start-sc")
    } else {
      true
    }
    val infiniband = if (config.hasPath("deployment.torque.job.infiniband")) {
      config.getBoolean("deployment.torque.job.infiniband")
    } else {
      true
    }
    val workersOnCoordinatorNode = if (config.hasPath("deployment.workers-on-coordinator-node")) {
      config.getBoolean("deployment.workers-on-coordinator-node")
    } else {
      true
    }
    val jobNumberOfNodes = config.getInt("deployment.torque.job.number-of-nodes")
    val jobCoresPerNode = config.getInt("deployment.torque.job.cores-per-node")
    val jobMemory = config.getString("deployment.torque.job.memory")
    val jobWalltime = config.getString("deployment.torque.job.walltime")
    val jobWorkingDir = config.getString("deployment.torque.job.working-dir")
    val deploymentJar = config.getString("deployment.jvm.deployed-jar")
    val deploymentJvmPath = config.getString("deployment.jvm.binary-path")
    val deploymentJvmParameters = config.getString("deployment.jvm.parameters")
    val jobSubmitter = new SlurmJobSubmitter(username = serverUsername, hostname = serverAddress)
    val kryoInitializer = if (config.hasPath("deployment.akka.kryo-initializer")) {
      config.getString("deployment.akka.kryo-initializer")
    } else {
      "com.signalcollect.configuration.KryoInit"
    }

    val fixedNumberOfWorkersPerNode = if (config.hasPath("deployment.torque.job.fixed-number-of-workers-per-node")) {
      Some(config.getInt("deployment.torque.job.fixed-number-of-workers-per-node"))
    } else {
      None
    }

    if (config.hasPath("deployment.setup.copy-files")) {
      val copyConfigs = config.getConfigList("deployment.setup.copy-files")
      for (copyConfig <- copyConfigs) {
        val localCopyPath = copyConfig.getString("local-path")
        val remoteCopyPath = copyConfig.getString("remote-path")
        jobSubmitter.copyFileToCluster(localCopyPath, remoteCopyPath)
      }
    }
    val kryoRegistrations = {
      if (config.hasPath("deployment.akka.kryo-registrations")) {
        config.getList("deployment.akka.kryo-registrations").map(_.unwrapped.toString).toList
      } else {
        List.empty[String]
      }
    }
    val deploymentAlgorithm = config.getString("deployment.algorithm.class")
    val parameterMap = config.getConfig("deployment.algorithm.parameters").entrySet.map {
      entry => (entry.getKey, entry.getValue.unwrapped.toString)
    }.toMap
    val priorityString = s"""#SBATCH -t $jobWalltime
    						#SBATCH --mem=$jobMemory"""
    val akkaPort = 2552
    val torque = new SlurmHost(
      jobSubmitter = jobSubmitter,
      coresPerNode = jobCoresPerNode,
      localJarPath = deploymentJar,
      jdkBinPath = deploymentJvmPath,
      jvmParameters = deploymentJvmParameters,
      priority = priorityString,
      workingDir = jobWorkingDir)
    val baseId = s"sc-${RandomString.generate(6)}-"
    val jobIds = (1 to jobRepetitions).map(i => baseId + i)
    val jobs = jobIds.map { id =>
      Job(
        execute = SlurmNodeBootstrap(
          startSc,
          infiniband,
          "",
          deploymentAlgorithm,
          parameterMap,
          jobNumberOfNodes,
          fixedNumberOfWorkersPerNode,
          akkaPort,
          workersOnCoordinatorNode,
          kryoRegistrations,
          kryoInitializer).slurmExecutable _,
        jobId = id,
        numberOfNodes = jobNumberOfNodes)
    }
    println(s"Submitting jobs ${jobs.toList}")
    torque.executeJobs(jobs.toList, copyExecutable = true)
  }
}
