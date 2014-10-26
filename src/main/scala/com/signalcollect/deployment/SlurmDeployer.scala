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

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet

import com.signalcollect.nodeprovisioning.Job
import com.signalcollect.nodeprovisioning.slurm.SlurmHost
import com.signalcollect.nodeprovisioning.slurm.SlurmJobSubmitter
import com.signalcollect.util.RandomString
import com.typesafe.config.Config

object SlurmDeployer extends App {

  def deploy(config: Config) {
    val serverAddress = config.getString("deployment.server.address")
    val serverUsername = config.getString("deployment.server.username")
    val jobRepetitions = if (config.hasPath("deployment.job.repetitions")) {
      config.getInt("deployment.job.repetitions")
    } else {
      1
    }
    val partition = if (config.hasPath("deployment.job.partition")) {
      Some(config.getString("deployment.job.partition"))
    } else {
      None
    }
    val startSc = if (config.hasPath("deployment.job.start-sc")) {
      config.getBoolean("deployment.job.start-sc")
    } else {
      true
    }
    val infiniband = if (config.hasPath("deployment.job.infiniband")) {
      config.getBoolean("deployment.job.infiniband")
    } else {
      true
    }
    val idleDetectionPropagationDelayInMilliseconds = if (config.hasPath("deployment.job.idle-detection-propagation-delay")) {
      config.getInt("deployment.job.idle-detection-propagation-delay")
    } else {
      1
    }
    if (config.hasPath("deployment.workers-on-coordinator-node")) {
      throw new UnsupportedOperationException("Coordinator on a separate node is not supported for SLURM deployment")
    }
    val jobNumberOfNodes = config.getInt("deployment.job.number-of-nodes")
    val jobCoresPerNode = config.getInt("deployment.job.cores-per-node")
    val copyJar = {
      if (config.hasPath("deployment.job.copy-jar")) {
        config.getBoolean("deployment.job.copy-jar")
      } else {
        true
      }
    }
    val deploymentJar = config.getString("deployment.jvm.deployed-jar")
    val deploymentJvmPath = config.getString("deployment.jvm.binary-path")
    val deploymentJvmParameters = config.getString("deployment.jvm.parameters")
    val jobSubmitter = new SlurmJobSubmitter(username = serverUsername, hostname = serverAddress)
    val kryoInitializer = if (config.hasPath("deployment.akka.kryo-initializer")) {
      config.getString("deployment.akka.kryo-initializer")
    } else {
      "com.signalcollect.configuration.KryoInit"
    }

    val fixedNumberOfWorkersPerNode = if (config.hasPath("deployment.job.fixed-number-of-workers-per-node")) {
      Some(config.getInt("deployment.job.fixed-number-of-workers-per-node"))
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
    val partitionString = if (partition.isDefined) {
      s"""#SBATCH --partition=${partition.get}"""
    } else {
      ""
    }
    val akkaPort = 2552
    val slurm = new SlurmHost(
      jobSubmitter = jobSubmitter,
      coresPerNode = jobCoresPerNode,
      localJarPath = deploymentJar,
      jdkBinPath = deploymentJvmPath,
      jvmParameters = deploymentJvmParameters,
      priority = partitionString)
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
          idleDetectionPropagationDelayInMilliseconds,
          akkaPort,
          kryoRegistrations,
          kryoInitializer).slurmExecutable _,
        jobId = id,
        numberOfNodes = jobNumberOfNodes)
    }
    println(s"Submitting jobs ${jobs.toList}")
    slurm.executeJobs(jobs.toList, copyExecutable = copyJar)
  }
}
