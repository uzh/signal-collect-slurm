/*
 *  @author Philip Stutz
 *  @author Daniel Strebel
 *
 *  Copyright 2012 University of Zurich
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

package com.signalcollect.nodeprovisioning.slurm

import org.apache.commons.codec.binary.Base64
import java.io.File
import java.io.FileOutputStream
import java.io.FileWriter

abstract class AbstractSlurmJobSubmitter extends Serializable {

  def runOnClusterNodes(
    jobId: String,
    numberOfNodes: Int,
    coresPerNode: Int,
    jarname: String,
    mainClass: String,
    priority: String = SlurmPriority.superfast,
    jvmParameters: String,
    jdkBinPath: String,
    workingDir: String,
    mailAddress: Option[String] = None): String = {
    val script = getShellScript(jobId, numberOfNodes, coresPerNode, jarname, mainClass, priority, jvmParameters, jdkBinPath, workingDir, mailAddress)
    //println("The batchscript")
    //println(script)
    val qsubCommand = """sbatch """ + jobId + ".sh" //TODO // | base64 -d
    executeCommandOnClusterManager(qsubCommand)
  }

  def executeCommandOnClusterManager(command: String): String

  def copyFileToCluster(localPath: String, targetPath: String = "")

  def getShellScript(
    jobId: String,
    numberOfNodes: Int,
    coresPerNode: Int,
    jarname: String,
    mainClass: String,
    priority: String,
    jvmParameters: String,
    jdkBinPath: String,
    workingDir: String,
    mailAddress: Option[String]): String = {

    println("Copying submission script")

    val script = """#!/bin/bash
#SBATCH --job-name=""" + jobId + """
#SBATCH -N """ + numberOfNodes + """
#SBATCH -n """ + numberOfNodes + """
#SBATCH -c """ + coresPerNode + """

#SBATCH --exclusive
""" + priority + """
#SBATCH -o
#SBATCH --mail-type=ALL
#SBATCH --export=ALL
#SBATCH -o out/""" + jobId + """.out
#SBATCH -e err/""" + jobId + """.err
""" + { if (mailAddress.isDefined) "#SBATCH --mail-user=" + mailAddress.get else "" } + s"""

# copy jar
srun --ntasks-per-node=1 cp ~/$jarname $workingDir/

# run test
srun --ntasks-per-node=1 """ + jdkBinPath + s"""java $jvmParameters -cp $workingDir/$jarname $mainClass """ + jobId

    val fileSeparator = System.getProperty("file.separator")

    val folder = new File("." + fileSeparator + "config-tmp")
    if (!folder.exists) {
      folder.mkdir
    }
    val scriptPath = "." + fileSeparator + jobId + ".sh"
    val out = new FileWriter(scriptPath)
    out.write(script)
    out.close
    copyFileToCluster(scriptPath)

    println("Jarname is: " + jarname)
    println("Script in abstract is")
    println(script)
    ////TODO: maybe add 
    //#create scratch, if it does not exist
    //srun --ntasks-per-node=1 mkdir $workingDir/
    //
    //before copy jar
    script
  }
}
