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
import com.signalcollect.nodeprovisioning.slurm.SlurmPriority;
import com.signalcollect.nodeprovisioning.torque.AbstractJobSubmitter

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
    val scriptBase64 = Base64.encodeBase64String(script.getBytes).replace("\n", "").replace("\r", "")
    val qsubCommand = """echo """ + scriptBase64 + """ | base64 -d | sbatch""" //TODO
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
    val script = """ 
#!/bin/bash
#SBATCH --job-name=""" + jobId + """
#SBATCH -N """ + numberOfNodes + """
#SBATCH -n """ + coresPerNode + """
""" + priority + """
#SBATCH -o
#SBATCH --mail-type=ALL
#SBATCH --export=ALL
#SBATCH -o out/""" + jobId + """.out
#SBATCH -e err/""" + jobId + """.err
""" + { if (mailAddress.isDefined) "#SBATCH --mail-user=" + mailAddress.get else "" } + """

jarname=""" + jarname + """
mainClass=""" + mainClass + """
workingDir=""" + workingDir + """
vm_args="""" + jvmParameters + """"

# copy jar
srun --ntasks-per-node=1 cp ~/$jarname $workingDir/

# run test
pbsdsh --ntasks-per-node=1 """ + jdkBinPath + """java $vm_args -cp $workingDir/$jarname $mainClass """ + jobId + """
"""

////TODO: maybe add 
//#create scratch, if it does not exist
//srun --ntasks-per-node=1 mkdir $workingDir/
//
//before copy jar
    script
  }
}
