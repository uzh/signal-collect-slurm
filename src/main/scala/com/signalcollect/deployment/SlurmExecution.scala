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

import java.io.File

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * Deploys a job to Slurm using a configuration.
 * First priority have all configuration parameters from the config file that is optionally passed
 * as an argument.
 * Second priority have configuration parameters defined in the optional default configration file
 * named './deployment.config'.
 */
object SlurmExecution extends App {

  deployToSlurm(args)

  def deployToSlurm(arguments: Array[String]) {
    val defaultConfigPath = "./deployment.config"
    val defaultConfig = readConfig(defaultConfigPath)
    val mainConfig = if (arguments.size > 0) {
      readConfig(arguments(0))
    } else {
      None
    }
    val config = (mainConfig, defaultConfig) match {
      case (Some(main), Some(default)) =>
        println(s"Using the configuration ${arguments(0)}, which was passed as an argument."
          + "Using $defaultConfigPath as a fallback for unspecified parameters.")
        main.withFallback(default)
      case (Some(main), None) =>
        println(s"Using the configration ${arguments(0)}, which was passed as an argument.")
        main
      case (None, Some(default)) =>
        println(s"Using the default configuration from '$defaultConfigPath'.")
        default
      case (None, None) =>
        throw new Exception(s"Either the path to a configuration file has to be passed as an argument, " +
          s"or the default configuration file @ '$defaultConfigPath' has to exist.")
    }
    SlurmDeployer.deploy(config)
  }

  def readConfig(path: String): Option[Config] = {
    val configFile = new File(path)
    if (configFile.exists) {
      Some(ConfigFactory.parseFile(configFile))
    } else {
      None
    }
  }

}
