/*
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
 */

package com.signalcollect.deployment

import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers

class NodeNameExtractionSpec extends FlatSpec with ShouldMatchers{

  "NodeNameExtractor" should "correctly build the names in a simple example" in {
    val nodeNames = "minion[01,02-03,08-10,13]"
    val correctOutput = List("minion01", "minion02", "minion03",
      "minion08", "minion09", "minion10", "minion13")
    SlurmNodeBootstrap.buildNodeNameList(nodeNames) === correctOutput
  }

}
