package com.signalcollect.nodeprovisioning.slurm

/**
 * Determines the priority in Slurm's scheduling queue
 */
object SlurmPriority {
  val superfast = """#SBATCH -t 0-00:59:59"""+
"""#SBATCH --mem=50"""
  val fast = """#SBATCH -t 0-11:59:59"""+
"""#SBATCH --mem=50"""
  val slow = """#SBATCH -t 8-08:59:59"""+
"""#SBATCH --mem=50"""
}