#!/bin/bash
#SBATCH --partition=physical

# Multithreaded (SMP) job: must run on one node and the cloud partition
#SBATCH --nodes=1

#SBATCH --ntasks=1

# Maximum number of tasks/CPU cores used by the job:
#SBATCH --cpus-per-task=1


# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=0-1:0:00


# The modules to load:
module load Python/3.7.3-spartan_gcc-8.1.0

# The job command(s):
mpirun python CCC-Assignment1.py bigTwitter.json 1node1core
