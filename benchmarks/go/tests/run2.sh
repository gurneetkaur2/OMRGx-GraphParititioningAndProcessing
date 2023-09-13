#!/bin/bash -l

#SBATCH -p short
#SBATCH --mem=425GB
#SBATCH -c 64
#SBATCH -N 1
#SBATCH --exclusive

module load centos
#centos.sh 'make clean && make'
centos.sh ./runwk.sh
