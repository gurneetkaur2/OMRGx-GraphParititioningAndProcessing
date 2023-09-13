#!/bin/bash -l

#SBATCH -p intel
#SBATCH --mem=425GB
#SBATCH -c 64
#SBATCH -N 1
#SBATCH --exclusive

module load centos
#centos.sh 'make clean && make'
centos.sh ./runbt2.sh
