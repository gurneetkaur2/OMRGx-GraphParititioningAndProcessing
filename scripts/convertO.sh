#!/bin/bash -l
  
#SBATCH --nodes=1 --exclusive      ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=64   ## for multithreading in shared memory
#SBATCH --mem=425G
#SBATCH --time=0-02:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="Yo_Sconvert"
#SBATCH -p short # This is the default partition, you can use any of the following; intel,

# Print current date
date

# Load samtools
module load samtools

# Concatenate BAMs
samtools cat -h header.sam -o out.bam in1.bam in2.bam

# Print name of node
hostname

ulimit -S -n 1000000
##./partitioner friendster.graph 2 > outFrd;
##./partitioner livejournal.graph 2 > outLj;
##./partitioner twitter.graph 2 > outTw;
##./partitioner orkut.graph 2 
##./partitioner bigtwitter.graph 2 > outBt
./part youtube.graph

##./etoa friendster.graph edgelist;
##./etoa twitter.graph edgelist;
##./etoa ../inputs/orkut.graph edgelist;
##./etoa ../inputs/twitter.graph edgelist;
##./etoa ../inputs/friendster.graph edgelist;
##./etoa ../inputs/ukdomain.graph edgelist;
##./etoa ../inputs/bigtwitter.graph edgelist;
