#!/bin/bash -l
  
#SBATCH --nodes=1       ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=32   ## for multithreading in shared memory
## ##SBATCH --num_threads=8
#SBATCH --mem=356G
#SBATCH --time=0-08:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="f_convert"
#SBATCH -p intel # This is the default partition, you can use any of the following; intel,

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

./part friendster.graph;

##./etoa friendster.graph edgelist;
##./etoa twitter.graph edgelist;
##./etoa-d ../inputs/orkut.graph edgelist;
##./etoa-d ../inputs/friensdter.graph edgelist;
