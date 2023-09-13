#!/bin/bash -l
  
#SBATCH --nodes=1      ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=32   ## for multithreading in shared memory
#SBATCH --mem=256G
#SBATCH --time=0-09:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="iConvertU2_newgraph"
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

##./newgraph pokec-mt testCP 24 1632803 >> outtransgraph 2>&1

## adj file is same for all go versions and mtmetis
##./newgraph wiki-mt testCW 24 12150976 >> outtransgraph 2>&1;

##./newgraph orkut-mt testCO 8 3072441 >> outtransgraph 2>&1;
##./newgraph orkut-gohi testCOGohi 24 3072441 >> outtg 2>&1;

##cp ../../datasets/inputs/adj_wiki.graph ../inputs/wk8-mt;
#cp ../../datasets/inputs/adj_wiki.graph ../inputs/wk8-gohi;

##./newgraph wk8-mt testCW 8 12150976 >> outtranswk 2>&1;
##./newgraph wk8-gohi testCWGohi 8 12150976 >> outtranswk 2>&1;
./newgraph u2-mt16 testCU2 16 18520486 >> outtransu2 2>&1;
./newgraph u2-gohi16 testCU2Gohi 16 18520486 >> outtransu2 2>&1;

./newgraph u2-mt24 testCU2 24 18520486 >> outtransu2 2>&1;
./newgraph u2-gohi24 testCU2Gohi 24 18520486 >> outtransu2 2>&1;

##mv ../inputs/parts_8/wk8-* ../inputs/newgraphs/;
##mv ../inputs/wk8-*.* ../inputs/newgraphs/;

##rm ../input/wk8-mt;
##rm ../input/wk8-gohi;


##./newgraph adj_pokec-gohi.graph testPCGohi 24 1632803 >> outtransgraph 2>&1;
