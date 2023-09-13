#!/bin/bash -l
  
#SBATCH --nodes=1 --exclusive      ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=32   ## for multithreading in shared memory
#SBATCH --mem=825G
#SBATCH --time=0-24:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-type=ALL
#SBATCH --job-name="CnvtMe_bt-go_newgraph"
#SBATCH -p highmem # This is the default partition, you can use any of the following; intel,

# Print current date
date

# Load samtools
module load samtools

# Concatenate BAMs
samtools cat -h header.sam -o out.bam in1.bam in2.bam

# Print name of node
hostname

ulimit -S -n 1000000

##./newgraph adj_pokecg.graph testCP 24 1632803 >> outtransgraph 2>&1

## adj file is same for all go versions and mtmetis
##./newgraph wiki-mt testCW 24 12150976 >> outtransgraph 2>&1;
##./newgraph wiki-gohi testCWGohi 24 12150976 >> outtransgraph 2>&1;

##./newgraph orkut-mt testCO 24 3072441 >> outtransgraph2 2>&1;
##./newgraph orkut-gohi testCOGohi 24 3072441 >> outtransgraph2 2>&1;
##./newgraph twitter-gomed.graph testCTGomed 24 41652230 >> outtransgraph2 2>&1;
##./newgraph twitter-gohi.graph testCTGohi 8 41652230 >> outtg 2>&1;
./newg tw-go testCTGomed 8 41652230 med >> outtranstwme 2>&1;
##./newg tw-go testCTGolo 8 41652230 lo >> outtranstwl 2>&1;
##./newg16 bt-go testCBGolo 16 999999987 lo >> outtransbtl 2>&1;
##./newg bt-go testCBGolo 16 999999987 lo >> outtransbtl 2>&1;
##./newg bt-go testCBGolo 8 999999987 lo >> outtransbtl 2>&1;

##./newgraph twitter-gomed.graph testCTGomed 16 41652230 >> outtw 2>&1;
##./newgraph twitter-golo.graph testCTGolo 16 41652230 >> outtw 2>&1;

##./newgraph twitter-gohi.graph testCTGohi 8 41652230 >> outtransgraph2 2>&1;
##./newgraph twitter-gomed.graph testCTGomed 8 41652230 >> outtransgraph2 2>&1;
##./newgraph twitter-golo.graph testCTGolo 8 41652230 >> outtransgraph2 2>&1;


##./newgraph adj_pokec-gohi.graph testPCGohi 24 1632803 >> outtransgraph 2>&1;
