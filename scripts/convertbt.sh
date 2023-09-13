#!/bin/bash -l
  
#SBATCH --nodes=1 --exclusive     ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=32   ## for multithreading in shared memory
#SBATCH --mem=825G
#SBATCH --time=0-20:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="Cnvt_hBT_newgraph"
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

##./newgraph pokec-mt testCP 24 1632803 >> outtransgraph 2>&1

## adj file is same for all go versions and mtmetis
##./newgraph wiki-mt testCW 24 12150976 >> outtransgraph 2>&1;
##./newgraph wiki-gohi testCWGohi 24 12150976 >> outtransgraph 2>&1;

##./newgraph orkut-mt testCO 8 3072441 >> outtransgraph 2>&1;
##./newgraph orkut-gohi testCOGohi 24 3072441 >> outtg 2>&1;
DATA_HOME=~/bigdata/gkaur007/datasets/inputs/;
CP_TO=../inputs/;

##    adj_livejournal.graph
##    adj_orkut.graph
##    adj_uk2002.graph
outf=outtrans;
np=16;
name1=ok-mt;
name2=bt-go
out1=testCB;
out2=testCBGohi;
vertices=999999987; ##2150976;

#for g in ${GRAPH_FILES[@]};
#do
	##cp -- "${DATA_HOME}${g}" "${CP_TO}"pk-mt${np};
	##cp -- "${DATA_HOME}${g}" "${CP_TO}"pk-gohi${np};
	##cp ../../datasets/inputs/adj_wiki.graph ../inputs/wk8-gohi;


	##echo "./newgraph ${name1}${np} ${out1} ${np} ${vertices}" >> outtranst3 2>&1;
	##./newgraph wk-mt16 testCW 16 12150976 >> outtranst3 2>&1
	##./newgraph ${name1}${np} ${out1} ${np} ${vertices} >> ${outf}${np} 2>&1;

	echo "./newg ${name2}${np} ${out2} ${np} ${vertices}" >> outtranstw 2>&1;
	./newg16 tw-go testCTGohi 16 41652230 hi >> outtranstw 2>&1;
	##./newgraph bt-go testCBGohi 16 999999987  >> outtransbth 2>&1;
	##./newg bt-go testCBGohi 16 999999987 hi >> outtransbth 2>&1;
	##./newg bt-go testCBGohi 24 999999987 hi >> outtransbth 2>&1;
	##./newgraph bt-gohi testCBGohi 24 999999987 >> outtransbt 2>&1;

	##./newgraph ${name2}${np} ${out2} ${np} ${vertices} >> ${outf}${np} 2>&1;
#done;

##./newgraph ok-mt16 testCO 16 3072626 >> outtransok 2>&1;
##./newgraph ok-gohi16 testCOGohi 16 3072626 >> outtransok 2>&1;

##mv ../inputs/parts_8/wk8-* ../inputs/newgraphs/;
##mv ../inputs/wk8-*.* ../inputs/newgraphs/;

##rm ../input/wk8-mt;
##rm ../input/wk8-gohi;


##./newgraph adj_pokec-gohi.graph testPCGohi 24 1632803 >> outtransgraph 2>&1;
