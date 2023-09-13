#!/bin/bash -l

#SBATCH --ntasks=1 --exclusive     ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=32   ## for multithreading in shared memory
## ##SBATCH --num_threads=8
#SBATCH --mem=425G
#SBATCH --time=0-02:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="RunOmrG"
#SBATCH -p short # This is the default partition, you can use any of the following; intel, batch, highmem, gpu

# Print current date
date

# Load samtools
module load samtools

# Concatenate BAMs
samtools cat -h header.sam -o out.bam in1.bam in2.bam

# Print name of node
hostname

ulimit -S -n 1000000

BMS=(
    pagerank
    ##wcc
);

##GRAPHS=(
    ##u2gohi24_grid
##);

GRAPHS_FILES=(
	adj_uk2007.graph
##	adj_twitter.graph
     ##adj_wiki.graph
##    adj_orkut.graph
    ##adj_bigtwitter.graph
);

outf=intGCUk732;
NITERS=2;
ethreads=32
output=./outputs/;

NSHARDS=( 24 32 ); ## 2 4 8 16 24 32 );
GRIDGRAPH_HOME=~/work/GridGraph
GRAPHCHI_HOME=~/work/graphchi-cpp
DATA_HOME=~/bigdata/gkaur007/metisdata/inputs

for bm in ${BMS[@]}; do
    COUNTER=0;
    for s in ${NSHARDS[@]}; do
       ##   cd "${GRIDGRAPH_HOME}" && pwd && \
		echo " Number of Partitions ${s} of file ${GRAPHS_FILES[$COUNTER]} on 425GB "
##	     SECONDS=0
##            time ./bin/${bm} ${DATA_HOME}/gridgraph/${g} ${NITERS} 8 2>&1 | tee ${DATA_HOME}/gridgraph/gdout/gridgraph.${bm}.${g}.${NITERS}.txt
##	    duration=$SECONDS
##	    echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed." | tee ${DATA_HOME}/gridgraph/gdout/gridgraph.${bm}.${g}.${NITERS}.txt;
        
        cd "${GRAPHCHI_HOME}" && pwd && \
	    SECONDS=0
            srun time bin/example_apps/${bm} file ${DATA_HOME}/${GRAPHS_FILES[$COUNTER]} filetype metis nshards ${s} niters ${NITERS} >> ${output}${outf} 2>&1;
            ##time bin/example_apps/${bm} file ${DATA_HOME}/${GRAPHS_FILES[$COUNTER]} filetype metis execthreads ${ethreads} nshards ${s} niters ${NITERS} 2>&1;
	    ##| tee ${DATA_HOME}/graphchi/gcout/graphchi.${bm}.${GRAPHS_FILES[$COUNTER]}.${NITERS}.txt
	    duration=$SECONDS
	    echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."; 
	    ##| tee ${DATA_HOME}/graphchi/gcout/graphchi.${bm}.${GRAPHS_FILES[$COUNTER]}.${NITERS}.txt;
	rm -rf ${DATA_HOME}/${GRAPHS_FILES[$COUNTER]}.*;
	rm -rf ${DATA_HOME}/${GRAPHS_FILES[$COUNTER]}_deg*;
	##COUNTER=$((COUNTER + 1));
    done;
done;

