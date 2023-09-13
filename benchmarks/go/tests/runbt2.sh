#!/bin/bash -l

#SBATCH --nodes=1  --exclusive      ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=32   ## for multithreading in shared memory
#SBATCH --ntasks-per-core=1
## ##SBATCH --num_threads=8
#SBATCH --mem=425G
#SBATCH --time=0-20:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="S2_GOBigT"
#SBATCH -p intel # This is the default partition, you can use any of the following; intel, batch, highmem, gpu

# Print current date
date

# Load samtools
module load samtools

# Concatenate BAMs
samtools cat -h header.sam -o out.bam in1.bam in2.bam

# Print name of node
hostname

ulimit -S -n 1000000

npart=( 2 16 32 24 ); ##4 8 24 16 32 );
output=./outputs/;
partsD=parts_${npart};
testd=~/bigdata/gkaur007/datasets/inputs/${partsD}/;  ##/scratch/gkaur007/inputs/${partsD}/;   ###~/bigdata/gkaur007/datasets/inputs/${partsD}/;
vertices=999999987;
input=~/bigdata/gkaur007/datasets/inputs/bt/; ##btsplt;  ##adj_bigtwitter.graph; ##/scratch/gkaur007/inputs/adj_bigtwitter.graph;   ###~/bigdata/gkaur007/datasets/inputs/adj_bigtwitter.graph;
outpre=testG2BT;
outf=intTBT232;
bsize=( 65000000 ); ## 41250000 27500000 15990000 );  ##27500000;  ##55000000;  
hideg=9000;
mappers=32;
gb=1;
k=100;
iters=1;
flag1=-DUSE_GOMR;
flag2=-DUSE_GRAPHCHI;
mrdata=/rhome/gkaur007/bigdata/gkaur007/;

for batchsize in ${bsize[@]} ;
do
    for nparts in ${npart[@]} ;
    do
	echo -e " 1/2                              " >> ${output}${outf} ; 
	echo -e "\n*************************\n\n" >> ${output}${outf} ; 
	echo " /usr/bin/time -f "%P %M" ./graphchi-go.bin ${input} ${gb} ${mappers} ${nparts} ${batchsize} ${k} ${vertices} ${iters} ${hideg} ${testd}${outpre}${batchsize}${nparts} ${flag1} ${flag2}" >> ${output}${outf};
	ulimit -n 32768;
	echo "\n---------------------------\n\n ">> ${output}${outf}; 
	../go.bin ${input} ${gb} ${mappers} ${nparts} ${batchsize} ${k} ${vertices} ${iters} ${hideg} ${testd}${outpre}${batchsize}${nparts} ${flag1} >> ${output}${outf} 2>&1;
	echo "--------------------------- ">> ${output}${outf};
	echo "Meta Data size " >> ${output}${outf};
                op=`du -s ${mrdata}/mrdata/data/`; 
                siz=`echo ${op} | cut -d' ' -f1`;
                echo "MRDATA ACTUAL DATA SIZE: ${siz} KB" >> ${output}${outf};
                op=`du -s ${mrdata}/mrdata/meta/`;
                siz=`echo ${op} | cut -d' ' -f1`;
                echo "MRDATA ACTUAL META SIZE: ${siz} KB" >> ${output}${outf};
		           
		rm -rf ${mrdata}/mrdata/data;
                rm -rf ${mrdata}/mrdata/meta;
                rm -rf ${mrdata}/combdata/*;
	echo -e "\n*************************\n\n" >> ${output}${outf} ; 
    done;
done;
