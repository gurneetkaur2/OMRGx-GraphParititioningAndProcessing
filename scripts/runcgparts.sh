#!/bin/bash -l
#SBATCH --nodes=1 --exclusive        ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=32   ## for multithreading in shared memory
## ##SBATCH --num_threads=8
#SBATCH --mem=256G
#SBATCH --time=0-02:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="runcgparts"
#SBATCH -p short # This is the default partition, you can use any of the following; intel,

# Print current date
date

# Load samtools
##module load samtools

# Concatenate BAMs
##samtools cat -h header.sam -o out.bam in1.bam in2.bam

# Print name of node
hostname

parts="mod"; ##"hi" "med" "lo" mod=go-75 );
inputs=~/bigdata/gkaur007/datasets/inputs/;
filename=( O ); ##"LJ" "OK" "U2" "WK" "TW" "TM" "U7" );
outpre=./cgparts/;
size=( 8 16 24 );
partsD=parts;
pre=parts;
out=out;

for filename in ${filename} ;
do
	##        partsD=parts${fname}/;
	for p in ${size[@]} ;
	do
		#echo "\n./cgpart ${inputs}${partsD}${pre}${parts}${p} 8 \n ">> ${outpre}${out}${filename}${parts}${p} 2>&1 ;
		#./cgpart ${inputs}${partsD}${pre}${parts}${p} 8 >> ${outpre}${out}${filename}${parts}${p} ;
		#echo "\n./cgpart ${inputs}${partsD}${pre}${parts}16 16 \n" >> ${outpre}${out}${filename}${parts}${p} 2>&1 ;
		#./cgpart ${inputs}${partsD}${pre}${parts}16 16 >> ${outpre}${out}${filename}${parts}${p} ;
		echo "\n./cgpart ${inputs}${partsD}${filename}/${pre}${parts}${p} ${p} \n" >> ${outpre}${out}${filename}${parts}${p} 2>&1 ;
		./cgpart ${inputs}${partsD}${filename}/${pre}${parts}${p} ${p} >> ${outpre}${out}${filename}${parts}${p} ;

##	./cgpart ./inputs/${fname}/parts${parts}16 16 >> outFL${parts}16 ;
##	./cgpart ./inputs/${fname}/parts${parts}24 24 >> outFL${parts}24 ;
	done
done





##./cgpart ./inputs/partsFL/partslo8 8 >> outFLlo8 &
