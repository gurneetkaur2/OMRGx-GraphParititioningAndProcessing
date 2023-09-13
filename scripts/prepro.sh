#!/bin/bash -l
  
#SBATCH --nodes=1    --exclusive  ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=32   ## for multithreading in shared memory
#SBATCH --mem=125G
#SBATCH --time=0-02:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="Gridgraph_preprocess"
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

GRID_HOME=~/bigdata/gkaur007/gridgraph/;
part1=2;
part2=4;
part3=8;
part4=16;
part5=24;
part6=32;
nvertices=12150977; ##3072627; ##41652231;  ##3072627; ##18520487;    ## wk 12150977;     ##ok 3072627;      ## bt 999999988;
nvertices2=12150977; ##3072627; ##41652231;  ##3072627; ##18520487;    ## wk 12150977;     ##ok 3072627;      ## bt 999999988;
mt=u2mt;
con=wk-gohi;
go=wk;
conmt=u2-mt
conGo=wk-cyccyc;
conGomo=tw-cyclcyc;
conGom=wk-gomed;
conGol=wk-golo;
##go=wkcyc;
gomo=oklcyc8;
gom=wkgomed;
gol=wkgolo;
##DATA_HOME=~/bigdata/gkaur007/metisdata/inputs/parts_${part}/;
DATA_HOME=~/bigdata/gkaur007/appdata/;

##for part in ${nparts[@]}; 
##do
##	DATA_HOME=$((${HOME}${part}/));
        ##cat /etc/*-release
	##./convert2bin ${DATA_HOME}/metisdata/inputs/parts_${part}/ok-mt${part}.newGraph ${DATA_HOME}/gridgraph/okmt${part}.bin;
	##./convert2bin ${DATA_HOME}/metisdata/inputs/parts_${part}/ok-gohi${part}.newGraph ${DATA_HOME}/gridgraph/okgohi${part}.bin;
##1	echo "./convert2bin ${DATA_HOME}${conmt}${part}.newGraph ${GRID_HOME}${mt}${part}.bin";
##2	./convert2bin ${DATA_HOME}${conmt}${part}.newGraph ${GRID_HOME}${mt}${part}.bin;

##	echo "./convert2bin ${DATA_HOME}${conGo}${part}.newGraph ${GRID_HOME}${go}${part}.bin";
##	./convert2bin ${DATA_HOME}${conGo}${part}.newGraph ${GRID_HOME}${go}${part}.bin;
##	./convert2bin ${DATA_HOME}${conGo}${part2}.newGraph ${GRID_HOME}${go}${part2}.bin;
##	./convert2bin ${DATA_HOME}${conGo}${part3}.newGraph ${GRID_HOME}${go}${part3}.bin;

	echo "./convert2bin ${DATA_HOME}${con}${part3}.newGraph ${GRID_HOME}${go}.bin";
	./convert2bin ${DATA_HOME}${con}${part3}.newGraph ${GRID_HOME}${go}.bin;
	##./convert2bin ${DATA_HOME}${conGomo}${part2}.newGraph ${GRID_HOME}${gomo}${part2}.bin;
##	./convert2bin ${DATA_HOME}${conGomo}${part3}.newGraph ${GRID_HOME}${gomo}${part3}.bin;
	
##	./convert2bin ${DATA_HOME}${conGom}${part}.newGraph ${GRID_HOME}${gom}${part}.bin;
##	./convert2bin ${DATA_HOME}${conGom}${part2}.newGraph ${GRID_HOME}${gom}${part2}.bin;
##	./convert2bin ${DATA_HOME}${conGom}${part3}.newGraph ${GRID_HOME}${gom}${part3}.bin;

##	echo "./convert2bin ${DATA_HOME}${conGol}${part}.newGraph ${GRID_HOME}${gol}${part}.bin";
##	./convert2bin ${DATA_HOME}${conGol}${part}.newGraph ${GRID_HOME}${gol}${part}.bin;
##	./convert2bin ${DATA_HOME}${conGol}${part2}.newGraph ${GRID_HOME}${gol}${part2}.bin;
##	./convert2bin ${DATA_HOME}${conGol}${part3}.newGraph ${GRID_HOME}${gol}${part3}.bin;

##	./convert2bin ~/bigdata/gkaur007/appdata/tw-golo16.newGraph ~/bigdata/gkaur007/gridgraph/twgolo16.bin;
##	./bin/preprocess -i ${GRID_HOME}twgolo16.bin -o ${GRID_HOME}twgolo16_grid/ -v 41652232 -p 4 -t 0;
##	 time -p ./bin/pagerank ~/bigdata/gkaur007/gridgraph/wkgohi8_grid 4;
	 ##time -p ./bin/wcc ~/bigdata/gkaur007/gridgraph/twgohi8_grid 8;

##5	echo "./bin/preprocess -i ${GRID_HOME}${mt}${part}.bin -o ${GRID_HOME}${mt}${part}_grid/ -v ${nvertices} -p 4 -t 0";
##6	./bin/preprocess -i ${GRID_HOME}${mt}${part}.bin -o ${GRID_HOME}${mt}${part}_grid/ -v ${nvertices} -p 4 -t 0;

#	echo "./bin/preprocess -i ${GRID_HOME}${go}${part}.bin -o ${GRID_HOME}${go}${part}_grid/ -v ${nvertices} -p 4 -t 0";
#	./bin/preprocess -i ${GRID_HOME}${go}${part}.bin -o ${GRID_HOME}${go}${part}_grid/ -v ${nvertices} -p 4 -t 0;
#	./bin/preprocess -i ${GRID_HOME}${go}${part2}.bin -o ${GRID_HOME}${go}${part2}_grid/ -v ${nvertices} -p 4 -t 0;
#	./bin/preprocess -i ${GRID_HOME}${go}${part3}.bin -o ${GRID_HOME}${go}${part3}_grid/ -v ${nvertices} -p 4 -t 0;

	echo "./bin/preprocess -i ${GRID_HOME}${go}.bin -o ${GRID_HOME}${go}_grid/ -v ${nvertices2} -p 4 -t 0";
	./bin/preprocess -i ${GRID_HOME}${go}.bin -o ${GRID_HOME}${go}${part1}_grid/ -v ${nvertices} -p 2 -t 0;
	./bin/preprocess -i ${GRID_HOME}${go}.bin -o ${GRID_HOME}${go}${part2}_grid/ -v ${nvertices} -p 4 -t 0;
	./bin/preprocess -i ${GRID_HOME}${go}.bin -o ${GRID_HOME}${go}${part3}_grid/ -v ${nvertices} -p 8 -t 0;
	./bin/preprocess -i ${GRID_HOME}${go}.bin -o ${GRID_HOME}${go}${part4}_grid/ -v ${nvertices} -p 16 -t 0;
	./bin/preprocess -i ${GRID_HOME}${go}.bin -o ${GRID_HOME}${go}${part5}_grid/ -v ${nvertices} -p 24 -t 0;
	./bin/preprocess -i ${GRID_HOME}${go}.bin -o ${GRID_HOME}${go}${part6}_grid/ -v ${nvertices} -p 32 -t 0;
	##./bin/preprocess -i ${GRID_HOME}${gomo}${part2}.bin -o ${GRID_HOME}${gomo}${part2}_grid/ -v ${nvertices2} -p 4 -t 0;
	##./bin/preprocess -i ${GRID_HOME}${gomo}${part3}.bin -o ${GRID_HOME}${gomo}${part3}_grid/ -v ${nvertices2} -p 4 -t 0;

##	./bin/preprocess -i ${GRID_HOME}${gom}${part}.bin -o ${GRID_HOME}${gom}${part}_grid/ -v ${nvertices} -p 4 -t 0;
##	./bin/preprocess -i ${GRID_HOME}${gom}${part2}.bin -o ${GRID_HOME}${gom}${part2}_grid/ -v ${nvertices} -p 4 -t 0;
##	./bin/preprocess -i ${GRID_HOME}${gom}${part3}.bin -o ${GRID_HOME}${gom}${part3}_grid/ -v ${nvertices} -p 4 -t 0;

##	echo "./bin/preprocess -i ${GRID_HOME}${gol}${part}.bin -o ${GRID_HOME}${gol}${part}_grid/ -v ${nvertices} -p 4 -t 0";
##	./bin/preprocess -i ${GRID_HOME}${gol}${part}.bin -o ${GRID_HOME}${gol}${part}_grid/ -v ${nvertices} -p 4 -t 0;
##	./bin/preprocess -i ${GRID_HOME}${gol}${part2}.bin -o ${GRID_HOME}${gol}${part2}_grid/ -v ${nvertices} -p 4 -t 0;
##	./bin/preprocess -i ${GRID_HOME}${gol}${part3}.bin -o ${GRID_HOME}${gol}${part3}_grid/ -v ${nvertices} -p 4 -t 0;

	##./bin/preprocess -i ~/bigdata/gkaur007/gridgraph/u2gohi${nparts}.bin -o ~/bigdata/gkaur007/gridgraph/u2gohi{part}_grid/ -v ${nvertices} -p 4 -t 0;
         
  	##./bin/preprocess -i ~/bigdata/gkaur007/gridgraph/u2mt8.bin -o ~/bigdata/gkaur007/gridgraph/u2mt8_grid -v 18520487 -p 4 -t 0;
	##./bin/preprocess -i ~/bigdata/gkaur007/gridgraph/ljmt16.bin -o ~/bigdata/gkaur007/gridgraph/ljmt16_grid -v 5204177 -p 4 -t 0;
##done;

##./bin/pagerank ~/bigdata/gkaur007/gridgraph/wkgohi8_grid/ 20 8 >> pgout;

##./bin/pagerank ~/bigdata/gkaur007/gridgraph/wkmt8_grid/ 20 8 >> pgout;
##./bin/pagerank ./wikigohi_grid8/ 2 10;
