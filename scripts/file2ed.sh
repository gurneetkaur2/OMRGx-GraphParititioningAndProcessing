#!/bin/bash -l
#SBATCH --nodes=1        ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=32   ## for multithreading in shared memory
## ##SBATCH --num_threads=8
#SBATCH --mem=425G
#SBATCH --time=0-02:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="fil2edit_YGo"
#SBATCH -p short # This is the default partition, you can use any of the following; intel,

# Print current date
date

# Load samtools
##module load samtools

# Concatenate BAMs
##samtools cat -h header.sam -o out.bam in1.bam in2.bam

# Print name of node
hostname

## insert text in preceding line number
sed -i '1 i\12150976 288277822' ~/bigdata/gkaur007/metisdata/inputs/adj_wiki.graph
##sed -i '1 i\999999988 1963263821' ~/bigdata/gkaur007/metisdata/bigtwitter.graph
##sed -i '1 i\1248361801 1806067135' ~/bigdata/gkaur007/metisdata/friendster.graph
##sed -i '1 i\1248361801 1806067135' ~/bigdata/gkaur007/metisdata/friendster.graph

## delete line 
##sed -i '1d' ./inputs/adj_bigtwitter.graph;
##sed -i '1d' ./inputs/adj_youtube.graph;
##sed -i '2,99d' ../metisdata/inputs/adj_friendster.graph;

## replace line
##sed -i '1s/.*/41652230 1468365182/' ~/bigdata/gkaur007/metisdata/twitter14.graph
##sed -i '1s/.*/105153952 3301876564/' ~/bigdata/gkaur007/metisdata/ukdomain.graph
##sed -i '1s/.*/3072441  117185083/' ~/bigdata/gkaur007/metisdata/orkut.graph
