#!/bin/bash -l
#SBATCH --nodes=1        ## 
#SBATCH --ntasks=1      ## for distributed or MPI more than 1
#SBATCH --cpus-per-task=64   ## for multithreading in shared memory
## ##SBATCH --num_threads=8
#SBATCH --mem=225G
#SBATCH --time=0-06:00:00     # 1 day and 15 minutes 1-00:15:00
#SBATCH --mail-user=gkaur007@ucr.edu
#SBATCH --mail-type=ALL
#SBATCH --job-name="Count_Uk2"
#SBATCH -p intel # This is the default partition, you can use any of the following; intel,

# Print current date
date

# Load samtools
##module load samtools

# Concatenate BAMs
##samtools cat -h header.sam -o out.bam in1.bam in2.bam

# Print name of node
hostname


#sed -i '1d' ./adjgraph/adj_bigtwitter.graph

#echo "./count ../metisdata/livejournal.graph" >> outCountlines; 
#./count ../metisdata/livejournal.graph >> outCountlines; 
#echo "./count orkut.graph" >> outCountlines;
#./count ../metisdata/orkut.graph >> outCountlines;
#echo "./count ../metisdata/twitter.graph" >> outCountlines;
#./count ../metisdata/twitter.graph >> outCountlines;
#echo "./count ../metisdata/friendster.graph" >> outCountlines;
#./count ../metisdata/friendster.graph >> outCountlines;
#echo "./count ../metisdata/bigtwitter.graph" >> outCountlines;
#./count ../metisdata/bigtwitter.graph >> outCountlines;
#echo "./count ../metisdata/ukdomain.graph" >> outCountlines;
#echo "./count ../metisdata/ukdomain.graph" >> outCountlines;
##echo "./count ../metisdata/uk2002.graph " >> outCountlines;
##./count ./inputs/adj_uk2002.graph >> outCountlines;


##./count ../metisdata/inputs/adj_bigtwitter.graph >> outCountlines;

##sed -i '1 i\4847571 68993773' ~/bigdata/gkaur007/metisdata/livejournal.graph 
##sed -i '1 i\999999988 1963263821' ~/bigdata/gkaur007/metisdata/bigtwitter.graph
##sed -i '1 i\1248361801 1806067135' ~/bigdata/gkaur007/metisdata/friendster.graph #done

## delete line 
#sed -i '1d' ./inputs/adj_twitter.graph ##done
##sed -i '1d' ./inputs/adj_twitter.graph ##done

## replace line
## done ##sed -i '1s/.*/41652230 1468365182/' ~/bigdata/gkaur007/metisdata/twitter14.graph
sed -i '1d' ~/bigdata/gkaur007/datasets/inputs/adj_ukdomain.graph
##sed -i '1s/.*/105153952 3301876564/' ~/bigdata/gkaur007/metisdata/ukdomain.graph
##sed -i '1s/.*/3072441  117185083/' ~/bigdata/gkaur007/metisdata/orkut.graph ##done
