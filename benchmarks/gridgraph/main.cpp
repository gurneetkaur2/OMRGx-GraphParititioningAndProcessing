//RUN: ./graphchi.bin ~/work/datasets/inputs/mdual 1 2 2 2000 100 258570 2 10000 test -DUSE_GOMR -DUSE_GRAPHCHI
#include "data.pb.h"

#define USE_NUMERICAL_HASH

#include "../../engine/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>
#include <google/malloc_extension.h>
#include <mutex>

#define INIT_VAL -1
#define SHARDSIZE_MB 1024
#define MAX_UINT (ULLONG_MAX)
int nvertices;
int nmappers;
static std::string outputPrefix = "";

const long double DAMPING_FACTOR = 0.85; // Google's recommendation in original paper.
const long double TOLERANCE = (1e-1);
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through
__thread IdType totalRecords = 0;
static pthread_barrier_t barCompute;
static pthread_barrier_t barWait;
std::mutex m;

template <typename KeyType, typename ValueType>
class GridGraph : public MapReduce<KeyType, ValueType>
{
  static thread_local std::ofstream ofile;
  static thread_local IdType edgeCounter;
  static thread_local unsigned iteration;

  public:
 // std::vector<IdType> *prOutput;

  void* beforeMap(const unsigned tid) {
    unsigned nCols = this->getCols();
    efprintf(stderr, "TID: %d, nvert:  %d part: %d \n", tid, nvertices, tid % nCols);
    return NULL;
  }

  void writeInit(unsigned nCols, unsigned nVtces){
      //prOutput = new std::vector<IdType>[nCols];
      edgeCounter = 0;
      iteration = 0;
      efprintf(stderr,"\nInside writeINIT ************NOT EMPTY Vertices: %d ", nvertices);
  }

unsigned setPartitionId(const unsigned tid)
  {
    unsigned nCols = this->getCols();
    //fprintf(stderr,"\nTID: %d writing to partition: %d " , tid, tid % nCols);
   return -1; //tid % nCols;
  }

  void* map(const unsigned tid, const unsigned fileId, const std::string& input, const unsigned nbufferId, const unsigned hiDegree)
  {
  // fprintf(stderr,"\nTID: %d Inside Map", tid);
    std::stringstream inputStream(input);
    unsigned to, token;
    std::vector<unsigned> from;
    unsigned currentShardCount = tid;

    inputStream >> to;

    while(inputStream >> token){
      from.push_back(token);
    }
    std::sort(from.begin(), from.end()); // using default comparator

    for(unsigned i = 0; i < from.size(); ++i){
           Edge e;
           e.src = to;
           e.dst = from[i];
           e.rank = 1.0/nvertices;
           e.vRank = 1.0/nvertices;
           e.numNeighbors = from.size();
         
           edgeCounter++;
           // create edgeblock partitioning based on destination vertex
           unsigned bufferId = hashKey(from[i]) % this->getCols();
 //     fprintf(stderr,"\tTID: %d, src: %zu, dst: %zu, vrank: %f, rank: %f nNbrs: %u", tid, e.src, e.dst, e.vRank, e.rank, e.numNeighbors);
      this->writeBuf(tid, to, e, bufferId, from.size());
    }
    return NULL;
  }

  void* beforeReduce(const unsigned tid) {
      edgeCounter = 0;
   // fprintf(stderr,"\nTID: %d AFTER beforeReduce ", tid);
  }

  void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) {
    timeval s, e;
    IdType indexCount = 0;
    std::vector<Edge *> vertices; 
    auto it_first = container.begin();
    IdType v = it_first->second[0].dst;
    //fprintf(stderr,"\nSHARD: %u, CONTAINER elements to SHIVEL LowerBound: %d ", tid, ii[shard].lbIndex);
    for(auto it = container.begin(); it != container.end(); it++){
      for(int k=0; k<it->second.size(); k++) {
           Edge e = it->second[k];
           v = e.dst;
         vertices.push_back(&e);
        indexCount++;
        while(k<it->second.size() && v==e.dst) k++;  //Skip the remaining in the adjlist
        }
    }
    gettimeofday(&e, NULL);
    efprintf(stderr, "\nInitializing subgraph for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
    assert(vertices.size() == indexCount);
  
   // then parallel-process shard -- calculate Pagerank
    gettimeofday(&s, NULL);
                                                          
       efprintf(stderr, "TID: %d, PR Processing shard: %u \n", tid, memoryShard);
    for(unsigned i= 0; i<vertices.size(); ) { 
       IdType dst = vertices[i]->dst;
       long double sum = 0.0;

       // process neighbors -- Read Window
       unsigned dstStartIndex = i;
       while(i<vertices.size() && dst == vertices[i]->dst){
            if(vertices[i]->numNeighbors > 0){
              sum += (vertices[i]->rank / vertices[i]->numNeighbors);
            }
            i++;
       }
       double rank = (1-DAMPING_FACTOR) + (DAMPING_FACTOR*sum);
       //fprintf(stderr,"\nSHARD: %u, PR VERTICEs StartIndex : %u, ENDIndex: %u, Rank: %f ", memoryShard, dstStartIndex, i, rank);
       unsigned dstEndIndex = i;
       // Write Window
       for(unsigned dstIndex = dstStartIndex; dstIndex<dstEndIndex; dstIndex++){
          auto my_lock = std::unique_lock<std::mutex>(m);
          vertices[dstIndex]->vRank = rank;
       }
  }
     gettimeofday(&e, NULL);
     efprintf(stderr, "!!!!!Parallel processing of subgraph for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
     efprintf(stderr, "-------------------------%c\n", '-');
   efprintf(stderr,"\nTID %d Waiting at barrier ------ " , tid);
    return NULL;
  }

  void* updateReduceIter(const unsigned tid) {
    //fprintf(stderr,"\nTID: %d Updating reduce Iteration ", tid);

    edgeCounter = 0;
    ++iteration;
    if(iteration >= this->getIterations()){
       efprintf(stderr, "\nTID: %d, Iteration: %d Complete ", tid, iteration);
       don = true;
       return NULL;
    }
    this->notDone(tid);
     // assign next to prev for the next iteration , copy to prev of all threads
     
    efprintf(stderr,"\nTID: %d, iteration: %d ----", tid, iteration);
   return NULL;
  }

  void* afterReduce(const unsigned tid) {
    //fprintf(stderr,"\nTID: %d After Reduce ", tid);
    return NULL;
  }
  
/*  void printParts(const unsigned tid, std::string fileName) {
    ofile.open(fileName);
    assert(ofile.is_open());
    for(unsigned i = 0; i <= nvertices; ++i){
      if(prOutput[tid][i] != INIT_VAL)
        ofile<<i << "\t" << prOutput[tid][i]<< std::endl;
    }
    ofile.close();
  }*/
};


template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
  to.insert(to.end(), from.begin(), from.end());
  return NULL;
}

template <typename KeyType, typename ValueType>
thread_local std::ofstream GridGraph<KeyType, ValueType>::ofile;

template <typename KeyType, typename ValueType>
thread_local IdType GridGraph<KeyType, ValueType>::edgeCounter;

template <typename KeyType, typename ValueType>
thread_local unsigned GridGraph<KeyType, ValueType>::iteration;

//-------------------------------------------------
int main(int argc, char** argv)
{
  GridGraph<IdType, Edge> gc;

  if (argc < 8)
  {
  std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems> <optional - nvertices> <optional - iters> <hidegree> <optional - partition output prefix>" << std::endl;

  return 0;
}

std::string folderpath = argv[1];
int gb = atoi(argv[2]);
nmappers = atoi(argv[3]);
int nreducers = atoi(argv[4]);
int niterations = 1;
int hiDegree;
#ifdef USE_GOMR
nvertices = atoi(argv[7]);
niterations = atoi(argv[8]);
if(atoi(argv[9]) > 0)
   hiDegree = atoi(argv[9]);
else
   hiDegree = 0;

if(argv[10])
  outputPrefix = argv[10];
else
  outputPrefix = "";
#else
nvertices = -1;
niterations = 1;
hiDegree = 0;
#endif

int batchSize = atoi(argv[5]);
int kitems = atoi(argv[6]);

assert(batchSize > 0);

gc.init(folderpath, gb, nmappers, nreducers, nvertices, hiDegree, batchSize, kitems, niterations);

gc.writeInit(nreducers, nvertices);
//slidingShard = (Edge **) calloc(numShards, sizeof(Edge *));

pthread_barrier_init(&barCompute, NULL, nreducers);
pthread_barrier_init(&barWait, NULL, nreducers);

double runTime = -getTimer();
gc.run();
runTime += getTimer();

std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
MallocExtension::instance()->ReleaseFreeMemory();
return 0;
}

