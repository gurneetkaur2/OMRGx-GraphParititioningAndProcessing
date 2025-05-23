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

#define INIT_VAL -1
#define SHARDSIZE_MB 1024
#define MAX_UINT (ULLONG_MAX)
int nvertices;
int nmappers;
static std::string outputPrefix = "";

//EdgeList* edgeLists = NULL;
IdType totalEdges = 0;
unsigned long long numEdges = 0;
unsigned edgesPerShard = SHARDSIZE_MB*1024*1024/sizeof(IdType);
//Meta data for GraphChi shard processing
typedef struct __intervalInfo {
  IdType lbEdgeCount;
  IdType ubEdgeCount;
  IdType edgeCount; // dont need this

  IdType lbIndex;
  IdType ubIndex;
  IdType indexCount; // dont need this
} IntervalInfo;
IntervalInfo *ii = NULL;

//------------------------------------------------------------------
typedef struct __intervalLengths {
    IdType startEdgeIndex;
      IdType endEdgeIndex;
        IdType length;
} IntervalLengths;

IntervalLengths **gcd; // GraphChiData
//------------------------------------------------------------------

const long double DAMPING_FACTOR = 0.85; // Google's recommendation in original paper.
const long double TOLERANCE = (1e-1);
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through
//__thread unsigned iteration = 0;
__thread IdType totalRecords = 0;
//__thread IdType edgeCounter = 0;
//pagerank for previous and next iteration
  std::vector<Edge>* readEdges;
std::vector<double> ds_times;
std::vector<double> pr_times;
static pthread_barrier_t barCompute;
static pthread_barrier_t barWait;
unsigned *ssIndex;
pthread_mutex_t countTotalTime;
static double countTotalIterTime = 0.0;

template <typename KeyType, typename ValueType>
class GraphChi : public MapReduce<KeyType, ValueType>
{
  static thread_local std::ofstream ofile;
  static thread_local IdType edgeCounter;
  static thread_local unsigned iteration;
  static thread_local uint64_t countVertexUpdatesPerThread;
  static thread_local double iterationTime;
  static thread_local bool donePr;

  public:
 // std::vector<IdType> *prOutput;

  void* beforeMap(const unsigned tid) {
    unsigned nCols = this->getCols();
    efprintf(stderr, "TID: %d, nvert:  %d part: %d \n", tid, nvertices, tid % nCols);
    return NULL;
  }

  void writeInit(unsigned nCols, unsigned nVtces){
      readEdges = new std::vector<Edge>[nCols];
      //prOutput = new std::vector<IdType>[nCols];
      edgeCounter = 0;
      iteration = 0;
      efprintf(stderr,"\nInside writeINIT ************NOT EMPTY Vertices: %d ", nvertices);
      ds_times.resize(nCols, 0.0);
      pr_times.resize(nCols, 0.0);
/*    for(unsigned i=0; i<nCols; i++){ 
        for(IdType j=0; j<=nVtces; j++) 
          prOutput[i][j] = -1;
     }*/
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
    double time_mr = -getTimer();
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
           e.src = from[i];
           e.dst = to;
           e.rank = 1.0/nvertices;
           e.vRank = 1.0/nvertices;
           e.numNeighbors = from.size();
         
           edgeCounter++;
 //     fprintf(stderr,"\tTID: %d, src: %zu, dst: %zu, vrank: %f, rank: %f nNbrs: %u", tid, e.src, e.dst, e.vRank, e.rank, e.numNeighbors);
      this->writeBuf(tid, to, e, nbufferId, from.size());
    }
    ii[tid%this->getCols()].ubEdgeCount = edgeCounter;
    time_mr += getTimer();
    iterationTime += time_mr;
    return NULL;
  }

  void* beforeReduce(const unsigned tid) {
  //  assert(false);
   // fprintf(stderr,"\nTID: %d BEFORE reduce ", tid);
      double time_br = -getTimer();
      Edge e;
      e.src = -1;
      e.dst = -1;
      e.rank = 1.0/nvertices;
      e.vRank = 1.0/nvertices;
      e.numNeighbors = 0;

    // fprintf(stderr,"\nInside before reduce ************ Container size: %d ECounter: %d  ", this->getContainerSize(), ii[tid].ubEdgeCount);
      //for (unsigned j = 0; j<=nvertices; ++j) {
      for (unsigned j = 0; j<ii[tid].ubEdgeCount; ++j) {
          readEdges[tid].push_back(e);
  //        ssIndex[tid][j] = 0;
        }
      edgeCounter = 0;
    time_br += getTimer();
    iterationTime += time_br;
   efprintf(stderr,"\nTID: %d AFTER beforeReduce ", tid);
  }

  void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) {
    //BuildGraphChi Metadata
    double time_ds = -getTimer();
    unsigned shard = tid;
    IdType indexCount = 0;
     //fprintf(stderr,"\nInside reduce ************ Container size: %d ", container.size());

    ii[shard].lbEdgeCount = edgeCounter;
    unsigned memoryShard = tid;  // current partition/tid is memoryShard
    assert(container.size() > 0);
    auto it_first = container.begin();
    ii[shard].lbIndex = it_first->first;
    
    IdType lbIndex = container.begin()->first;
    efprintf(stderr, "Initialize sub-graph: %u\n", memoryShard);
    timeval s, e;
    gettimeofday(&s, NULL);
   // std::vector<Edge *> vertices; 
   unsigned id = 0;
   // IdType v = it_first->second[0].dst;
    //fprintf(stderr,"\nSHARD: %u, CONTAINER elements to SHIVEL LowerBound: %d ", tid, ii[shard].lbIndex);
    for(auto it = container.begin(); it != container.end(); it++){
        // fprintf(stderr,"\nSHARD: %u, CONTAINER elements Key: %u, value size: %d, values: ", tid, it->first, it->second.size());
      for(int k=0; k<it->second.size(); k++) {
           Edge e = it->second[k];
          // v = e.dst;
      //  fprintf(stderr,"\nSHARD: %d ES elements dst: %zu src: %zu ", shard, it->second[k].dst, it->second[k].src);
          readEdges[shard][id++] = (it->second[k]);
        //  fprintf(stderr,"\t src:%zu dst %zu ", e.src, e.dst);
        // vertices.push_back(&e);
        //while(k<it->second.size() && v==e.dst) k++;  //TODO: edgecount should be buffer count
         edgeCounter++;
        }
        indexCount++;
        ii[shard].ubIndex = it->first;
    }
    std::vector<Edge *> vertices; 
    IdType v = it_first->second[0].dst;
    //fprintf(stderr,"\nSHARD: %u, CONTAINER elements to SHIVEL LowerBound: %d ", tid, ii[shard].lbIndex);
    for(auto it = container.begin(); it != container.end(); it++){
      for(int k=0; k<it->second.size(); k++) {
           Edge e = it->second[k];
           v = e.dst;
         vertices.push_back(&e);
        while(k<it->second.size() && v==e.dst) k++;  //TODO: edgecount should be buffer count
        }
    }
    gettimeofday(&e, NULL);
    efprintf(stderr, "\nInitializing subgraph for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
    assert(readEdges != NULL);
   ii[shard].indexCount = indexCount;
   ii[shard].ubEdgeCount = edgeCounter;
   ii[shard].edgeCount = readEdges[shard].size();
   assert(vertices.size() == ii[shard].indexCount);
  
    //fprintf(stderr,"\nShard: %d Waiting at BARRIER ", tid);
  // pthread_barrier_wait(&(barCompute)); 

    IdType count = 0;
    IdType edgeGCounter = ii[tid].lbEdgeCount;
   for(unsigned j=0; j < this->getCols(); j++){
        gcd[tid][j].startEdgeIndex = edgeGCounter;
        efprintf(stderr, "\nProcessing shard: %u, interval %u: StartVertex: %zu, EndVertex: %zu, SEdgeIndex: %zu, readEdges SRC: %u\n", tid, j, ii[j].lbIndex, ii[j].ubIndex, gcd[tid][j].startEdgeIndex, readEdges[shard][count].src);
        while(readEdges[shard][count].src >= ii[j].lbIndex && readEdges[shard][count].src < ii[j].ubIndex && count < ii[tid].edgeCount) {
              count++; // skip over
              edgeGCounter++;
        }
    efprintf(stderr, "After interval: %zu (prev: %zu)\n", readEdges[shard][count].src, readEdges[shard][count-1].src);
    gcd[tid][j].endEdgeIndex = edgeGCounter;
    gcd[tid][j].length = gcd[tid][j].endEdgeIndex - gcd[tid][j].startEdgeIndex;
    efprintf(stderr, "Shard: %u, Interval: %u, Start: %zu, End: %zu, Length: %zu\n", tid, j, gcd[tid][j].startEdgeIndex, gcd[tid][j].endEdgeIndex, gcd[tid][j].length);
    }
   
    time_ds += getTimer();
    ds_times[tid] += time_ds;
    efprintf(stderr, "%c---------------------------------\n", '-');
    // ***************** Done building meta data *****************
   efprintf(stderr, "Sorting memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));

    efprintf(stderr, "Num vertices: %zu, IC: %zu\n", vertices.size(), ii[shard].indexCount); //container.size());
    // then parallel-process shard -- calculate Pagerank
    gettimeofday(&s, NULL);
                     

     double time_pr = -getTimer();
     efprintf(stderr, "TID: %d, PR Processing shard: %u \n", tid, memoryShard);
      donePr = false;
    
     for(unsigned i= 0; i<vertices.size(); ) { 
       IdType dst = vertices[i]->dst;
       long double sum = 0.0;

       // process neighbors
       unsigned dstStartIndex = i;
       while(i<vertices.size() && dst == vertices[i]->dst){
            if(vertices[i]->numNeighbors > 0){
              sum += (vertices[i]->rank / vertices[i]->numNeighbors);
            }
            i++;
       }

       double rank = ((1-DAMPING_FACTOR) + (DAMPING_FACTOR*sum));
       //fprintf(stderr,"\nSHARD: %u, PR VERTICEs StartIndex : %u, ENDIndex: %u, Rank: %f ", memoryShard, dstStartIndex, i, rank);
      
      unsigned dstEndIndex = i;
      
      for(unsigned dstIndex = dstStartIndex; dstIndex<dstEndIndex; dstIndex++){
          long double old = vertices[dstIndex]->vRank;
     	// Count vertices per thread - vertex update happens only when previous rank is greater than new rank
	  if(vertices[dstIndex]->vRank != rank)
     	    countVertexUpdatesPerThread++;
          
	 vertices[dstIndex]->vRank = rank;
         
	// if (fabs(old - rank) >= 0.1 ){
           //fprintf(stderr, "\ntid: %d old: %f, rank: %f ", tid, old, rank);
	//   donePr = true;
	//   }
	  // if(fabs(old - rank) < 0.0001)
	  //   donePr = true;
	// else
        // if(!outputPrefix.empty())
         // prOutput[tid].at(dstIndex) = rank; // should not conflict with the other threads writing to it as vertices will be different
       }
       
   // update dst's rank in sliding shards
   unsigned ss = 0;
   for(; ss<this->getCols(); ss++) { // current interval is 'memoryShard'; find dst in slidingShard[ss] and update
     while(ssIndex[ss]<gcd[ss][memoryShard].length && dst != readEdges[ss][ssIndex[ss]].src) ssIndex[ss]++;
       if(dst == readEdges[ss][ssIndex[ss]].src) {
         while(ssIndex[ss]<gcd[ss][memoryShard].length && dst == readEdges[ss][ssIndex[ss]].src) { // update ALL instances
          efprintf(stderr, "TID: %u - Updating shard: %u, %zu\n", tid, memoryShard, readEdges[ss][ssIndex[ss]].src);
	  if(readEdges[ss][ssIndex[ss]++].rank != vertices[i]->vRank)
     	    countVertexUpdatesPerThread++;
               readEdges[ss][ssIndex[ss]++].rank = vertices[i]->vRank;
         }
         break; // proceed to next sliding shard
       }
    }
  }
    time_pr += getTimer();
    pr_times[tid] += time_pr;
    iterationTime += (time_ds + time_pr);
     gettimeofday(&e, NULL);
     efprintf(stderr, "!!!!!Parallel processing of subgraph for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
     efprintf(stderr, "-------------------------%c\n", '-');
   efprintf(stderr,"\nTID %d Waiting at barrier ------ " , tid);
   pthread_barrier_wait(&(barWait)); 
    readEdges[tid].clear();
    //diskWriteContainer(tid, ii[shard].lbEdgeCount, ii[shard].edgeCount, readEdges[tid].begin(), readEdges[tid].end());
   // totalRecords += readEdges[tid].size() ;
    return NULL;
  }

  void* updateReduceIter(const unsigned tid) {
    efprintf(stderr,"\nTID: %d Updating reduce Iteration, donePr: %d ", tid, donePr);

    edgeCounter = 0;
    ++iteration;
    pthread_mutex_lock(&countTotalTime);
      countTotalIterTime += iterationTime;
      pthread_mutex_unlock(&countTotalTime);
   // if (donePr){
    if(iteration >= this->getIterations()){
       efprintf(stderr, "\nTID: %d, Iteration: %d Complete , vertices updated per thread: %d", tid, iteration, countVertexUpdatesPerThread);
       efprintf(stderr, "\nTID: %d, iterationTime: %f, TotalIterTime: %f ", tid, iterationTime, countTotalIterTime);
       don = true;
      return NULL;
    }
    this->notDone(tid);
     // assign next to prev for the next iteration , copy to prev of all threads
    
    efprintf(stderr,"\nTID: %d, iteration: %d , vertices updated per thread: %d ----", tid, iteration, countVertexUpdatesPerThread);
       efprintf(stderr, "\nTID: %d, iterationTime: %f, TotalIterTime: %f ", tid, iterationTime, countTotalIterTime);
     countVertexUpdatesPerThread = 0;
    // iterationTime = 0;
  return NULL;
  }

  void* afterReduce(const unsigned tid) {
   // readEdges[tid].clear();
//    if(!outputPrefix.empty()){
  //    std::string fileName = outputPrefix + std::to_string(tid);
    //   printParts(tid, fileName.c_str());
    //}
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
thread_local uint64_t GraphChi<KeyType, ValueType>::countVertexUpdatesPerThread = 0;

template <typename KeyType, typename ValueType>
thread_local double GraphChi<KeyType, ValueType>::iterationTime = 0;

template <typename KeyType, typename ValueType>
thread_local bool GraphChi<KeyType, ValueType>::donePr = 0;

template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
  to.insert(to.end(), from.begin(), from.end());
  return NULL;
}

template <typename KeyType, typename ValueType>
thread_local std::ofstream GraphChi<KeyType, ValueType>::ofile;

template <typename KeyType, typename ValueType>
thread_local IdType GraphChi<KeyType, ValueType>::edgeCounter;

template <typename KeyType, typename ValueType>
thread_local unsigned GraphChi<KeyType, ValueType>::iteration;

//-------------------------------------------------
int main(int argc, char** argv)
{
  GraphChi<IdType, Edge> gc;

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
ii = (IntervalInfo *) calloc(nreducers, sizeof(IntervalInfo));
assert(ii != NULL);


gcd = (IntervalLengths **) calloc(nreducers, sizeof(IntervalLengths *));
for(unsigned i=0; i<nreducers; i++) {
    gcd[i] = (IntervalLengths *) calloc(nreducers, sizeof(IntervalLengths));
    assert(gcd[i] != NULL);
}

/*if(!outputPrefix.empty()){
prOutput = (IdType **) calloc(nreducers, sizeof(IdType *));
for(unsigned i=0; i<nreducers; i++) {
    prOutput[i] = (IdType *) calloc(nreducers, sizeof(IdType));
     for(IdType j=0; j<nvertices; j++) 
          prOutput[i][j] = INIT_VAL;
    assert(prOutput[i] != NULL);
}
}*/
// for efficiently traversing sliding shards
ssIndex = (unsigned *) calloc(nreducers, sizeof(unsigned)); assert(ssIndex != NULL);

double runTime = -getTimer();
gc.run();
runTime += getTimer();

auto ds_time = max_element(std::begin(ds_times), std::end(ds_times));
std::cout << " Data Structure time : " << *ds_time << " (msec)" << std::endl;

auto pr_time = max_element(std::begin(pr_times), std::end(pr_times));
std::cout << " Page Rank Processing time : " << *pr_time << " (msec)" << std::endl;

std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
free(ii); free(gcd); free(ssIndex); //free(prOutput);
delete[] readEdges;
MallocExtension::instance()->ReleaseFreeMemory();
return 0;
}

