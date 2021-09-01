#ifdef USE_ONE_PHASE_IO
#include "recordtype.h"
#else
#include "data.pb.h"
#include "edgelist.pb.h"
#endif

#define USE_NUMERICAL_HASH

#include "../../engine/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>

#define SHARDSIZE_MB 1024
#define MAX_UINT (ULLONG_MAX)
int nvertices;
int nmappers;

EdgeList* edgeLists = NULL;
IdType totalEdges = 0;
unsigned long long numEdges = 0;
char modifiedArg[256] = {0x0};
const char *compThreadsArg = "--prodsm-compthreads=";
//#define SHARDSIZE_MB 1024 // via compiler directive
unsigned edgesPerShard = SHARDSIZE_MB*1024*1024/sizeof(IdType);
unsigned numShards = 0;
unsigned memoryShard = 0;
typedef struct _edgeType {
  IdType src;
  IdType dst;

  double vRank;
  double rank; // of src
  IdType numNeighbors; // of src
} Edge;

/*struct edgeCompare {
 bool operator() (const Edge& a, const Edge& b) {
    if (a.src == b.src) return (a.dst <= b.dst);
    return (a.src <= b.src);
  }
} EdgeCompare;
*/

//Meta data for GraphChi shard processing
typedef struct __intervalInfo {
  IdType lbEdgeCount;
  IdType ubEdgeCount;
  IdType edgeCount;

  IdType lbIndex;
  IdType ubIndex;
  IdType indexCount;
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

//static uint64_t countTotalWords = 0;
//pthread_mutex_t countTotal;
Edge **slidingShard = NULL;
Edge **readEdges = NULL;
const long double DAMPING_FACTOR = 0.85; // Google's recommendation in original paper.
const long double TOLERANCE = (1e-1);
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through
__thread unsigned iteration = 0;
__thread IdType edgeCounter = 0;
//pagerank for previous and next iteration
  std::vector<double>* prev; // = (nvertices, -1);
  std::vector<double>* next; // = (nvertices, -1);

template <typename KeyType, typename ValueType>
class GraphChi : public MapReduce<KeyType, ValueType>
{
 // static thread_local uint64_t countThreadWords;
  //static thread_local std::vector<unsigned> prev; // = (nvertices, -1);
  //static thread_local std::vector<unsigned> next; // = (nvertices, -1);

  public:

  void* beforeMap(const unsigned tid) {
    unsigned nCols = this->getCols();
    fprintf(stderr, "TID: %d, nvert:  %d \n", tid, nvertices);
    ii[tid % nCols].lbIndex = MAX_UINT;  //Account for nCols not nRows
    //ii[tid % nCols].lbEdgeCount = 0;
    for (unsigned i = 0; i<=nvertices; ++i) {
        prev[tid % nCols].push_back(1.0/nvertices);
        next[tid % nCols].push_back(1.0/nvertices);
     // fprintf(stderr, "\n TID: %d, BEFORE key: %d prev: %f next: %f rank: %.2f \n", tid, i, prev[tid][i], next[tid][i], (1/nvertices));
    }
    return NULL;
  }
  void* map(const unsigned tid, const unsigned fileId, const std::string& input, const unsigned nbufferId, const unsigned hiDegree)
  {

    std::stringstream inputStream(input);
    unsigned to, token;
    std::vector<unsigned> from;
    unsigned currentShardCount = tid;
    //bool doneCurrentShovel = false;
    //IdType bufferSize = edgesPerShard;
    IdType indexCount = 0;

    inputStream >> to;
    indexCount++;

//    IdType currentShovelSize = 0;
    if(ii[currentShardCount].lbIndex > to)
      ii[currentShardCount].lbIndex = to;
    ii[currentShardCount].lbEdgeCount = edgeCounter;
    //std::vector<Edge> shovel; shovel.clear();


    while(inputStream >> token){
      from.push_back(token);
    }
    std::sort(from.begin(), from.end()); // using default comparator

    prev[tid].at(to) = 1.0/nvertices;
    for(unsigned i = 0; i < from.size(); ++i){
      //                fprintf(stderr,"\nVID: %d FROM: %zu size: %zu", to, from[i], from.size());
/*      Edge e;
      e.src = from[i];
      e.dst = to;
  *///    e.rank = 1.0/nvertices;
      prev[tid].at(from[i]) = 1.0/nvertices;
   //   e.vRank = 1.0/nvertices;
   //   e.numNeighbors = from.size(); 

      //shovel.push_back(e);
      this->writeBuf(tid, to, from[i], nbufferId, 0);
    }
    edgeCounter += from.size();
    if(ii[currentShardCount].ubIndex < to)
      ii[currentShardCount].ubIndex = to;
    ii[currentShardCount].indexCount = indexCount; //total vertices
    ii[currentShardCount].ubEdgeCount = edgeCounter;  //total edges encountered 
    ii[currentShardCount].edgeCount = edgeCounter; //edges in partition

  }

  void* beforeReduce(const unsigned tid) {
  }

  void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) {
    //std::vector<ValueType>& values) {
    //countThreadWords += std::accumulate(values.begin(), values.end(), 0);
    //BuildGraphChi Metadata
    std::vector<Edge> shovel; shovel.clear();
    IdType edgeCounter = ii[tid].lbEdgeCount;
    for(InMemoryContainerIterator<KeyType, ValueType> it = container.begin(); it != container.end(); it++){
        for(std::vector<unsigned>::const_iterator vit = it->second.begin(); vit != it->second.end(); vit++){ 
           Edge e;
           e.src = *vit;
           e.dst = it->first;
           shovel.push_back(e);
        }
    }
    //This loads data for Memory shard too
    readEdges[shard] = (Edge*) calloc( container.size(), sizeof(Edge) );
    for(IdType i=0; i<shovel.size(); i++) {
        readEdges[i] = shovel[i];
    }
    assert(readEdges != NULL);

    IdType count = 0;
    for(unsigned j=0; j < this->getnCols; i++){
        gcd[tid][j].startEdgeIndex = edgeCounter;
        fprintf(stderr, "Processing shard: %u, interval %u: StartVertex: %zu, EndVertex: %zu, SEdgeIndex: %zu\n", tid, j, ii[tid].lbIndex, ii[j].ubIndex, gcd[tid][j].startEdgeIndex);
        while(readEdges[count].src >= ii[j].lbIndex && readEdges[count].src < ii[j].ubIndex && count < ii[tid].edgeCount) {
              count++; // skip over
              edgeCounter++;
        }
    }
    fprintf(stderr, "After interval: %zu (prev: %zu)\n", readEdges[count].src, readEdges[count-1].src);
    gcd[tid][j].endEdgeIndex = edgeCounter;
    gcd[tid][j].length = gcd[tid][j].endEdgeIndex - gcd[tid][j].startEdgeIndex;
    fprintf(stderr, "Shard: %u, Interval: %u, Start: %zu, End: %zu, Length: %zu\n", i, j, gcd[tid][j].startEdgeIndex, gcd[tid][j].endEdgeIndex, gcd[tid][j].length);
   
    fprintf(stderr, "%c---------------------------------\n", '-');
    //***************** Done building meta data *****************
    memoryShard = tid;  // current partition/tid is memoryShard
 //for(unsigned shard=0; shard< this->getCols(); shard++) {
    shard = tid;
    timeval s, e;
    fprintf(stderr, "Initialize sub-graph: %u\n", memoryShard);
    gettimeofday(&s, NULL);
    std::vector<Edge *> vertices; 
    unsigned vertexCounter = 0;
    IdType v = shovel[0]->dst;
    for(unsigned j=0; j<ii[shard].edgeCount;) {
        v = shovel[j]->dst;
        vertices.push_back(shovel[j++]); vertexCounter++;
        while(j<ii[shard].edgeCount && v==shovel[j]->dst) j++;  //TODO: edgecount should be buffer count
    }
    gettimeofday(&e, NULL);
    fprintf(stderr, "Initializing subgraph for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
    fprintf(stderr, "Num vertices: %zu, IC: %zu\n", vertices.size(), ii[tid].indexCount);
    assert(vertices.size() == ii[shard].indexCount);
    
     // then load sliding shards
     gettimeofday(&s, NULL);
     for(unsigned ss=0; ss<this->getCols(); ss++) {
        if(ss == shard) { 
           slidingShard[ss] = readEdges[shard];
           continue;
        }
        efprintf(stderr, "Loading sliding shard: %u\n", ss);
        slidingShard[ss] = (Edge *) calloc(gcd[ss][shard].length, sizeof(Edge));
        std::vector<Edge> edges; edges.clear();
        for(unsigned i=gcd[ss][shard].startEdgeIndex, i<gcd[ss][shard].length; i++){
            egdes[i] = readEdges[ss][i];
        }
        slidingShard[ss] = edges;
        assert(slidingShard[ss] != NULL);
     }
    gettimeofday(&e, NULL);
    fprintf(stderr, "Loading sliding shards for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
    
    // then parallel-process shard
    bool changed = true;
    gettimeofday(&s, NULL);
                                                          
    long double sum = 0.0;
       // iterate each vertex neighbor in adjlist
   // fprintf(stderr, "TID: %d, Reducing values \n", tid);
   /* for(auto it = values.begin(); it != values.end(); ++it) { 
        float val = prev[tid].at(*it);
        sum += val;
    }*/
     long double key = 1;
     long double old = prev[tid].at(key);
     //next[tid].at(key) = (1-DAMPING_FACTOR) + (DAMPING_FACTOR*sum);
      float pagerank = DAMPING_FACTOR + (1 - DAMPING_FACTOR) * sum;

    for(auto it = container.begin(); it != container.end(); ++it) { 
         // check if the fetched neighbor has its adjlist
        //need adjlist size for each neighbor 
        // need to know number of neighbors for each value???? 
      //  fprintf(stderr, "TID: %d checking values it: %d \n", tid, *it);
       // fprintf(stderr, "TID: %d, Prev: %f Neighbor %d \n", tid, prev[tid].at(*it), nNbrs.at(*it));
   /*   if(this->nNbrs.at(*it) > 0) { 
        next[tid].at(*it) = ( pagerank / this->nNbrs.at(*it));
      }
      else
        fprintf(stderr, "TID: %d, Neighbors of %d not found \n", tid, *it);
    }*/
    }
    fprintf(stderr, "\n TID: %d, AFTER Key: %d prev: %f next: %f \n", tid, key, prev[tid].at(key), next[tid].at(key));
   // fprintf(stderr, "TID: %d, DONE Reducing key prev: %d, next: %d \n", tid, prev[tid].at(key), next[tid].at(key));
    next[tid].at(key) = pagerank;
    if(iteration > this->getIterations()){
      don = 1;
    }
     if( fabs(old - next[tid].at(key)) > TOLERANCE ){
      //  fprintf(stderr, "TID: %d, Still not done: %d  \n", tid, this->getDone(tid));
                        // flag to iterate records again--  
                        this->notDone(key);
                        don = 0; 
     }
    free(readEdges); readEdges = NULL;
    MallocExtension::instance()->ReleaseFreeMemory();
    return NULL;
  }

  void* updateReduceIter(const unsigned tid) {

    ++iteration;
    if(iteration > this->getIterations()){
           //      fprintf(stderr, "\nTID: %d, Iteration: %d Complete ", tid, iteration);
       don = true;
    }
    //fprintf(stderr, "AfterReduce\n");
     // assign next to prev for the next iteration , copy to prev of all threads
   //  iters = iters + 1;
     prev[tid].assign(next[tid].begin(), next[tid].end());
  //  for (auto i = 0; i < prev[tid].size(); i++){
    //  fprintf(stderr, "\n TID: %d, prev: %f next: %f \n", tid, prev[tid][i], next[tid][i]);
   // }
    fprintf(stderr,"\nTID: %d, iteration: %d ----", tid, iteration);
//fprintf(stderr, "pagerank: thread %u iteration %d took %.3lf ms to process %llu vertices and %llu edges\n", tid, iteration, timevalToDouble(e) - timevalToDouble(s), nvertices, nedges);
//}

   return NULL;
  }

  void* afterReduce(const unsigned tid) {

    return NULL;
  }

  //------------------------------------------------------------------
  void readEdgeLists(EdgeList* edgeLists) {
/*  IdType to, from;

  unsigned edge;
  while (infile >> to >> from >> edge) {
    edgeLists[to].add_nbrs(from);
    totalEdges++;
   }*/
  }

};


template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
  to.insert(to.end(), from.begin(), from.end());
  return NULL;
}

//-------------------------------------------------
int main(int argc, char** argv)
{
  GraphChi<unsigned, unsigned> gc;

  if (argc < 8)
  {
  std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems> <optional - nvertices> <optional - iters>" << std::endl;

  return 0;
}

std::string folderpath = argv[1];
int gb = atoi(argv[2]);
nmappers = atoi(argv[3]);
int nreducers = atoi(argv[4]);
int niterations = 1;
int hiDegree;
//int nvertices;
//  if (select == "OMR")
//    nvertices = -1;
#ifdef USE_GOMR
nvertices = atoi(argv[7]);
niterations = atoi(argv[8]);
if(atoi(argv[9]) > 0)
   hiDegree = atoi(argv[9]);
else
   hiDegree = 0;
#else
nvertices = -1;
niterations = 1;
hiDegree = -1
#endif

int batchSize = atoi(argv[5]);
int kitems = atoi(argv[6]);

assert(batchSize > 0);
//pthread_mutex_init(&countTotal, NULL);

    prev = new std::vector<double>[nmappers];
    next = new std::vector<double>[nmappers];

gc.init(folderpath, gb, nmappers, nreducers, nvertices, hiDegree, batchSize, kitems, niterations);

//slidingShard = (Edge **) calloc(numShards, sizeof(Edge *));
timeval s, e;
gettimeofday(&s, NULL);
fprintf(stderr, "Reading edge lists\n");
edgeLists = new EdgeList[nvertices];
//readEdgeLists(edgeLists);
gettimeofday(&e, NULL);
fprintf(stderr, "Reading edge lists took: %.3lf\n", tmDiff(s, e));

ii = (IntervalInfo *) calloc(numShards, sizeof(IntervalInfo));
assert(ii != NULL);

gcd = (IntervalLengths **) calloc(nreducers, sizeof(IntervalLengths *));
for(unsigned i=0; i<nreducers; i++) {
    gcd[i] = (IntervalLengths *) calloc(nreducers, sizeof(IntervalLengths));
    assert(gcd[i] != NULL);
}

double runTime = -getTimer();
gc.run();
runTime += getTimer();

std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
delete[] edgeLists; 
free(ii);
//std::cout << "Total words: " << countTotalWords << std::endl;
return 0;
}

