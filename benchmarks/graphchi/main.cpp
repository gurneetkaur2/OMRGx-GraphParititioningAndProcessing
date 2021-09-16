#ifdef USE_ONE_PHASE_IO
#include "recordtype.h"
#else
#include "data.pb.h"
#include "edgelist.pb.h"
#include "adjacencyList.pb.h"
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
#include <google/malloc_extension.h>

#define SHARDSIZE_MB 1024
#define MAX_UINT (ULLONG_MAX)
int nvertices;
int nmappers;

//EdgeList* edgeLists = NULL;
IdType totalEdges = 0;
unsigned long long numEdges = 0;
//char modifiedArg[256] = {0x0};
//const char *compThreadsArg = "--prodsm-compthreads=";
//#define SHARDSIZE_MB 1024 // via compiler directive
unsigned edgesPerShard = SHARDSIZE_MB*1024*1024/sizeof(IdType);
typedef struct _edgeType {
  IdType src;
  IdType dst;

  double vRank;
  double rank; // of src
  IdType numNeighbors; // of src
} Edge;

struct edgeCompare {
 bool operator() (const Edge& a, const Edge& b) {
    if (a.src == b.src) return (a.dst <= b.dst);
    return (a.src <= b.src);
  }
} EdgeCompare;


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

//static uint64_t countTotalWords = 0;
//pthread_mutex_t countTotal;
const long double DAMPING_FACTOR = 0.85; // Google's recommendation in original paper.
const long double TOLERANCE = (1e-1);
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through
__thread unsigned iteration = 0;
__thread IdType totalRecords = 0;
__thread IdType edgeCounter = 0;
//pagerank for previous and next iteration
  std::vector<Edge>* readEdges;
  std::vector<double>* prev; // = (nvertices, -1);
  std::vector<double>* next; // = (nvertices, -1);
static pthread_barrier_t barCompute;
static pthread_barrier_t barWait;

template <typename KeyType, typename ValueType>
class GraphChi : public MapReduce<KeyType, ValueType>
{

  public:

  void* beforeMap(const unsigned tid) {
    unsigned nCols = this->getCols();
    fprintf(stderr, "TID: %d, nvert:  %d part: %d \n", tid, nvertices, tid % nCols);
    return NULL;
  }

  void writeInit(unsigned nCols, unsigned nVtces){
      prev = new std::vector<double>[nCols];
      next = new std::vector<double>[nCols];
      readEdges = new std::vector<Edge>[nCols];
      unsigned init = 1.0/nVtces;
      Edge e;
      e.src = -1;
      e.dst = -1;
      //fprintf(stderr,"\nInside writeINIT ************ Cols: %d, Vertices: %d ", nCols, nVtces);
     for (unsigned i = 0; i<nCols; ++i) {
        for (unsigned j = 0; j<=nVtces; ++j) {
            prev[i].push_back(init);
            next[i].push_back(init);
           readEdges[i].push_back(e);
        }
     }
  }
unsigned setPartitionId(const unsigned tid)
  {
    unsigned nCols = this->getCols();
    //fprintf(stderr,"\nTID: %d writing to partition: %d " , tid, tid % nCols);
   return -1; // tid % nCols;
  }

  void* map(const unsigned tid, const unsigned fileId, const std::string& input, const unsigned nbufferId, const unsigned hiDegree)
  {
   //fprintf(stderr,"\nTID: %d Inside Map", tid);
    std::stringstream inputStream(input);
    unsigned to, token;
    std::vector<unsigned> from;
    unsigned currentShardCount = tid;

    inputStream >> to;

    while(inputStream >> token){
      from.push_back(token);
    }
    std::sort(from.begin(), from.end()); // using default comparator

    prev[tid].at(to) = 1.0/nvertices;
    for(unsigned i = 0; i < from.size(); ++i){
      prev[tid].at(from[i]) = 1.0/nvertices;
           /*Edge e;
           e.src = from[i];
           e.dst = to;
           e.rank = 1.0/nvertices;
           e.vRank = 1.0/nvertices;
           e.numNeighbors = from.size();
*/
      //shovel.push_back(e);
      this->writeBuf(tid, to, from[i], nbufferId, 0);
      //this->writeBuf(tid, to, e, nbufferId, 0);
    }

    return NULL;
  }

  void* beforeReduce(const unsigned tid) {
  //  assert(false);
  }

  void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) {
    //BuildGraphChi Metadata
    //fprintf(stderr,"\nTID: %d Inside REDUCER @@@@@@ " , tid);
    unsigned shard = tid;
    IdType indexCount = 0;

    ii[shard].lbEdgeCount = edgeCounter;
    unsigned memoryShard = tid;  // current partition/tid is memoryShard
    std::vector<Edge> shovel; shovel.clear();
    assert(container.size() > 0);
    auto it_first = container.begin();
    ii[shard].lbIndex = it_first->first;

    IdType lbIndex = container.begin()->first;
    //fprintf(stderr,"\nSHARD: %u, CONTAINER elements to SHIVEL LowerBound: %d ", tid, ii[shard].lbIndex);
    for(auto it = container.begin(); it != container.end(); it++){
     //     fprintf(stderr,"\nSHARD: %u, CONTAINER elements Key: %u, values: ", tid, it->first);
        for(std::vector<unsigned>::const_iterator vit = it->second.begin(); vit != it->second.end(); vit++){ 
         // fprintf(stderr,"\t%u ", *vit);
           Edge e;
           e.src = *vit; //src;
           e.dst = it->first; //*vit.dst; //it->first;
           e.rank = 1.0/nvertices;
           e.vRank = 1.0/nvertices;
           e.numNeighbors = it->second.size();
           shovel.push_back(e);
        }
        indexCount++;
        ii[shard].ubIndex = it->first;
    }
   // fprintf(stderr,"\nSHARD: %u, CONTAINER elements to SHIVEL upperBound: %d ", tid, ii[shard].ubIndex);
    edgeCounter += shovel.size();
   ii[shard].indexCount = indexCount;
   ii[shard].ubEdgeCount = edgeCounter;
   ii[shard].edgeCount = shovel.size();
   // Sort shovel and add to array
   //std::sort(shovel.begin(), shovel.end(), EdgeCompare);

    std::map< IdType, std::vector<IdType> > adjacencyList; adjacencyList.clear();
 //  fprintf(stderr,"\nTID: %d Inside Reduce container size: %d, Shovel Size: %d EdgeCounter: %zu ", tid, container.size(), shovel.size(), edgeCounter);
    //This loads data for Memory shard too
   // fprintf(stderr,"\nSHARD: %d ADJLIST readEdges[shard] size: %u ", shard, readEdges[shard].size());
    for(IdType i=0; i<shovel.size(); i++) {
        readEdges[shard][i] = shovel[i];
    //      fprintf(stderr,"\nSHARD: %u, READEDGES elements src: %u, dst: %u  adJList: %zu ", tid, readEdges[shard][i].src, readEdges[shard][i].dst, adjacencyList[readEdges[shard][i].dst]);
        IdType key = readEdges[shard][i].dst;
        adjacencyList[key].push_back(i); // index location in current shovel; to avoid sorting during processing
    }
    assert(readEdges != NULL);

    AdjacencyList *aList = new AdjacencyList[indexCount];
//    std::map< IdType, std::vector<IdType> > aList;
    std::map< IdType, std::vector<IdType> >::iterator it;
    unsigned idx = 0;
    for(it=adjacencyList.begin(), idx=0; it!=adjacencyList.end(); it++, idx++) {
        aList[idx].set_key(it->first);
        for(unsigned z=0; z<it->second.size(); z++) // in sorted order of keys
            aList[idx].add_nbrid(it->second[z]);
    }
    fprintf(stderr,"\nShard: %d Waiting at BARRIER ", shard);
   pthread_barrier_wait(&(barCompute)); 

    IdType count = 0;
    IdType edgeGCounter = ii[tid].lbEdgeCount;
   for(unsigned j=0; j < this->getCols(); j++){
        gcd[tid][j].startEdgeIndex = edgeGCounter;
        fprintf(stderr, "\nProcessing shard: %u, interval %u: StartVertex: %zu, EndVertex: %zu, SEdgeIndex: %zu, readEdges SRC: %u\n", tid, j, ii[j].lbIndex, ii[j].ubIndex, gcd[tid][j].startEdgeIndex, readEdges[shard][count].src);
        while(readEdges[shard][count].src >= ii[j].lbIndex && readEdges[shard][count].src < ii[j].ubIndex && count < ii[tid].edgeCount) {
          //fprintf(stderr,"\nTID: %d ii[j].lbIndex: %d, ii[j].ubIndex: %d ", tid, ii[j].lbIndex, ii[j].ubIndex);
        //while(readEdges[shard][count].src >= ii[j].lbIndex && readEdges[shard][count].src < ii[j].ubIndex && count < container.size()) {
              count++; // skip over
              edgeGCounter++;
        }
    fprintf(stderr, "After interval: %zu (prev: %zu)\n", readEdges[shard][count].src, readEdges[shard][count-1].src);
    gcd[tid][j].endEdgeIndex = edgeGCounter;
    gcd[tid][j].length = gcd[tid][j].endEdgeIndex - gcd[tid][j].startEdgeIndex;
    fprintf(stderr, "Shard: %u, Interval: %u, Start: %zu, End: %zu, Length: %zu\n", tid, j, gcd[tid][j].startEdgeIndex, gcd[tid][j].endEdgeIndex, gcd[tid][j].length);
    }
   
    fprintf(stderr, "%c---------------------------------\n", '-');
    //***************** Done building meta data *****************
 //for(unsigned shard=0; shard< this->getCols(); shard++) {
   timeval s, e;
   gettimeofday(&s, NULL);
   std::vector<Edge *> es; 
   es.reserve(ii[shard].edgeCount);
   fprintf(stderr, "*******Shard: %u EDGES: %zu\n", shard, ii[shard].edgeCount);
   for(unsigned j=0; j<ii[shard].indexCount; j++) {
      for(int k=0; k<aList[j].nbrid_size(); k++) {
         assert(readEdges[shard][aList[j].nbrid(k)].dst == aList[j].key());
         es.push_back(&readEdges[shard][aList[j].nbrid(k)]);
      }
   }
   assert(es.size() == ii[shard].edgeCount);
   //if(es.size() == 0) continue;
   gettimeofday(&e, NULL);
   fprintf(stderr, "Sorting memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));

    fprintf(stderr, "Initialize sub-graph: %u\n", memoryShard);
    gettimeofday(&s, NULL);
    std::vector<Edge *> vertices; 
    unsigned vertexCounter = 0;
    IdType v = es[0]->dst;
    for(unsigned j=0; j<ii[shard].edgeCount;) {
        v = es[j]->dst;
        vertices.push_back(es[j++]); vertexCounter++;
        while(j<ii[shard].edgeCount && v==es[j]->dst) j++;  //TODO: edgecount should be buffer count
    }
    gettimeofday(&e, NULL);
    fprintf(stderr, "\nInitializing subgraph for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
    fprintf(stderr, "Num vertices: %zu, IC: %zu\n", vertices.size(), ii[shard].indexCount); //container.size());
    assert(vertices.size() == ii[shard].indexCount);
    
     // then load sliding shards
     /*fprintf(stderr, "\n NEXT -- TID: %d Loading sliding shards", tid);
      Edge edge;
      edge.src = -1;
      edge.dst = -1;
     for (unsigned i = 0; i<this->getCols(); ++i) {
        for (unsigned j = 0; j<readEdges[shard].size(); ++j) {
            slidingShard[i][j] = edge;
        }
     }
     gettimeofday(&s, NULL);
    // fprintf(stderr,"\nShard: %d, readEdges[shard].size(): %d ", shard, readEdges[shard].size()); 
     for(unsigned ss=0; ss<this->getCols(); ss++) {
        if(ss == shard) { 
           for(unsigned i=0; i<readEdges[shard].size(); i++){
              slidingShard[ss][i] = readEdges[shard][i];
           }
           continue;
        }
        efprintf(stderr, "Loading sliding shard: %u\n", ss);
        slidingShard[ss] = (Edge *) calloc(gcd[ss][shard].length, sizeof(Edge));
        std::vector<Edge> edges; edges.clear();

        for(unsigned i=gcd[ss][shard].startEdgeIndex; i<gcd[ss][shard].length; i++){
        //    edges[i] = readEdges[ss][i];
              slidingShard[ss][i] = readEdges[ss][i]; //TODO: I may not need edges vector now 
        }
        //slidingShard[ss] = edges; 
        assert(slidingShard[ss] != NULL);
     }
    gettimeofday(&e, NULL);
    fprintf(stderr, "Loading sliding shards for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
    */
    // then parallel-process shard
    bool changed = true;
    gettimeofday(&s, NULL);
                                                          
    //for(unsigned i=0; i<container.size(); ) { 
       fprintf(stderr, "TID: %d, Processing shard: %u \n", tid, memoryShard);
    for(unsigned i= 0; i<vertices.size(); ) { 
       IdType dst = vertices[i]->dst;
       long double sum = 0.0;

       // process neighbors
       unsigned dstStartIndex = i;
       //while(i<container.size() && dst == vertices[i]->dst){
       while(i<vertices.size() && dst == vertices[i]->dst){
            //if(nNbrs.at(vertices[i]->dst) > 0){
            if(vertices[i]->numNeighbors > 0){
              sum += (vertices[i]->rank / vertices[i]->numNeighbors);
            }
            i++;
       }
       double rank = (1-DAMPING_FACTOR) + (DAMPING_FACTOR*sum);
       //fprintf(stderr,"\nSHARD: %u, PR VERTICEs StartIndex : %u, ENDIndex: %u, Rank: %f ", memoryShard, dstStartIndex, i, rank);
       unsigned dstEndIndex = i;
       for(unsigned dstIndex = dstStartIndex; dstIndex<dstEndIndex; dstIndex++)
          vertices[dstIndex]->vRank = rank;

    }
     gettimeofday(&e, NULL);
     fprintf(stderr, "!!!!!Parallel processing of subgraph for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
     fprintf(stderr, "-------------------------%c\n", '-');
   pthread_barrier_wait(&(barWait)); 
    //gcd[tid] = { };
    readEdges[tid].clear();
    delete [] aList; //aList = NULL;
   //fprintf(stderr,"\nTID %d Deleted aList ------ " , tid);
    //diskWriteContainer(tid, ii[shard].lbEdgeCount, ii[shard].edgeCount, readEdges[tid].begin(), readEdges[tid].end());
   // totalRecords += readEdges[tid].size() ;
    return NULL;
  }

  void* updateReduceIter(const unsigned tid) {
    fprintf(stderr,"\nTID: %d Updating reduce Iteration ", tid);
    //free(slidingShard[tid]); slidingShard[tid] = NULL;
    //gcd[tid] = { };
   // readEdges[tid] = { };
   // free(readEdges[tid]); readEdges[tid] = NULL;
    //readEdges[tid].clear();

    edgeCounter = 0;
    ++iteration;
    if(iteration > this->getIterations()){
       fprintf(stderr, "\nTID: %d, Iteration: %d Complete ", tid, iteration);
       don = true;
       return NULL;
    }
    this->notDone(tid);
    //fprintf(stderr, "AfterReduce\n");
     // assign next to prev for the next iteration , copy to prev of all threads
     
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

  // Clear sliding shards:
    //free(slidingShard[tid]); slidingShard[tid] = NULL;
   // free(gcd[tid]); gcd[tid] = NULL;
   // free(readEdges[tid]); readEdges[tid] = NULL;
    readEdges[tid].clear();
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
hiDegree = 0;
#endif

int batchSize = atoi(argv[5]);
int kitems = atoi(argv[6]);

assert(batchSize > 0);
//pthread_mutex_init(&countTotal, NULL);

    //prev = new std::vector<double>[nreducers];
    //next = new std::vector<double>[nreducers];

gc.init(folderpath, gb, nmappers, nreducers, nvertices, hiDegree, batchSize, kitems, niterations);

gc.writeInit(nreducers, nvertices);
//slidingShard = (Edge **) calloc(numShards, sizeof(Edge *));
timeval s, e;
gettimeofday(&s, NULL);
//fprintf(stderr, "Reading edge lists\n");
//edgeLists = new EdgeList[nvertices];
//readEdgeLists(edgeLists);
gettimeofday(&e, NULL);
fprintf(stderr, "Reading edge lists took: %.3lf\n", tmDiff(s, e));

pthread_barrier_init(&barCompute, NULL, nreducers);
pthread_barrier_init(&barWait, NULL, nreducers);
ii = (IntervalInfo *) calloc(nreducers, sizeof(IntervalInfo));
assert(ii != NULL);


gcd = (IntervalLengths **) calloc(nreducers, sizeof(IntervalLengths *));
for(unsigned i=0; i<nreducers; i++) {
    gcd[i] = (IntervalLengths *) calloc(nreducers, sizeof(IntervalLengths));
//    readEdges[i] = (Edge *) calloc(kitems, sizeof(Edge));
    assert(gcd[i] != NULL);
    //assert(readEdges[i] != NULL);
}
   // readEdges = new Edge[nreducers]; // (Edge *) calloc( nreducers, sizeof(Edge) ); // separate for each thread

// for efficiently traversing sliding shards
unsigned *ssIndex = (unsigned *) calloc(nreducers, sizeof(unsigned)); assert(ssIndex != NULL);

double runTime = -getTimer();
gc.run();
runTime += getTimer();

std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
//delete[] edgeLists; 
free(ii); //free(gcd);
delete[] readEdges;
MallocExtension::instance()->ReleaseFreeMemory();
//std::cout << "Total words: " << countTotalWords << std::endl;
return 0;
}

