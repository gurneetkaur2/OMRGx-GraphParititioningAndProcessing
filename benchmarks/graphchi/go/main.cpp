//RUN: ./graphchi.bin ~/work/datasets/inputs/mdual 1 2 2 2000 100 258570 2 10000 test -DUSE_GOMR -DUSE_GRAPHCHI
#include "data.pb.h"
//#include "edgelist.pb.h"
//#include "adjacencyList.pb.h"
//#include "graph.h"

#define USE_NUMERICAL_HASH

#include "../../../engine/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>
#include <google/malloc_extension.h>

#define INIT_VAL 50000
#define SHARDSIZE_MB 1024
#define MAX_UINT (ULLONG_MAX)
int nvertices;
int nmappers;
int nReducers;
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
static pthread_barrier_t barWait;
static pthread_barrier_t barCompute;
static pthread_barrier_t barEdgeCuts;
static pthread_barrier_t barRefine;
static pthread_barrier_t barWriteInfo;
static pthread_barrier_t barAfterRefine;
static pthread_barrier_t barClear;
//static pthread_barrier_t barShutdown;
unsigned *ssIndex;

template <typename KeyType, typename ValueType>
class GraphChi : public MapReduce<KeyType, ValueType>
{
  static thread_local std::ofstream ofile;
  static thread_local IdType edgeCounter;
  static thread_local unsigned iteration;
  std::vector<pthread_mutex_t> locks;
  unsigned long long *totalPECuts;
  //unsigned long long *totalEdgesInPart;
  std::map<unsigned, unsigned>* dTable;
  std::map<unsigned, unsigned >* bndIndMap;
  std::vector<unsigned long>* where;
  std::vector<unsigned long> gWhere;
  std::vector<unsigned>* pIdsCompleted;
  std::vector<unsigned>* markMax;
  std::vector<unsigned>* markMin;
  std::set<unsigned>* fetchPIds;
  std::map<unsigned, unsigned> pIdStarted;
  unsigned totalCuts;

  public:
  std::vector<IdType> *prOutput;

  void* beforeMap(const unsigned tid) {
    unsigned nCols = this->getCols();
    efprintf(stderr, "TID: %d, nvert:  %d part: %d \n", tid, nvertices, tid % nCols);
    //totalEdgesInPart[tid%nCols] = 0;
    return NULL;
  }

  void writeInit(unsigned nCols, unsigned nVtces){
    where = new std::vector<unsigned long>[nCols]; 
    readEdges = new std::vector<Edge>[nCols];
    prOutput = new std::vector<IdType>[nCols];
    edgeCounter = 0;
    iteration = 0;
    efprintf(stderr,"\nInside writeINIT ************NOT EMPTY Vertices: %d  reducers: %d", nvertices, nReducers);
    for(unsigned i=0; i<nCols; i++){ 
      for(IdType j=0; j<=nVtces; j++) 
        where[i].push_back(INIT_VAL);
      //       prOutput[i][j] = -1;
    }
    for (unsigned j = 0; j<=nVtces; ++j) {
      gWhere.push_back(-1);
    }
  }

  unsigned setPartitionId(const unsigned tid)
  {
    unsigned nCols = this->getCols();
    //fprintf(stderr,"\nTID: %d writing to partition: %d " , tid, tid % nCols);
    return  -1; //tid % nCols;
  }

  void* map(const unsigned tid, const unsigned fileId, const std::string& input, const unsigned nbufferId, const unsigned hiDegree)
  {
     efprintf(stderr,"\nTID: %d Inside Map", tid);
    std::stringstream inputStream(input);
    unsigned to, token;
    std::vector<unsigned> from;
    unsigned currentShardCount = tid;

    inputStream >> to;

    while(inputStream >> token){
      from.push_back(token);
    }
    std::sort(from.begin(), from.end()); // using default comparator

    unsigned bufferId = hashKey(to) % this->getCols();
    unsigned part = tid % this->getCols();
     efprintf(stderr,"\nTID: %d TO: %d from size: %d buffer: %d part: %d ", tid, to, from.size(), bufferId, part);
    if(where[part][to] == INIT_VAL)
      where[part][to] = bufferId;

    for(unsigned i = 0; i < from.size(); ++i){
      unsigned whereFrom = hashKey(from[i]) % this->getCols();
      if(where[part][from[i]] == INIT_VAL)
        where[part][from[i]] = whereFrom;

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
     efprintf(stderr,"\nTID: %d EXIT Map", tid);
    return NULL;
  }

  void* initRefineStructs() {
    //initialize the locks to be used for synchronization
    // fprintf(stderr, "\n,INSIDE initRefine reducers: %d\n", nReducers);
    nReducers = this->getCols();
    for(unsigned i=0; i<nReducers; ++i) {
      pthread_mutex_t mutex;
      pthread_mutex_init(&mutex, NULL);
      locks.push_back(mutex);

      //refineMap[i] = 0;
    }
    //initialize the data structures required in refinement phase
    fetchPIds = new std::set<unsigned>[nReducers];
    dTable = new std::map<unsigned, unsigned>[nReducers];
    bndIndMap = new std::map<unsigned, unsigned >[nReducers];
    pIdsCompleted = new std::vector<unsigned>[nReducers];
    markMax = new std::vector<unsigned>[nReducers];
    markMin = new std::vector<unsigned>[nReducers];
    totalPECuts = new unsigned long long[nReducers];
    //totalEdgesInPart = new unsigned long long[nReducers];
    
  }


  void* beforeReduce(const unsigned tid) {
    //  assert(false);
    efprintf(stderr,"\nTID: %d BEFORE reduce ", tid);
    if(tid ==0){
      this->gCopy(tid, gWhere);
    }
    //fprintf(stderr,"\nTID: %d BEFORE RefineInit ", tid);
    refineInit(tid);
   //  fprintf(stderr,"\nTID: %d AFTER refineInit 2 ", tid);

    //fprintf(stderr,"\nInside before reduce ************ Container size: %d ECounter: %d edgeCounter: %d  ", this->getContainerSize(), ii[tid].ubEdgeCount, edgeCounter);
    //for (unsigned j = 0; j<=nvertices; ++j) {
    for (unsigned j = 0; j<ii[tid].ubEdgeCount; ++j) {
        Edge e;
        e.src = -1;
        e.dst = -1;
        e.rank = 1.0/nvertices;
        e.vRank = 1.0/nvertices;
        e.numNeighbors = 0;

      readEdges[tid].push_back(e);
      //        ssIndex[tid][j] = 0;
    }
    edgeCounter = 0;
    // fprintf(stderr,"\nTID: %d AFTER beforeReduce ", tid);
  }

  void refineInit(const unsigned tid) {
//     fprintf(stderr, "\nTID: %d,INSIDE RefineINIT reducers: %d fetchPIds[tid] size: %d\n", tid, nReducers, fetchPIds[tid].size());
    for (unsigned i = 0; i < nReducers; i++) {
      fetchPIds[tid].insert(i);  //partition ids - 0,1,2 .. 
      pIdsCompleted[tid].push_back(0);
    // fprintf(stderr, "\nTID: %d,RI PIds[tid] size(): %d  \n", tid, pIdsCompleted[tid].size());
    }
    //fprintf(stderr,"\ntTID %d, RefineINit fetchPID size: %d ----", tid, fetchPIds[tid].size());
    totalPECuts[tid] = 0;
    totalCuts = 0;
    // fprintf(stderr,"\nTID: %d AFTER refineInit ", tid);
  }

  void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) {
    //GO partition refinement
     // this barrier is needed when number of threads are more to prevent others from using gWhere before its updated
     pthread_barrier_wait(&(barRefine)); 
     efprintf(stderr,"\nTID: %d Inside reduce --- container size: %d ", tid, container.size());
    unsigned hipart = tid;
    for(auto it = fetchPIds[tid].begin(); it != fetchPIds[tid].end(); ++it) {
      //for(auto wherei=0; wherei < nReducers; wherei++){ //start TID loop
      unsigned whereMax = *it;
      if(whereMax == tid){
              efprintf(stderr, "\nTID: %d pIdsCompleted[%d][%d]: %d ", tid, hipart, whereMax, pIdsCompleted[hipart][whereMax]);
        pIdsCompleted[tid][*it] = 1;
        //    fprintf(stderr, "\nTID: %d going to NEXT Iter", tid);
        continue;
      }
      else if (hipart < nReducers && whereMax >= nReducers){
        pIdsCompleted[tid][*it] = 1; // true;
        continue;
      }
      else if ( hipart >= nReducers && whereMax < nReducers) {
        pIdsCompleted[tid][*it] = 1; //true;
        continue;
      }
      bool ret = this->checkPIDStarted(tid, hipart, whereMax);

      //fprintf(stderr, "\nFINAL TID: %d, WHEREMAX: %d, Ret: %d !!!", tid, whereMax, ret);
      //      fprintf(stderr, "\nTID: %d, refining with: %d, ret: %d ", tid, whereMax, ret);
      //fprintf(stderr, "\nTID: %d, Going to Compute EC  Container: %d", tid, container.size());
      ComputeBECut(tid, gWhere, bndIndMap[tid], container);
      // wait for other threads to compute edgecuts before calculating dvalues values
      // fprintf(stderr, "\nTID: %d, Before BarEDGECUTS ", tid);
           pthread_barrier_wait(&(barEdgeCuts)); 

      efprintf(stderr, "\nTID: %d, Computing Gain  Container: %d", tid, container.size());
      if(ret == true){
        int maxG = -1;
        do{
          maxG = computeGain(tid, hipart, whereMax, markMax[hipart], markMin[hipart], container);
          //  fprintf(stderr, "\nTID: %d, MaxG > 0: %d", tid, maxG);
        } while(maxG > 0);  //end do
      } // end if ret

      //  pthread_barrier_wait(&(barWriteInfo)); 
      if(ret == true) {
        //writePartInfo
       //   fprintf(stderr,"\nTID %d going to write part markMax size: %d ", tid, markMax[hipart].size());
        for(unsigned it=0; it<markMax[hipart].size(); it++){
          unsigned vtx1 = markMax[hipart].at(it);     //it->first;
          unsigned vtx2 = markMin[hipart].at(it);
          //        fprintf(stderr,"\nTID %d whereMax %d vtx1: %d vtx2: %d  ", tid, whereMax, vtx1, vtx2);
          pthread_mutex_lock(&locks[tid]);
          gWhere.at(vtx1) = whereMax;
          gWhere.at(vtx2) = hipart;
          pthread_mutex_unlock(&locks[tid]);
          // pthread_mutex_unlock(&locks[tid]);
      //              fprintf(stderr, "\nTID: %d, Change Where ", tid);
          //changewhere
          where[hipart].at(vtx1) = gWhere[vtx1];
          where[hipart].at(vtx2) = gWhere[vtx2];
          //    pthread_mutex_lock(&locks[tid]);
          where[whereMax].at(vtx1) = gWhere[vtx1];
          where[whereMax].at(vtx2) = gWhere[vtx2];
          //  pthread_mutex_unlock(&locks[tid]);
        }
      } // end of fetch tid loop
      pIdsCompleted[hipart][whereMax] = 1; //true;
    }
    pIdsCompleted[tid].clear();
    //fprintf(stderr,"\ntid: %d CLEARING MEM STRCUST ", tid);
     pthread_barrier_wait(&(barClear)); 
    clearMemorystructures(tid);
    
    efprintf(stderr,"\nShard: %d Waiting at BARRIER 1 ", tid);
     pthread_barrier_wait(&(barWriteInfo)); 
    //copy the partition information
    if(tid == 0)
      this->gCopy(tid, gWhere);

    //BuildGraphChi Metadata
    unsigned shard = tid;
    IdType indexCount = 0;
    //fprintf(stderr,"\nInside reduce ************ Container size: %d ", container.size());

    ii[shard].lbEdgeCount = edgeCounter;
    unsigned memoryShard = tid;  // current partition/tid is memoryShard
    assert(container.size() > 0);
    auto it_first = container.begin();
    ii[shard].lbIndex = it_first->first;

    IdType lbIndex = container.begin()->first;
    efprintf(stderr, "\nInitialize sub-graph: %u\n", memoryShard);
    timeval s, e;
    gettimeofday(&s, NULL);
    unsigned id = 0;
    //fprintf(stderr,"\nSHARD: %u, CONTAINER elements to SHIVEL LowerBound: %d ", tid, ii[shard].lbIndex);
    for(auto it = container.begin(); it != container.end(); it++){
       efprintf(stderr,"\nSHARD: %u, CONTAINER elements Key: %u, value size: %d, values: ", tid, it->first, it->second.size());
      for(int k=0; k<it->second.size(); k++) {
        Edge e = it->second[k];
        //  fprintf(stderr,"\nSHARD: %d ES elements dst: %zu src: %zu ", shard, it->second[k].dst, it->second[k].src);
          efprintf(stderr,"\n gwhere[dst]:%zu e.dst: %zu ", gWhere[e.dst], e.dst);
//        if(gWhere[e.dst] != INIT_VAL && gWhere[e.dst] == shard && gWhere[e.src] == shard)
          readEdges[shard][id++] = (it->second[k]);
        edgeCounter++;
      }
    efprintf(stderr, "\nTID: %d filled container\n", memoryShard);
      indexCount++;
      ii[shard].ubIndex = it->first;
    }
    std::vector<Edge *> vertices; 
    IdType v = it_first->second[0].dst;
    //fprintf(stderr,"\nSHARD: %u, CONTAINER elements to SHIVEL LowerBound: %d ", tid, it_first->second[0].dst);
    for(auto it = container.begin(); it != container.end(); it++){
      for(int k=0; k<it->second.size(); k++) {
        Edge e = it->second[k];
        v = e.dst;
        vertices.push_back(&e);
        while(k<it->second.size() && v==e.dst) k++;  //TODO: edgecount should be buffer count
      }
    }
    gettimeofday(&e, NULL);
    //fprintf(stderr, "\nInitializing subgraph for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
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
      //fprintf(stderr, "\nProcessing shard: %u, interval %u: StartVertex: %zu, EndVertex: %zu, SEdgeIndex: %zu, readEdges SRC: %u\n", tid, j, ii[j].lbIndex, ii[j].ubIndex, gcd[tid][j].startEdgeIndex, readEdges[shard][count].src);
      while(readEdges[shard][count].src >= ii[j].lbIndex && readEdges[shard][count].src < ii[j].ubIndex && count < ii[tid].edgeCount) {
        count++; // skip over
        edgeGCounter++;
      }
      efprintf(stderr, "After interval: %zu (prev: %zu)\n", readEdges[shard][count].src, readEdges[shard][count-1].src);
      gcd[tid][j].endEdgeIndex = edgeGCounter;
      gcd[tid][j].length = gcd[tid][j].endEdgeIndex - gcd[tid][j].startEdgeIndex;
      efprintf(stderr, "Shard: %u, Interval: %u, Start: %zu, End: %zu, Length: %zu\n", tid, j, gcd[tid][j].startEdgeIndex, gcd[tid][j].endEdgeIndex, gcd[tid][j].length);
    }

    efprintf(stderr, "%c---------------------------------\n", '-');
    // ***************** Done building meta data *****************
    efprintf(stderr, "Sorting memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));

    efprintf(stderr, "Num vertices: %zu, IC: %zu\n", vertices.size(), ii[shard].indexCount); //container.size());
    // then parallel-process shard -- calculate Pagerank
    gettimeofday(&s, NULL);

    efprintf(stderr, "TID: %d, PR Processing shard: %u \n", tid, memoryShard);
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
      double rank = (1-DAMPING_FACTOR) + (DAMPING_FACTOR*sum);
      efprintf(stderr,"\nSHARD: %u, PR VERTICEs StartIndex : %u, ENDIndex: %u, Rank: %f ", memoryShard, dstStartIndex, i, rank);
      unsigned dstEndIndex = i;
      for(unsigned dstIndex = dstStartIndex; dstIndex<dstEndIndex; dstIndex++){
        vertices[dstIndex]->vRank = rank;
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
            readEdges[ss][ssIndex[ss]++].rank = vertices[i]->vRank;
          }
          break; // proceed to next sliding shard
        }
      }
    }
    gettimeofday(&e, NULL);
    efprintf(stderr, "!!!!!Parallel processing of subgraph for memory shard %u took: %.3lf\n", memoryShard, tmDiff(s, e));
    efprintf(stderr, "-------------------------%c\n", '-');
    efprintf(stderr,"\n-------------TID %d Waiting at barrier ------ " , tid);
    pthread_barrier_wait(&(barWait)); 
    readEdges[tid].clear();
    //diskWriteContainer(tid, ii[shard].lbEdgeCount, ii[shard].edgeCount, readEdges[tid].begin(), readEdges[tid].end());
    // totalRecords += readEdges[tid].size() ;
    return NULL;
    }

    void* updateReduceIter(const unsigned tid) {
      efprintf(stderr,"\nTID: %d Updating reduce Iteration ", tid);
  //    pthread_barrier_wait(&(barCompute));
      
      efprintf(stderr, "\nTID: %d, Going to REFINEINIT ", tid);
      refineInit(tid);
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
      efprintf(stderr,"\nTID: %d After Reduce ", tid);
    //  fprintf(stderr, "\nthread %u waiting for others to finish Refine\n", tid);
      pthread_barrier_wait(&(barAfterRefine));
      
     // if(tid == 0)
      //  this->gCopy(tid, gWhere);

      //refineInit(tid);
      //clearMemorystructures(tid);
      // readEdges[tid].clear();
      //    if(!outputPrefix.empty()){
      //    std::string fileName = outputPrefix + std::to_string(tid);
      //   printParts(tid, fileName.c_str());
      //}
      if(tid == 0){
        clearRefineStructures();
      }
      //fprintf(stderr,"\nTID: %d GOING to EXIT ", tid);
      return NULL;
    }


    //-------------------
    void ComputeBECut(const unsigned tid, const std::vector<unsigned long>& where, InMemTable& bndind, const InMemoryContainer<KeyType, ValueType>& inMemMap) {
      IdType dst;
      std::vector<unsigned> bndvert;
     // fprintf(stderr, "\nTID: %d, Computing EdgeCuts COntainer Size: %d ", tid, inMemMap.size());

      for (InMemoryContainerConstIterator<KeyType, ValueType> it = inMemMap.begin(); it != inMemMap.end(); ++it) {
        dst = it->first;
           //  fprintf(stderr, "\nTID: %d, Computing EC DST: %d ", tid, dst);
        for(int k=0; k<it->second.size(); k++) {
        //for (std::vector<unsigned>::const_iterator vit = it->second.begin(); vit != it->second.end(); ++vit){
          bndvert.push_back(it->second[k].src);
        }
        //  refineMap[tid] = key; // assuming each thread writes at different index
        unsigned nbrs = bndvert.size();
        int countTopK=0;
        int costE = 0;  //count external cost of edge
        //compute the number of edges cut for every key-values pair in the map
        for(auto it = bndvert.begin(); it != bndvert.end(); ++it) {
          IdType src = *it;
          if( where[src] != INIT_VAL && where[src] != where[dst] ) {
            //  fprintf(stderr,"\nTID: %d, where[%d]: %d != where[%d]: %d ", tid, src, where[src], dst, where[dst]);
            totalPECuts[tid]++;
            costE++;
            bndind[src]++; // = costE ;     
          }
        }
        bndvert.clear();

        //calculate d-values
        // fprintf(stderr, "\nTID: %d, Calculate DVals ", tid);
        unsigned costI = nbrs - costE;
        unsigned ddst = costE - costI;      // External - Internal cost
        dTable[tid][dst] = ddst;
      } //end Compute edgecuts main Loop
      //   pthread_barrier_wait(&(barCompute)); //wait for the dvalues from all the threads to be populated
      // fprintf(stderr, "\nTID: %d,Finished Computing EdgeCuts ****** ", tid);
    }


    //-------------------
    unsigned computeGain(const unsigned tid, const unsigned hipart, const unsigned whereMax, std::vector<unsigned>& markMax, std::vector<unsigned>& markMin, const InMemoryContainer<KeyType, ValueType>& inMemMap){
      int maxG = 0;
      int maxvtx = -1, minvtx = -1;
      // fprintf(stderr, "\nTID: %d, Computing GAIN ", tid);
      for (auto it = dTable[hipart].begin(); it != dTable[hipart].end(); ++it) {
         //fprintf(stderr, "\nTId %d dTable hipart Begin: %d, dTable Size %d ----- " , tid,dTable[hipart].begin(), dTable[hipart].size());
        unsigned dst = it->first; 
        std::vector<unsigned>::iterator it_max = std::find (markMax.begin(), markMax.end(), dst);
        if(it_max != markMax.end()){
          continue;
        }
        else{
          auto it_map = inMemMap.find(dst);
          if(it_map != inMemMap.end()){
            if(dTable[whereMax].size() > 0 ){

              for (auto it_hi = dTable[whereMax].begin(); it_hi != dTable[whereMax].end(); ++it_hi) {
                unsigned src = it_hi->first;
       //         fprintf(stderr, "\nTID: %d, Computing Gain src: %d, dest: %d ", tid, src, dst);
                bool connect = 0;
                unsigned ddst = dTable[hipart][dst];
                unsigned dsrc = dTable[whereMax][src];
                if(dsrc >= 0 || ddst >= 0){
                  std::vector<unsigned>::iterator it_min = std::find (markMin.begin(), markMin.end(), src);
                  if(it_min != markMin.end()){     // check if the dst is in the masked vertices
                    //              fprintf(stderr, "\nTID: %d, Computing Gain DST: %d is masked ", tid, dst);
                    continue;
                  }
                  else{
                    for(unsigned k=0; k<it_map->second.size(); k++) {
                      if(it_map->second[k].src == src){
                    //if (std::find(it_map->second.begin(), it_map->second.end(), src) == it_map->second.end()){
                      	connect = 1;
			break;
                      }
                      else
                        connect = 0;
                     }
     //               fprintf(stderr, "\nTID: %d, Computing Gain SRC: %d is boundary vertex ", tid, src);
                    int currGain = -1;
                    if(!connect)
                      currGain = dsrc + ddst;
                    else
                      currGain = dsrc + ddst - 2;
                    if(currGain > maxG){                                                                                            maxG = currGain;
                      maxvtx = dst;
                      minvtx = src;
                    }
                  }
                }
                else
                  continue;
              } // end refinemap for loop
            } // end check for masked vertices
          }
        }
      }
      //}
      if(maxvtx != -1 && minvtx != -1){
        //fprintf(stderr, "\nTID: %d, MASKING src: %d, dst: %d, Gain: %d ", tid, maxvtx, minvtx, maxG);
        markMax.push_back(maxvtx);
        markMin.push_back(minvtx);
        return maxG;
      }
      return -1;
  }



  //-------------------
/*  unsigned countTotalPECut(const unsigned tid) {
    //totalCuts = 0;
    for(unsigned i=0; i<nReducers; i++){
      //for(auto i=fetchPIds.begin(); i != fetchPIds.end(); ++i){
      //   fprintf(stderr,"\nTID: %d TotalPECUTs: %d ", tid, totalPECuts[i]);
      totalCuts += totalPECuts[i];
    }

    return (totalCuts/2);
    }

*/
    //-------------------
    void clearRefineStructures(){
      
      pthread_barrier_destroy(&barRefine);
      pthread_barrier_destroy(&barCompute);
      pthread_barrier_destroy(&barWait);
      pthread_barrier_destroy(&barEdgeCuts);
      pthread_barrier_destroy(&barWriteInfo);
      pthread_barrier_destroy(&barClear);
    //  pthread_barrier_destroy(&barShutdown);
      pthread_barrier_destroy(&barAfterRefine);

      //fprintf(stderr,"\nCLEAR REfine Structs " );
      //  dTable->clear();
     // markMax->clear();
     // markMin->clear();
      fetchPIds->clear();
      pIdsCompleted->clear();
      delete[] totalPECuts;
      //delete[] totalEdgesInPart;
      delete[] bndIndMap;
      delete[] dTable;
      delete[] markMax;
      delete[] markMin;
      delete[] pIdsCompleted;
      delete[] where;
      delete[] fetchPIds;
    }


    //-------------------
    void clearMemorystructures(const unsigned tid){

      bndIndMap[tid].clear();
      markMax[tid].clear();
      markMin[tid].clear();
      dTable[tid].clear();
      // pIdsCompleted[tid].clear();

    }

    //-----------------------------------
    void gCopy(const unsigned tid, std::vector<unsigned long>& gWhere){
      bool first = 1;
    fprintf(stderr,"\nTID: %d Inside gCopy ", tid);
      for(unsigned i=0; i<nReducers; ++i){
        for(unsigned j=0; j<=nvertices; ++j){
          if(first){  // All Values of first thread will be copied
            //         fprintf(stderr,"\nwhere[%d][%d]: %d,", i, j, where[i][j]);
            gWhere[j] = where[i][j];
          }
          else {
            if(where[i][j] != INIT_VAL){
              gWhere[j] = where[i][j];
            }
          }
          //             fprintf(stderr,"\nGWHERE[%d]: %d", j, gWhere[j]);
        }
        first = 0;
      }
    //fprintf(stderr,"\nTID: %d Exit gCopy ", tid);
    }

    //-------------------
    bool checkPIDStarted(const unsigned tid, const unsigned hipart, const unsigned whereMax) {
      bool ret = false;
      pthread_mutex_lock(&locks[tid]);
      auto it_hi = pIdStarted.find(hipart);
      auto it_wh = pIdStarted.find(whereMax);
      if (it_hi != pIdStarted.end() || it_wh != pIdStarted.end()){   //that means key is present
        unsigned key1 = it_hi->first;
        unsigned val1 = it_hi->second;
        unsigned key2 = it_wh->first;
        unsigned val2 = it_wh->second;
        if((key1 == hipart && val1 == whereMax) || (key2 == whereMax && val2 == hipart)){
          //fprintf(stderr,"\n TID %d hipart %d is already present\n", tid, hipart);
          ret = false;
        }
        else
          ret = true;  //key present with a diff value
      }
      else{
        pIdStarted.emplace(hipart, whereMax);
        ret = true;     // this will compute gain
      }

      pthread_mutex_unlock(&locks[tid]);
      return ret;
    }


    //-------------------
   /* void printParts(const unsigned tid, std::string fileName) {
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
      std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems> <nvertices> <optional - iters> <hidegree> <optional - partition output prefix>" << std::endl;

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

    nReducers = nreducers;
    //slidingShard = (Edge **) calloc(numShards, sizeof(Edge *));
    //initialize barriers
    pthread_barrier_init(&barRefine, NULL, nReducers);
    pthread_barrier_init(&barCompute, NULL, nReducers);
    pthread_barrier_init(&barEdgeCuts, NULL, nReducers);
    pthread_barrier_init(&barWriteInfo, NULL, nReducers);
    pthread_barrier_init(&barClear, NULL, nReducers);
//    pthread_barrier_init(&barShutdown, NULL, nReducers);
    pthread_barrier_init(&barAfterRefine, NULL, nReducers);
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

    gc.writeInit(nreducers, nvertices);
    gc.init(folderpath, gb, nmappers, nreducers, nvertices, hiDegree, batchSize, kitems, niterations);

    gc.initRefineStructs();
 
    double runTime = -getTimer();
    gc.run();
    runTime += getTimer();
    
    std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
    free(ii); free(gcd); free(ssIndex); //free(prOutput);
    delete[] readEdges;
    MallocExtension::instance()->ReleaseFreeMemory();
    return 0;
  }

