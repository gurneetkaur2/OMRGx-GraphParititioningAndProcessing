#include "data.pb.h"

#define USE_NUMERICAL_HASH

#include "../../engine/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <math.h>
#include <algorithm>
#include <ctime>
#include <cstdlib>
#include <chrono>
#include <google/malloc_extension.h>
//#include <bits/stdc++.h>
#include <random>       // std::default_random_engine

#define UNMATCHED  -1
#define COARSEN_FRACTION  0.85  /* Node reduction between succesive coarsening levels */
#define MAX_UINT (ULLONG_MAX)
#define INIT_VAL 5000
static int nvertices;
static int nmappers;
static int nparts;

//EdgeList* edgeLists = NULL;
IdType totalEdges = 0;
unsigned long long numEdges = 0;

//Meta data for Coarsen Graph processing
typedef struct __partitionInfo {
  IdType lbEdgeCount;  //startkeyIndex
  IdType ubEdgeCount;  //endkeyIndex
  IdType edgeCount; // dont need this

  IdType lbIndex;  //startkey
  IdType ubIndex;  //endKey
  IdType indexCount; // total vertices
  unsigned levels;
} PartitionInfo;
PartitionInfo *ii = NULL;

//------------------------------------------------------------------
typedef struct __coarsenGraphLengths {
    IdType startIndex;
    IdType endIndex;
    IdType cnvtxs;
    IdType cnedges;
    IdType indexCount;
} CoarsenLengths;

CoarsenLengths **gcd; 

//------------------------------------------------------------------

const long double DAMPING_FACTOR = 0.85; // Google's recommendation in original paper.
const long double TOLERANCE = (1e-1);
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through
__thread unsigned iteration = 0;
__thread IdType totalRecords = 0;
__thread IdType edgeCounter = 0;
__thread IdType *match = NULL;
__thread IdType *perm = NULL;
__thread IdType *cmap = NULL;

//pagerank for previous and next iteration
  std::map<IdType, std::vector<IdType> >* readEdges;
static pthread_barrier_t barCompute;
static pthread_barrier_t barWait;

template <typename KeyType, typename ValueType>
class MtMetis : public MapReduce<KeyType, ValueType>
{
  std::vector<pthread_mutex_t> locks;
  std::vector<unsigned long>* where; // = (nvertices, -1);
  std::vector<unsigned long> gWhere; 
  std::map<unsigned, unsigned>* dTable;
  std::map<unsigned, unsigned >* bndIndMap;
  std::vector<unsigned>* markMax;
  std::vector<unsigned>* markMin;
  unsigned long long *totalPECuts;

  public:

  void* beforeMap(const unsigned tid) {
    unsigned nCols = this->getCols();
    fprintf(stderr, "TID: %d, nvert:  %d part: %d \n", tid, nvertices, tid % nCols);
    return NULL;
  }

  void writeInit(unsigned nCols, unsigned nVtces){
    readEdges = new std::map<IdType, std::vector<IdType> >[nCols];
    where = new std::vector<unsigned long>[nparts];
    dTable = new std::map<unsigned, unsigned>[nparts];
    bndIndMap = new std::map<unsigned, unsigned >[nparts]; 
    markMax = new std::vector<unsigned>[nparts];
    markMin = new std::vector<unsigned>[nparts];
    totalPECuts = new unsigned long long[nparts];

    for (unsigned i = 0; i<nparts; ++i) {
        for (unsigned j = 0; j<=nVtces; ++j) {
           where[i].push_back(INIT_VAL);
        }
        totalPECuts[i] = 0;
    }
    for (unsigned j = 0; j<=nVtces; ++j) {
       gWhere.push_back(-1);
    }
    for(unsigned i=0; i<this->getCols(); ++i) {
       pthread_mutex_t mutex;
       pthread_mutex_init(&mutex, NULL);
       locks.push_back(mutex);
   }
  }

unsigned setPartitionId(const unsigned tid)
  {
    unsigned nCols = this->getCols();
    //fprintf(stderr,"\nTID: %d writing to partition: %d " , tid, tid % nCols);
   return tid % nCols;
  }

  void* map(const unsigned tid, const unsigned fileId, const std::string& input, const unsigned nbufferId, const unsigned hiDegree)
  {
  // fprintf(stderr,"\nTID: %d Inside Map", tid);
    std::stringstream inputStream(input);
    unsigned to, token;
    std::vector<unsigned> from;

    inputStream >> to;

    while(inputStream >> token){
      from.push_back(token);
    }
    std::sort(from.begin(), from.end()); // using default comparator

    for(unsigned i = 0; i < from.size(); ++i){
 //     fprintf(stderr,"\tTID: %d, src: %zu, dst: %zu, vrank: %f, rank: %f nNbrs: %u", tid, e.src, e.dst, e.vRank, e.rank, e.numNeighbors);
      this->writeBuf(tid, to, from[i], nbufferId, 0);
    }

    return NULL;
  }

  void* beforeReduce(const unsigned tid) {
  //  assert(false);
      //fprintf(stderr,"\nInside writeINIT ************ Cols: %d, Vertices: %d ", nCols, nVtces);
  // for (unsigned i = 0; i<=this->getContainerSize(); ++i) {
    //  for (unsigned j = 0; j<=this->getContainerSize(); ++j) {
      //    readEdges[tid][i].push_back(1);
       // }
  // }
    match = (IdType *) calloc(nvertices, sizeof(IdType));
    //match = (int *) calloc(nvertices, sizeof(int));
    perm = (IdType *) calloc(nvertices, sizeof(IdType));
    cmap = (IdType *) calloc(nvertices, sizeof(IdType));
 
    for(unsigned i=0; i<nvertices; i++){
       match[i] = -1;
       //fprintf(stderr,"\ntid: %d, match[i]: %d, unmatched: %d ", tid, match[i], UNMATCHED);
       perm[i] = i;
       cmap[i] = -1; //TODO: it may give trouble if the vertices are not from beginning
    }
   unsigned seed = (nvertices*tid+1) % nvertices; 
   //(std::chrono::system_clock::now().time_since_epoch().count()) % nvertices;
   // Shuffling our array
   std::shuffle(perm, perm + nvertices, std::default_random_engine(seed));
  }

  void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) {
    
    unsigned part = tid;
    IdType indexCount = 0;

    ii[part].lbEdgeCount = edgeCounter;
    assert(container.size() > 0);
    auto it_first = container.begin();
    ii[part].lbIndex = it_first->first;
    
    IdType lbIndex = container.begin()->first;
    fprintf(stderr, "Initialize sub-graph: %u\n", part);
    timeval s, e;
    gettimeofday(&s, NULL);
    fprintf(stderr,"\nPART: %u, CONTAINER elements LowerBound: %d ", tid, ii[part].lbIndex);
    for(auto it = container.begin(); it != container.end(); it++){
        readEdges[part][it->first];
 //         fprintf(stderr,"\nPART: %u, CONTAINER elements Key: %u, values: ", tid, it->first);
      for(int k=0; k<it->second.size(); k++) {
           readEdges[part][it->first].push_back(it->second[k]);
   //       fprintf(stderr,"\t %ul ", it->second[k]);
        //fprintf(stderr,"\nPART: %d readEdges elements key: %zu value: %zu ", part, it->second[k].dst, it->second[k].src);
         edgeCounter++;
        }
        indexCount++;
        ii[part].ubIndex = it->first;
    }
    gettimeofday(&e, NULL);
    assert(readEdges != NULL);
   ii[part].indexCount = indexCount;
   ii[part].ubEdgeCount = edgeCounter;
   ii[part].edgeCount = ii[part].ubEdgeCount - ii[part].lbEdgeCount;
    fprintf(stderr, "\nInitializing subgraph for part %u took: %.3lf readEdges Size: %u\n", part, tmDiff(s, e), readEdges[tid].size());
   //coarsest graph
   std::map<KeyType, std::vector<ValueType>> last_cgraph; 
   last_cgraph = coarsen(tid); //, readEdges[tid]);

   initpartition(tid, last_cgraph);
 
   std::map<KeyType, std::vector<ValueType>> cgraph(last_cgraph);
   unsigned level = ii[tid].levels;
   do{
      fprintf(stderr, "\n*****Tid: %d Refining LEVEL: %u *****", tid, level);
      // refining the coarsest level graph which is in memory and fetching the finer levels from disk later
      refinepartition(tid, cgraph);
      level--;
      //project partition
      //fetch the finer level graph
      if(level >0){
        IdType nItems = gcd[tid][level].cnvtxs;
  fprintf(stderr,"\nTID: %d, BEFORE Reading cgraph size: %u, startKey: %u ", tid, nItems, gcd[tid][level].startIndex);
        cgraph = this->diskReadContainer(tid, gcd[tid][level].startIndex, nItems);
  fprintf(stderr,"\nTID: %d, After Reading cgraph size: %u, startKey: %u ", tid, cgraph.size(), gcd[tid][level].startIndex);
      //project partition to finer level 
        IdType k;
      //for(IdType i=0; i<gcd[tid][level].cnvtxs; i++){
        for(auto fit = cgraph.begin(); fit != cgraph.end(); fit++){
          k = cmap[fit->first];
          where[fit->first] = where[k];
        }
      }
    } while(level > 0);

   assert(false);
   // store the coarsened graph on disk
   
    
    readEdges[tid].clear();
    return NULL;
  }

  std::map<KeyType, std::vector<ValueType>> coarsen(const unsigned tid){ //, std::map<KeyType, std::vector<ValueType>> container){
  // std::map<KeyType, std::vector<ValueType>> container(readEdges[tid]);
   std::map<KeyType, std::vector<ValueType>> cgraph(readEdges[tid]);
  fprintf(stderr,"\nTID: %d, Coarsening graph container size: %u ", tid, cgraph.size());

   unsigned nvtxs = ii[tid].indexCount;
   unsigned nedges = ii[tid].edgeCount;
   IdType edgeCCounter = ii[tid].lbEdgeCount; // start key
   unsigned level = 0;
   unsigned CoarsenTo = 4; //std::max((nvtxs)/(20*log2(nparts)), 30*(nparts));
//   for(unsigned j=0; j < level; j++){
    gcd[tid][level].startIndex = 0;
   do{
       cgraph = MATCH_RM(tid, cgraph, nvtxs, nedges, level);
       //  number of vertices in coarsened graph
       gcd[tid][level].indexCount = gcd[tid][level].endIndex - gcd[tid][level].startIndex;
       IdType nItems = gcd[tid][level].cnvtxs;
      // it = std::next(readEdges[tid].begin(), nItems);
  fprintf(stderr,"\nTID: %d, BEFORE writing cgraph size: %u, startKey: %u ", tid, cgraph.size(), gcd[tid][level].startIndex);
       this->diskWriteContainer(tid, gcd[tid][level].startIndex, nItems, cgraph.begin(), cgraph.end());
       
       /*InMemoryContainer<KeyType, ValueType> container = this->diskReadContainer(tid, gcd[tid][level].startIndex, nItems);
  fprintf(stderr,"\nTID: %d, After Reading cgraph size: %u, startKey: %u ", tid, container.size(), gcd[tid][level].startIndex); */
       level++;
    gcd[tid][level].startIndex += nItems;
    ii[tid].levels = level;
   }
      while (gcd[tid][level].cnvtxs > CoarsenTo &&
             gcd[tid][level].cnvtxs < COARSEN_FRACTION*gcd[tid][level-1].indexCount && //graph->finer->nvtxs &&
             nedges > (gcd[tid][level].cnvtxs)/2);  

    // return last coarses graph
    return cgraph;
  }

std::map<KeyType, std::vector<ValueType>> MATCH_RM(const unsigned tid, std::map<KeyType, std::vector<ValueType>> container, IdType nvtxs, IdType nedges, unsigned level){
  IdType i, j, pi, cnvtxs, last_unmatched, maxidx;
  //IdType *match, *perm, *cmap;
 // IdType *cmap;
  size_t nunmatched=0;
  std::map<KeyType, std::vector<ValueType>> cgraph;

// for (cnvtxs=0, last_unmatched=0, pi=0; pi<nvtxs; pi++) {
 cnvtxs=0, last_unmatched=0; pi=0;// pi<nvtxs; pi++) {
 for(auto fit = container.begin(); fit != container.end(); fit++){
      i = perm[fit->first] % container.size();  // i = 4
 // fprintf(stderr,"\nTID: %d, Picked random vertex: %u, i: %u , match[i]: %d, Actual element: %u ", tid, perm[fit->first] % container.size(), i, match[i], fit->first);

    if (match[i] == UNMATCHED) {  /* Unmatched */
       //fprintf(stderr,"\nTID: %d i: %d UNMATCHED ", tid, i);
       maxidx = i; // = 4
       auto it = container.find(i);
       if(it != container.end()){
//         srand(time(NULL));
        // last_unmatched = rand() % it->second.size();
          //last_unmatched = std::max(pi, last_unmatched)+1;
          //last_unmatched = 0;
          for (; last_unmatched<it->second.size(); last_unmatched++) {
               j = it->second[last_unmatched]; // pick a random adjacent vertex of i
                 //make sure adj vertex is within this container && check if it is unmatched
               if (match[j] == UNMATCHED) {   
     //fprintf(stderr,"\nTID: %d, j: %u matched: %d  ", tid, j, match[j]);
                  maxidx = j; //
                 // pi++;
                  //last_unmatched = pi; 
                  break;
               }
            }
       }

       if (maxidx != UNMATCHED) {
          cmap[i]  = cmap[maxidx] = cnvtxs++;
          match[i] = maxidx;
          match[maxidx] = i;
//     fprintf(stderr,"\nTID: %d, i: %u, maxidx: %u cmap: %u match: %u ", tid, i, maxidx, cmap[i], match[i]);
       }
    }
 }
 /* match the final unmatched vertices with themselves and reorder the vertices 
      of the coarse graph for memory-friendly contraction */
 //for (cnvtxs=0, i=0; i<nvtxs; i++) {
   cnvtxs = 0;
 for(auto fit = container.begin(); fit != container.end(); fit++){
     if (match[fit->first] == UNMATCHED) {
         match[fit->first] = fit->first;
         cmap[fit->first]  = cnvtxs++;
     }
     else {
         if (fit->first <= match[fit->first])
            cmap[fit->first] = cmap[match[fit->first]] = cnvtxs++;
     }
     //fprintf(stderr,"\nTID: %d, FInal container element i: %u cmap: %u match: %u ", tid, fit->first, cmap[fit->first], match[fit->first]);
  }
 cgraph = CreateCoarseGraph(tid, container, cnvtxs, cmap, level);
 fprintf(stderr,"\nTID: %d coarser vertices: %u ", tid, cnvtxs);
 gcd[tid][level].cnvtxs = cgraph.size();
 return cgraph;
}
 
 std::map<KeyType, std::vector<ValueType>> CreateCoarseGraph(const unsigned tid, std::map<KeyType, std::vector<ValueType>> container, IdType cnvtxs, IdType* cmap, unsigned level){
 fprintf(stderr,"\nTID: %d inside createCoarse graph size: %u ", tid, container.size());
   IdType *htable;
   std::map<KeyType, std::vector<ValueType>> cgraph;
   IdType k, j, v, u, m, nedges, cnedges;
     
   htable = (IdType *) calloc(cnvtxs, sizeof(IdType));
   for(IdType i=0; i<container.size(); i++){
       htable[i] = -1;
   }

   cnedges = 0;
//   fprintf(stderr,"\nTID: %d CreateCoarse graph Container size: %u ", tid, container.size());
   for(auto it = container.begin(); it !=container.end(); it++){
      v = it->first;
  // fprintf(stderr,"\nTID: %d CreateCoarse First: %u match[v]: %u ", tid, v, match[v]);
      if ((u = match[v]) < v)
              continue;
      
      nedges = 0;
      for(unsigned vit=0; vit <it->second.size(); vit++){  //adjncy of v
          k = cmap[it->second[vit]]; // num of cnvtxs (coarsened vertices) of this vertex
   //fprintf(stderr,"\nTID: %d next Iter vit: %u k: %u ", tid, it->second[vit], k);
          if(k == UNMATCHED || k >ii[tid].ubIndex)
            continue;
         if(m = htable[k] == -1)
           htable[k] = nedges++;    
           cgraph[it->first].push_back(it->second[vit]);
  //   fprintf(stderr,"\nTID: %d, element: %d k: %u, nedges: %u match: %u htable: %u ", tid,it->second[vit], k, nedges, match[it->second[vit]], htable[k]);
      }

   //fprintf(stderr,"\nTID: %d before v!=u ", tid);
      if(v != u && u < ii[tid].ubIndex){
        auto uit = container.find(u);
        if(uit != container.end()){
          for(unsigned vit=0; vit <uit->second.size(); vit++){  //adjncy of u
              k = cmap[uit->second[vit]]; // num of cnvtxs (coarsened vertices) of this vertex
          //if(k == UNMATCHED)
          if(k == UNMATCHED || k >ii[tid].ubIndex)
            continue;
              if(m = htable[k] == -1)
              htable[k] = nedges++;    
           cgraph[it->first].push_back(uit->second[vit]); // edges will aggregate for matching vertices
    // fprintf(stderr,"\nTID: %d, v!=u element: %d k: %u, nedges: %u match: %u htable: %u ", tid,uit->second[vit], k, nedges, match[uit->second[vit]], htable[k]);
          }
        }
   //fprintf(stderr,"\nTID: %d before zero out cnvtxs: %u ", tid, cnvtxs);
       if ((htable[cnvtxs]) != -1) {
           htable[cnvtxs] = -1;
       }
     }
      /* Zero out the htable */
      for (j=0; j<nedges; j++)
           htable[j] = -1;
     if(gcd[tid][level].endIndex < it->first) 
       gcd[tid][level].endIndex = it->first;

     cnedges         += nedges;
   }
    fprintf(stderr,"\nTID: %d cnedges: %u ", tid, cnedges);
   gcd[tid][level].cnedges = cnedges;
   return cgraph;
 }

  void initpartition(const unsigned tid, std::map<KeyType, std::vector<ValueType>> cgraph){
    srand(time(NULL));

    fprintf(stderr,"\nTID: %d init partition cgraph: %u ", tid, cgraph.size());
    for (auto it= cgraph.begin(); it != cgraph.end(); it++){
       IdType to = it->first;
       //unsigned bufferId = tid % nparts; //hashKey(to) % this->getCols();
       unsigned part = rand() % nparts;
       //unsigned part = tid % this->getCols();
      if(where[part].at(to) == INIT_VAL)
         where[part].at(to) = part; //bufferId;
      
      for(auto vit=0; vit <it->second.size(); vit++){
        IdType from = it->second[vit];
     //   unsigned whereFrom = hashKey(from) % this->getCols();
         if(where[part].at(from) == INIT_VAL)
           where[part].at(from) = part; //bufferId; 
      }
   // fprintf(stderr,"\nTID: %d init partition key: %u where: %u ", tid, to, where[part].at(to));
   }
    if(tid ==0){
      this->gCopy(tid, gWhere);
    }           
 }

 void refinepartition(const unsigned tid, const std::map<KeyType, std::vector<ValueType>> partition){
    fprintf(stderr,"\nTID: %d Refine partition Partition: %u ", tid, partition.size());
   ComputeBECut(tid, gWhere, bndIndMap[tid], partition);
   for(unsigned i=0; i < nparts; i++){
     unsigned hipart = i;
      for(unsigned j=i+1; j < nparts; j++){
        unsigned whereMax = j;
        int maxG = -1;
        do{        
          maxG = ComputeGain(tid, hipart, whereMax, markMax[hipart], markMin[hipart], partition);
        } while(maxG > 0);
        for(unsigned it=0; it<markMax[hipart].size(); it++){
           unsigned vtx1 = markMax[hipart].at(it);     //it->first;
           unsigned vtx2 = markMin[hipart].at(it);
 //        fprintf(stderr,"\nTID %d whereMax %d vtx1: %d vtx2: %d  ", tid, whereMax, vtx1, vtx2);
           pthread_mutex_lock(&locks[tid]);
           gWhere.at(vtx1) = whereMax;
           gWhere.at(vtx2) = hipart;
           pthread_mutex_unlock(&locks[tid]);
// below assignment will not coincide with other threads as all threads will be working on different vtces - no locks
           where[hipart].at(vtx1) = gWhere[vtx1];
           where[hipart].at(vtx2) = gWhere[vtx2];
           where[whereMax].at(vtx1) = gWhere[vtx1];
           where[whereMax].at(vtx2) = gWhere[vtx2];
        }
      }
   }
 }

void ComputeBECut(const unsigned tid, const std::vector<unsigned long>& gwhere, InMemTable& bndind, const InMemoryContainer<KeyType, ValueType>& partition) {
  IdType src;
   fprintf(stderr,"\nTID: %d Compute Edgecuts Partition: %u ", tid, partition.size());
  std::vector<unsigned> bndvert;
  for (InMemoryContainerConstIterator<KeyType, ValueType> it = partition.begin(); it != partition.end(); ++it) {
      src = it->first;
    //  fprintf(stderr,"\nTID: %d src: %d, nbrs: %d dst: ", tid, it->first, it->second.size());
      for(std::vector<IdType>::const_iterator vit = it->second.begin(); vit != it->second.end(); ++vit){
      //   fprintf(stderr,"%d\t", *vit);
         bndvert.push_back(*vit);
      }
      unsigned nbrs = bndvert.size();
      int countTopK=0;
      int costE = 0;  //count external cost of edge
       //compute the number of edges cut for every key-values pair in the map
      for(auto it = bndvert.begin(); it != bndvert.end(); ++it) {
         IdType dst = *it;
         if( gwhere[dst] != INIT_VAL && gwhere[src] != gwhere[dst] ) {
          // fprintf(stderr,"\nTID: %d, where[%d]: %d != where[%d]: %d ", tid, src, gwhere[src], dst, gwhere[dst]);
           totalPECuts[gwhere[src]]++;
           costE++;
           bndind[dst]++; // = costE ;     
         }
      }
   bndvert.clear();
   //calculate d-values
   unsigned costI = nbrs - costE;
   unsigned dsrc = costE - costI;      // External - Internal cost
    //fprintf(stderr, "\nTID: %d, Calculate DVals src: %d dval: %d, where[src]: %d", tid, src, dsrc, gWhere[src]);
   dTable[gwhere[src]][src] = dsrc;
  }
}

//=======================

unsigned ComputeGain(const unsigned tid, const unsigned hipart, const unsigned whereMax, std::vector<unsigned>& markMax, std::vector<unsigned>& markMin, const InMemoryContainer<KeyType, ValueType>& inMemMap){
  int maxG = 0;
  int maxvtx = -1, minvtx = -1;
  fprintf(stderr, "\nTID: %d, Computing GAIN ", tid);

  for (auto it = dTable[hipart].begin(); it != dTable[hipart].end(); ++it) {
      unsigned src = it->first;
      std::vector<unsigned>::iterator it_max = std::find (markMax.begin(), markMax.end(), src);
      if(it_max != markMax.end()){
        continue;
      }
      else{
        auto it_map = inMemMap.find(src);
        if(it_map != inMemMap.end()){
          if(dTable[whereMax].size() > 0 ){
            for (auto it_hi = dTable[whereMax].begin(); it_hi != dTable[whereMax].end(); ++it_hi) {
                unsigned dst = it_hi->first;
         //  fprintf(stderr, "\nTID: %d, Computing Gain src: %d, dest: %d ", tid, src, dst);
                bool connect = 0;
                unsigned dsrc = dTable[hipart][src];
                unsigned ddst = dTable[whereMax][dst];
                if(dsrc >= 0 || ddst >= 0){
                  std::vector<unsigned>::iterator it_min = std::find (markMin.begin(), markMin.end(), dst);
                  if(it_min != markMin.end()){
                    continue;
                  }
                  else{
                    if (std::find(it_map->second.begin(), it_map->second.end(), dst) == it_map->second.end()){
                       connect = 0;
                    }
                    else
                       connect = 1;

                    int currGain = -1;
                    if(!connect)
                      currGain = dsrc + ddst;
                    else
                      currGain = dsrc + ddst - 2;

                    if(currGain > maxG){ 
                      maxG = currGain;
                      maxvtx = src;
                      minvtx = dst;
                    }
                  } //end else
                } //enf if dsrc>0 condition
               else  // dsrc, ddst both negative
                  continue;
            }// end for loop
          }// end dtable size check
        } // end it_map if condition
      } // end else
  } // end dtable for loop

  if(maxvtx != -1 && minvtx != -1){
//fprintf(stderr, "\nTID: %d, MASKING src: %d, dst: %d, Gain: %d ", tid, maxvtx, minvtx, maxG);
    markMax.push_back(maxvtx);
    markMin.push_back(minvtx);
    return maxG;
  }

 return -1;
}

//========================
void clearMemorystructures(const unsigned tid){
  for(unsigned i=0; i<nparts; i++){
    bndIndMap[i].clear();
    markMax[i].clear();
    markMin[i].clear();
    dTable[i].clear();
    totalPECuts[i] = 0;
  }
}


//========================
void clearRefineStructures(){
  markMax->clear();
  markMin->clear();
  delete[] totalPECuts;
  delete[] bndIndMap;
  delete[] dTable;                  
  delete[] markMax;                  
  delete[] markMin;
  delete[] where;
}


//=========================
 void gCopy(const unsigned tid, std::vector<unsigned long>& gWhere){
    bool first = 1;
    for(unsigned i=0; i<nparts; ++i){
       for(unsigned j=0; j<=nvertices; ++j){
           if(first){  // All Values of first thread will be copied                                                       //         fprintf(stderr,"\nwhere[%d][%d]: %d,", i, j, where[i][j]);
              gWhere[j] = where[i][j];
           }
                                                                                                                                 else {                                                                                                                   if(where[i][j] != INIT_VAL){
              gWhere[j] = where[i][j];
            }                                                                                                                    }
  //             fprintf(stderr,"\nGWHERE[%d]: %d", j, gWhere[j]);
       }
          first = 0;
    }
 }

  void* updateReduceIter(const unsigned tid) {
    fprintf(stderr,"\nTID: %d Updating reduce Iteration ", tid);

    edgeCounter = 0;
    ++iteration;
    clearMemorystructures(tid);
    if(iteration > this->getIterations()){
       fprintf(stderr, "\nTID: %d, Iteration: %d Complete ", tid, iteration-1);
       don = true;
       return NULL;
    }
    this->notDone(tid);
     // assign next to prev for the next iteration , copy to prev of all threads
     
    fprintf(stderr,"\nTID: %d, iteration: %d ----", tid, iteration);
   return NULL;
  }

  void* afterReduce(const unsigned tid) {
    //readEdges[tid].clear();
    if(tid == 0){
      clearRefineStructures();
    }
    return NULL;
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
  MtMetis<IdType, IdType> mt;

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
#ifdef USE_GOMR
nvertices = atoi(argv[7]);
niterations = atoi(argv[8]);
if(atoi(argv[9]) > 0)
   hiDegree = atoi(argv[9]);
else
   hiDegree = 0;

if(atoi(argv[10]) > 0)
   nparts = atoi(argv[10]);
else
   nparts = 2;

#else
nvertices = -1;
niterations = 1;
hiDegree = 0;
#endif

int batchSize = atoi(argv[5]);
int kitems = atoi(argv[6]);

assert(batchSize > 0);

mt.init(folderpath, gb, nmappers, nreducers, nvertices, hiDegree, batchSize, kitems, niterations);

mt.writeInit(nreducers, nvertices);
//slidingShard = (Edge **) calloc(numShards, sizeof(Edge *));

pthread_barrier_init(&barCompute, NULL, nreducers);
pthread_barrier_init(&barWait, NULL, nreducers);
ii = (PartitionInfo *) calloc(nreducers, sizeof(PartitionInfo));
assert(ii != NULL);


gcd = (CoarsenLengths **) calloc(nreducers, sizeof(CoarsenLengths *));
for(unsigned i=0; i<nreducers; i++) {
    gcd[i] = (CoarsenLengths *) calloc(nreducers, sizeof(CoarsenLengths));
    assert(gcd[i] != NULL);
}
/*
match = (IdType **) calloc(nreducers, sizeof(IdType *));
for(unsigned i=0; i<nreducers; i++) {
    match = (IdType *) calloc(nvertices, sizeof(IdType));
    assert(match[i] != NULL);
}
perm = (IdType **) calloc(nreducers, sizeof(IdType *));
for(unsigned i=0; i<nreducers; i++) {
    perm = (IdType *) calloc(nvertices, sizeof(IdType));
    assert(perm[i] != NULL);
}*/
//cmap = (IdType *) calloc(nvtxs, sizeof(IdType));
/*
// for efficiently traversing sliding shards
ssIndex = (unsigned *) calloc(nreducers, sizeof(unsigned)); assert(ssIndex != NULL);
*/
double runTime = -getTimer();
mt.run();
runTime += getTimer();

std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
free(ii); free(gcd); free(match); free(perm); free(cmap);
//readEdges->clear();
delete[] readEdges;
MallocExtension::instance()->ReleaseFreeMemory();

return 0;
}

