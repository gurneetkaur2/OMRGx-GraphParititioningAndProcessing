//RUN - ./mtmetis.bin ~/work/datasets/inputs/mdual 1 4 2 14000 20 258570 1 10000 2 test -DUSE_GOMR 
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
#include<mutex>

#define UNMATCHED  -1
#define COARSEN_FRACTION  0.85  /* Node reduction between succesive coarsening levels */
#define MAX_UINT (ULLONG_MAX)
#define INIT_VAL 5000
#define INTERVAL 20
#define gk_max(a, b) ((a) >= (b) ? (a) : (b))

static int nvertices;
static int nmappers;
static int nparts;
static std::string outputPrefix = "";

//EdgeList* edgeLists = NULL;
IdType totalEdges = 0;
unsigned long long numEdges = 0;

//Meta data for Coarsen Graph processing
typedef struct __partitionInfo {
  IdType edgeCount; // dont need this
  IdType ubIndex;  //endKey
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
//__thread IdType *match = NULL;
__thread IdType *perm = NULL;
//__thread IdType *cmap = NULL;
std::map<IdType, IdType>* match;
std::map<IdType, IdType>* cmap;
//pagerank for previous and next iteration
std::map<IdType, std::vector<IdType> >* clgraph; /// this can be used to store coarser level graphs
static pthread_barrier_t barCompute;
static pthread_barrier_t barEdgeCuts;
static pthread_barrier_t barWait;
std::mutex m;
std::vector<double> pr_times;

template <typename KeyType, typename ValueType>
class MtMetis : public MapReduce<KeyType, ValueType>
{
  std::vector<pthread_mutex_t> locks;
  // std::vector<std::mutex> mlocks;
  std::vector<unsigned long>* where; // = (nvertices, -1);
  std::vector<unsigned long> gWhere; 
  std::map<unsigned, unsigned>* dTable;
  std::map<unsigned, unsigned >* bndIndMap;
  std::vector<unsigned>* markMax;
  std::vector<unsigned>* markMin;
  unsigned long long *totalPECuts;
  static thread_local std::ofstream ofile;

  public:

  void* beforeMap(const unsigned tid) {
    unsigned nCols = this->getCols();
    efprintf(stderr, "TID: %d, nvert:  %d part: %d \n", tid, nvertices, tid % nCols);
    return NULL;
  }

  void writeInit(unsigned nCols, unsigned nVtces){
    clgraph = new std::map<IdType, std::vector<IdType> >[nCols];
    match = new std::map<IdType, IdType>[nCols];
    cmap = new std::map<IdType, IdType>[nCols];
    where = new std::vector<unsigned long>[nCols];
    dTable = new std::map<unsigned, unsigned>[nCols];
    bndIndMap = new std::map<unsigned, unsigned >[nCols]; 
    markMax = new std::vector<unsigned>[nCols];
    markMin = new std::vector<unsigned>[nCols];
    totalPECuts = new unsigned long long[nCols];

    for (unsigned i = 0; i<nCols; ++i) {
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
  pr_times.resize(nCols, 0.0);
  }

//------------------------------------------------

  unsigned setPartitionId(const unsigned tid)
  {
    unsigned nCols = this->getCols();
    //fprintf(stderr,"\nTID: %d writing to partition: %d " , tid, tid % nCols);
    return -1;// tid % nCols;  
  }

//------------------------------------------------

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
      Edge e;
      e.src = from[i];
      e.dst = to;
      e.rank = 1.0/nvertices;
      e.vRank = 1.0/nvertices;
      e.numNeighbors = from.size();
      //     fprintf(stderr,"\tTID: %d, src: %zu, dst: %zu, vrank: %f, rank: %f nNbrs: %u", tid, e.src, e.dst, e.vRank, e.rank, e.numNeighbors);
      this->writeBuf(tid, to, e, nbufferId, 0);
    }

    return NULL;
  }

//------------------------------------------------

  void* beforeReduce(const unsigned tid) {
    unsigned size = this->getContainerSize();
    perm = (IdType *) calloc(size, sizeof(IdType));
    }

//------------------------------------------------
    void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) {

      efprintf(stderr, "\nTID:%d Starting Reduce\n", tid);
      unsigned part = tid;
      IdType indexCount = 0;
      edgeCounter = 0;
//      ii[part].lbEdgeCount = edgeCounter;
      assert(container.size() > 0);

      IdType lbIndex = container.begin()->first;
      efprintf(stderr, "Initialize sub-graph: %u\n", part);
      timeval s, e;
      gettimeofday(&s, NULL);
      efprintf(stderr,"\nPART: %u, CONTAINER elements LowerBound: %d ", tid, lbIndex);

      for(auto it = container.begin(); it != container.end(); it++){
        edgeCounter += it->second.size();
        match[tid][it->first] = -1;
        //fprintf(stderr,"\ntid: %d, match[i]: %d, unmatched: %d ", tid, match[i], UNMATCHED);
        perm[indexCount++] = it->first;
        cmap[tid][it->first] = -1; //TODO: it may give trouble if the vertices are not from beginning
        ii[part].ubIndex = it->first;
      }
      efprintf(stderr, "\nTID: %d done adding container \n", tid);
      gettimeofday(&e, NULL);
      ii[part].edgeCount = edgeCounter;
   //   ii[part].edgeCount = ii[part].ubEdgeCount - ii[part].lbEdgeCount;
      efprintf(stderr, "\nInitializing subgraph for part %u took: %.3lf edgeCounter: %u\n", part, tmDiff(s, e), edgeCounter);
      //coarsest graph
      
      std::map<KeyType, std::vector<ValueType>> last_cgraph; 
      gettimeofday(&s, NULL);
      last_cgraph = coarsen(tid, container); //, readEdges[tid]);
      gettimeofday(&e, NULL);
      efprintf(stderr, "\nCOARSENING part %u took: %.3lf \n", part, tmDiff(s, e));

      initpartition(tid, last_cgraph);

      //last_cgraph = container;
      std::map<KeyType, std::vector<ValueType>> cgraph(last_cgraph);
//      unsigned level = ii[tid].levels;
      gettimeofday(&s, NULL);

      int level = 0;
      do{
        efprintf(stderr, "\n*****Tid: %d Refining LEVEL: %u *****\n", tid, level);
        //TODO: to refine all coarser level graphs -- clgraph
        //std::map<KeyType, std::vector<ValueType>> cgraph(clgraph[tid][level]);
        // refining the coarsest level graph which is in memory and fetching the finer levels later
        refinepartition(tid, cgraph);
        //refinepartition(tid, container);
        efprintf(stderr,"\nTID: %d Waiting after refine ", tid);
        pthread_barrier_wait(&(barWait));
        
        //project partition to finer level  
        IdType k;
        efprintf(stderr,"\nTID: %d Projecting partition ", tid);
        for(auto fit = cgraph.begin(); fit != cgraph.end(); fit++){
        //for(auto fit = container.begin(); fit != container.end(); fit++){
          k = cmap[tid][fit->first];
          //fprintf(stderr,"\ntid: %d fit->first: %u, k: %u, gWhere: %d ", tid, fit->first, k, gWhere[fit->first]); 
          pthread_mutex_lock(&locks[tid]);
          where[tid].at(fit->first) = where[tid][k];
          pthread_mutex_unlock(&locks[tid]);
        }
        level--;
        efprintf(stderr,"\nTID: %d DONE Projecting partition level: %d ", tid, level);
      } while(level > 0);

      gettimeofday(&e, NULL);
      fprintf(stderr, "\nRefining Partition for part %u took: %.3lf \n", part, tmDiff(s, e));
        //assert(false);
        // store the coarsened graph on disk

        efprintf(stderr,"\n\n****tid: %d Finished refining *** ", tid); 
        pthread_barrier_wait(&(barCompute));
    
    long double sum = 0.0;
   efprintf(stderr, "TID: %d, Running PR \n", tid);
   double time_pr = -getTimer();
    
   std::vector<Edge *> vertices;
   IdType v;
   for(auto it = container.begin(); it != container.end(); it++){ 
      unsigned key = it->first;
        for(int k=0; k<it->second.size(); k++) {
         Edge e = it->second[k];
         v = e.dst;
         vertices.push_back(&e);
         while(k<it->second.size() && v==e.dst) k++; 
        }
   }
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
        float rank = (DAMPING_FACTOR * sum) + (1 - DAMPING_FACTOR);
        unsigned dstEndIndex = i;
        for(unsigned dstIndex = dstStartIndex; dstIndex<dstEndIndex; dstIndex++){
            vertices[dstIndex]->vRank = rank;
        }
     }
     time_pr += getTimer();
     pr_times[tid] += time_pr;
        clearMemorystructures(tid);
        return NULL;
      }


//------------------------------------------------
      std::map<KeyType, std::vector<ValueType>> coarsen(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container){
        std::map<KeyType, std::vector<ValueType>> cgraph(container);
        efprintf(stderr,"\nTID: %d, Coarsening graph container size: %u ", tid, cgraph.size());
        IdType cnvtxs = 0; IdType cnedges;
        unsigned nedges = ii[tid].edgeCount;
        unsigned level = 0;
        unsigned CoarsenTo = 100; //gk_max((nvtxs)/(20*std::log2(nparts)), 30*(nparts));
        gcd[tid][level].startIndex = 0;

        //do{
          cgraph = MATCH_RM(tid, cgraph, nedges, level);
        // return last coarses graph
        return cgraph;
      }


//------------------------------------------------
      std::map<KeyType, std::vector<ValueType>> MATCH_RM(const unsigned tid, std::map<KeyType, std::vector<ValueType>>& container, IdType nedges, unsigned level){
        IdType i, j, pi, cnvtxs, last_unmatched, maxidx;

        unsigned seed = (container.size()*tid+1) % container.size(); 
        //(std::chrono::system_clock::now().time_since_epoch().count()) % nvertices;
        // Shuffling our array
        std::shuffle(perm, perm + container.size(), std::default_random_engine(seed));
        efprintf(stderr,"\ntid: %d Inside MATCH_RM ", tid); 
        size_t nunmatched=0;
        std::map<KeyType, std::vector<ValueType>> cgraph;

        cnvtxs=0, last_unmatched=0; pi=0;
        for(auto fit = container.begin(); fit != container.end(); fit++){
          i = perm[fit->first] % container.size();  // i = 4
          //fprintf(stderr,"\nTID: %d, Picked random vertex: %u, i: %u , match[i]: %d, Actual element: %u ", tid, perm[fit->first] % container.size(), i, match[i], fit->first);

          if (match[tid][i] == UNMATCHED) {  /* Unmatched */
            maxidx = i; // = 4
        //    auto it = container.find(i);
           // if(it != container.end()){
           //   for (; last_unmatched<it->second.size(); last_unmatched++) {
              for (; last_unmatched<container[i].size(); last_unmatched++) {
                j = container[i][last_unmatched]; // pick a random adjacent vertex of i
                //j = it->second[last_unmatched]; // pick a random adjacent vertex of i
                //make sure adj vertex is within this container && check if it is unmatched
                if (match[tid][j] == UNMATCHED) {   
                  // fprintf(stderr,"\nTID: %d, j: %u matched: %d  ", tid, j, match[j]);
                  maxidx = j; //
                  break;
                }
              }
           // }

            if (maxidx != UNMATCHED) {
              cmap[tid][i]  = cmap[tid][maxidx] = cnvtxs++;
              match[tid][i] = maxidx;
              match[tid][maxidx] = i;
              efprintf(stderr,"\nTID: %d, i: %u, maxidx: %u cmap: %u match: %u ", tid, i, maxidx, cmap[tid][i], match[tid][i]);
            }
          }
        }
        /* match the final unmatched vertices with themselves and reorder the vertices 
           of the coarse graph for memory-friendly contraction */
        //for (cnvtxs=0, i=0; i<nvtxs; i++) {
        cnvtxs = 0;
        for(auto fit = container.begin(); fit != container.end(); fit++){
          if (match[tid][fit->first] == UNMATCHED) {
            match[tid][fit->first] = fit->first;
            cmap[tid][fit->first]  = cnvtxs++;
          }
          else {
            if (fit->first <= match[tid][fit->first])
              cmap[tid][fit->first] = cmap[tid][match[tid][fit->first]] = cnvtxs++;
          }
          efprintf(stderr,"\nTID: %d, FInal container element i: %u cmap: %u match: %u ", tid, fit->first, cmap[tid][fit->first], match[tid][fit->first]);
        }

        cgraph = CreateCoarseGraph(tid, container, cnvtxs, cmap[tid], level);
        //fprintf(stderr,"\nTID: %d coarser vertices: %u ", tid, cnvtxs);
        gcd[tid][level].cnvtxs = cgraph.size();
        return cgraph;
      }


//------------------------------------------------
      std::map<KeyType, std::vector<ValueType>> CreateCoarseGraph(const unsigned tid, std::map<KeyType, std::vector<ValueType>> &container, IdType cnvtxs, std::map<IdType, IdType>& cmap, unsigned level){
        //fprintf(stderr,"\nTID: %d inside createCoarse graph size: %u ", tid, container.size());
        std::map<KeyType, std::vector<ValueType>> cgraph;
        IdType k, v, u, nedges, cnedges;

        cnedges = 0;
        //   fprintf(stderr,"\nTID: %d CreateCoarse graph Container size: %u ", tid, container.size());
        for(auto it = container.begin(); it !=container.end(); it++){
          v = it->first;
          // fprintf(stderr,"\nTID: %d CreateCoarse First: %u match[v]: %u ", tid, v, match[v]);
          if ((u = match[tid][v]) < v)
            continue;
          IdType to = it->first;
          //unsigned bufferId = tid % nparts; //hashKey(to) % this->getCols();
          // unsigned part = rand() % this->getCols(); //nparts;
          unsigned part = tid % this->getCols();
          if(where[part].at(to) == INIT_VAL)
            where[part].at(to) = part; //bufferId;

          nedges = 0;
          for(unsigned vit=0; vit <it->second.size(); vit++){  //adjncy of v
	    Edge e = it->second[vit];
            k = cmap[e.src]; // num of cnvtxs (coarsened vertices) of this vertex
            //fprintf(stderr,"\nTID: %d next Iter vit: %u k: %u ", tid, it->second[vit], k);
            if(k == UNMATCHED || k >ii[tid].ubIndex)
              continue;
  //          if(m = htable[k] == -1)
  //            htable[k] = nedges++;
            if(k == UNMATCHED)
              nedges++;    
	    IdType from = e.src;
            //   unsigned whereFrom = hashKey(from) % this->getCols();
            if(where[part].at(from) == INIT_VAL)
              where[part].at(from) = part; //bufferId; 

            cgraph[it->first].push_back(e.src);
            //   fprintf(stderr,"\nTID: %d, element: %d k: %u, nedges: %u match: %u htable: %u ", tid,it->second[vit], k, nedges, match[it->second[vit]], htable[k]);
          }
          cnedges         += nedges;
        }
        gcd[tid][level].cnedges = cnedges;
        return cgraph;
      }

//------------------------------------------------

      void initpartition(const unsigned tid, std::map<KeyType, std::vector<ValueType>> &cgraph){
   //     srand(time(NULL));

        efprintf(stderr,"\nTID: %d init partition cgraph: %u ", tid, cgraph.size());
        for (auto it= cgraph.begin(); it != cgraph.end(); it++){
          IdType to = it->first;
          //unsigned bufferId = tid % nparts; //hashKey(to) % this->getCols();
          // unsigned part = rand() % this->getCols(); //nparts;
          unsigned part = tid % this->getCols();
          if(where[part].at(to) == INIT_VAL)
            where[part].at(to) = part; //bufferId;

          for(auto vit=0; vit <it->second.size(); vit++){
            Edge e = it->second[vit];
	    IdType from = e.src;
            //   unsigned whereFrom = hashKey(from) % this->getCols();
            if(where[part].at(from) == INIT_VAL)
              where[part].at(from) = part; //bufferId; 
          }
          // fprintf(stderr,"\nTID: %d init partition key: %u where: %u ", tid, to, where[part].at(to));
        }
        /*  if(tid ==0){
            this->gCopy(tid, gWhere);
            }*/           
      }

//------------------------------------------------

      void refinepartition(const unsigned tid, const std::map<KeyType, std::vector<ValueType>> &partition){
        efprintf(stderr,"\nTID: %d Refine partition Partition: %u ", tid, partition.size());
        unsigned counter = 0;
        //unsigned hipart = tid;
        //InMemoryContainer<KeyType, ValueType> inMap;    
        //for(auto it = partition.begin(); it != partition.end(); it++){
        //  if (counter >= INTERVAL){
          ComputeBECut(tid, gWhere, bndIndMap[tid], partition);
       // pthread_barrier_wait(&(barEdgeCuts));
        //for(unsigned i=0; i < nparts; i++){
            unsigned nparts = this->getCols();
            unsigned hipart = tid; //i;
            for(unsigned j=tid; j < nparts; j++){
                unsigned whereMax = j;
        //for(auto whereMax=tid+1; whereMax < this->getCols(); whereMax++){ //start TID loop
             // if(whereMax == tid) continue;
                int maxG = -1;
                efprintf(stderr, "\nTID: %d, Computing GAIN hipart: %d, whereMax: %d ", tid, hipart, whereMax);
                do{        
                  maxG = ComputeGain(tid, hipart, whereMax, markMax[hipart], markMin[hipart], partition);
                } while(maxG > 0);
                for(unsigned it=0; it<markMax[hipart].size(); it++){
                  unsigned vtx1 = markMax[hipart].at(it);     //it->first;
                  unsigned vtx2 = markMin[hipart].at(it);
                  where[hipart].at(vtx1) = whereMax; //gWhere[vtx1];
                  where[hipart].at(vtx2) = hipart; //gWhere[vtx2];
                  where[whereMax].at(vtx1) = whereMax; //gWhere[vtx1];
                  where[whereMax].at(vtx2) = hipart; //gWhere[vtx2];
                }
            }
         /*   counter = 0;
          } //end if counter loop
          else{
            inMap[it->first] = it->second;
            counter++;
          }
        }*/
       // } //end for container loop
        efprintf(stderr,"\nTID: %d FINISHED Refine partition ", tid);
      }

//------------------------------------------------

      void ComputeBECut(const unsigned tid, const std::vector<unsigned long>& where, InMemTable& bndind, const InMemoryContainer<KeyType, ValueType>& partition) {
        IdType dst;
        efprintf(stderr,"\nTID: %d Compute Edgecuts Partition: %u ", tid, partition.size());
        std::vector<unsigned> bndvert;
        for (InMemoryContainerConstIterator<KeyType, ValueType> it = partition.begin(); it != partition.end(); ++it) {
          dst = it->first;
          //fprintf(stderr,"\nTID: %d src: %d, nbrs: %d dst: ", tid, it->first, it->second.size());
         // for(std::vector<IdType>::const_iterator vit = it->second.begin(); vit != it->second.end(); ++vit){
          for(int k=0; k<it->second.size(); k++) {
	    Edge e = it->second[k];
	    //   fprintf(stderr,"%d\t", *vit);
            dst = e.dst;
	    bndvert.push_back(e.src);
          }
          // fprintf(stderr,"\nTid: %d bndvert size: %d ", tid, bndvert.size());
          unsigned nbrs = bndvert.size();
          int countTopK=0;
          int costE = 0;  //count external cost of edge
          //compute the number of edges cut for every key-values pair in the map
          for(auto it = bndvert.begin(); it != bndvert.end(); ++it) {
            IdType src = *it;
            //if( where[tid][dst] != INIT_VAL && where[tid][src] != where[tid][dst] ) {
            if( where[src] != INIT_VAL && where[dst] != where[src] ) {
              // fprintf(stderr,"\nTID: %d, where[%d]: %d != where[%d]: %d ", tid, src, gwhere[src], dst, gwhere[dst]);
              totalPECuts[tid]++;
              costE++;
              bndind[src]++; // = costE ;     
            }
          }
          bndvert.clear();
          //calculate d-values
          if( where[dst] != INIT_VAL){ //
            unsigned costI = nbrs - costE;
            unsigned ddst = costE - costI;      // External - Internal cost
            //fprintf(stderr, "\nTID: %d, Calculate DVals src: %d dval: %d, where[src]: %d", tid, src, dsrc, gwhere[src]);
            //   pthread_mutex_lock(&locks[tid]);
            //  auto my_lock = std::unique_lock<std::mutex>(m);
            dTable[tid][dst] = ddst;
            //    pthread_mutex_unlock(&locks[tid]);
          }
        }
        efprintf(stderr, "\nTID: %d, Finished Computing EC ", tid);
      }

      //=======================

      unsigned ComputeGain(const unsigned tid, const unsigned hipart, const unsigned whereMax, std::vector<unsigned>& markMax, std::vector<unsigned>& markMin, const InMemoryContainer<KeyType, ValueType>& inMemMap){
        int maxG = 0;
        int maxvtx = -1, minvtx = -1;
        //fprintf(stderr, "\nTID: %d, Computing GAIN ", tid);

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
                      for(auto vit = it_map->second.begin(); vit != it_map->second.end(); vit++){
	              	Edge e = *vit;
                   	IdType v = e.src;
                    	if(v== dst){
                      	  connect = 1;
                      	  break;
                    	}
                    	else
                      	  connect = 0;
                     }

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
          // pthread_mutex_lock(&locks[tid]);
          markMax.push_back(maxvtx);
          markMin.push_back(minvtx);
          //   pthread_mutex_unlock(&locks[tid]);
          return maxG;
        }
        return -1;
      }

      //========================
      void clearMemorystructures(const unsigned tid){
        bndIndMap[tid].clear();
        markMax[tid].clear();
        markMin[tid].clear();
        dTable[tid].clear();
        clgraph[tid].clear();
        totalPECuts[tid] = 0;
      }


      //========================
      void clearRefineStructures(){
        markMax->clear();
        markMin->clear();
        //clgraph->clear();
        delete[] totalPECuts;
        delete[] bndIndMap;
        delete[] dTable;                  
        delete[] markMax;                  
        delete[] markMin;
        delete[] where;
        delete[] match;
        delete[] cmap;
        delete[] clgraph;
        pthread_barrier_destroy(&barWait);
        pthread_barrier_destroy(&barCompute);
      }


      //=========================
      /*void gCopy(const unsigned tid, std::vector<unsigned long>& gWhere){
        bool first = 1;
        efprintf(stderr,"\nTID: %d Inside gCOPY ", tid);
        for(unsigned i=0; i<this->getCols(); ++i){
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

      //============================
      void printParts(const unsigned tid, std::string fileName) {
      ofile.open(fileName);
      assert(ofile.is_open());
      //for(unsigned p = 0; p<nparts; p++){
      for(unsigned i = 0; i <= nvertices; ++i){
      if(gWhere[i] != -1 && (gWhere[i] == tid || gWhere[i] == tid % nparts)){
      ofile<<i << "\t" << gWhere[i]<< std::endl;
      }
      }
      // }
      ofile.close();
      }
       */
      //=======================
      void* updateReduceIter(const unsigned tid) {
        fprintf(stderr,"\nTID: %d Updating reduce Iteration ", tid);

        edgeCounter = 0;
        ++iteration;
        // if(tid==0)
        //clearMemorystructures(tid);

       // if(iteration > this->getIterations()){
          efprintf(stderr, "\nTID: %d, Iteration: %d Complete ", tid, iteration-1);
          don = true;
      //    return NULL;
       // }
        //this->notDone(tid);
        // assign next to prev for the next iteration , copy to prev of all threads

        efprintf(stderr,"\nTID: %d, iteration: %d ----", tid, iteration);
        return NULL;
      }

      void* afterReduce(const unsigned tid) {
        fprintf(stderr,"\nTID: %d After Reduce ", tid);
        if(tid == 0){
          clearRefineStructures();
        }

        //  std::string fileName = outputPrefix + std::to_string(tid);
        //   printParts(tid, fileName.c_str());

        return NULL;
      }

      };


      template <typename KeyType, typename ValueType>
        void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
          to.insert(to.end(), from.begin(), from.end());
          return NULL;
        }

      template <typename KeyType, typename ValueType>
        thread_local std::ofstream MtMetis<KeyType, ValueType>::ofile;

      //-------------------------------------------------
      int main(int argc, char** argv)
      {
        MtMetis<IdType, Edge> mt;

        if (argc < 10)
        {
          std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems> <nvertices> <iters> <hiDegree> <optional - partition output prefix>"<< std::endl;

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

        //if(atoi(argv[10]) > 0)
        //nparts = atoi(argv[10]);
        //else
        nparts = nreducers;

        outputPrefix = argv[10];
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
	auto pr_time = max_element(std::begin(pr_times), std::end(pr_times));
	std::cout << " Page Rank Processing time : " << *pr_time << " (msec)" << std::endl;
	
	free(ii); free(gcd); 
        //free(match); 
        free(perm); //free(cmap);
        MallocExtension::instance()->ReleaseFreeMemory();

        return 0;
      }


