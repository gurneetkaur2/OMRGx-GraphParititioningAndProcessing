#include "data.pb.h"
//#endif

#define USE_NUMERICAL_HASH
#define INIT_VAL 5000

#include "../../engine/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>
#include<mutex>

static int nvertices;
static int nmappers;
static int nReducers;
std::mutex m;
//static uint64_t countTotalWords = 0;
//pthread_mutex_t countTotal;
const long double DAMPING_FACTOR = 0.85; // Google's recommendation in original paper.
const long double TOLERANCE = (1e-1);
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through
__thread unsigned iteration = 0;
static std::string outputPrefix = "";
static pthread_barrier_t barCompute;
static pthread_barrier_t barEdgeCuts;
static pthread_barrier_t barRefine;
static pthread_barrier_t barWriteInfo;
static pthread_barrier_t barAfterRefine;
static pthread_barrier_t barClear;
static pthread_barrier_t barShutdown;
std::vector<double> pr_times;

template <typename KeyType, typename ValueType>
class Go : public MapReduce<KeyType, ValueType>
{
 // static thread_local uint64_t countThreadWords;
  //static thread_local std::vector<unsigned> prev; // = (nvertices, -1);
  //static thread_local std::vector<unsigned> next; // = (nvertices, -1);
  static thread_local double stime;
  static thread_local std::ofstream ofile;

  std::vector<pthread_mutex_t> locks;
  //unsigned long long *totalPECuts;
  std::map<unsigned, unsigned>* dTable;
  std::map<unsigned, unsigned >* bndIndMap; // TODO:move its declaration here to make it thread local
  // std::map<unsigned, unsigned > refineMap;  // to store the vertices from all partitions to be refined with each other 
  std::vector<unsigned long>* where; // = (nvertices, -1);
  std::vector<unsigned long> gWhere; // = (nvertices, -1);
  std::vector<bool>* pIdsCompleted;
  std::vector<unsigned>* markMax;
  std::vector<unsigned>* markMin;
  std::set<unsigned>* fetchPIds;
  std::map<unsigned, unsigned> pIdStarted;

//  std::vector<unsigned> nNbrs; // = (nvertices, -1);
  public:

  void* beforeMap(const unsigned tid) {
    return NULL;
  }

  void writeInit(unsigned nCols, unsigned nVtces){
    where = new std::vector<unsigned long>[nCols]; // nReducers cause problem here
    // fprintf(stderr,"\nInside writeINIT ************ Cols: %d, Vertices: %d ", nCols, nVtces);
    for (unsigned i = 0; i<nCols; ++i) {
      for (unsigned j = 0; j<=nVtces; ++j) {
        where[i].push_back(INIT_VAL);
      }
    }
    for (unsigned j = 0; j<=nVtces; ++j) {
      gWhere.push_back(-1);
    }
  pr_times.resize(nCols, 0.0);
  }

 unsigned setPartitionId(const unsigned tid)
   {
    unsigned nCols = this->getCols();
             //fprintf(stderr,"\nTID: %d writing to partition: %d " , tid, tid % nCols);
    return  -1; //tid % nCols;
   }


  void* map(const unsigned tid, const unsigned fileId, const std::string& input, const unsigned nbufferId, const unsigned hiDegree)
  {
    std::stringstream inputStream(input);
    unsigned to, token;
    std::vector<unsigned> from;

    inputStream >> to;
    while(inputStream >> token){
      from.push_back(token);
    }
    unsigned nCols = this->getCols();
    unsigned bufferId = hashKey(to) % nCols;
    unsigned part = tid % nCols;

    if(where[part].at(to) == INIT_VAL)
      where[part].at(to) = bufferId;


    for(unsigned i = 0; i < from.size(); ++i){
      //                fprintf(stderr,"\nVID: %d FROM: %zu size: %zu", to, from[i], from.size());
      Edge e;
      e.src = from[i];
      e.dst = to;
      e.rank = 1.0/nvertices;
      e.vRank = 1.0/nvertices;
      e.numNeighbors = from.size();
      
      unsigned whereFrom = hashKey(from[i]) % nCols;
      if(where[part].at(from[i]) == INIT_VAL)
        where[part].at(from[i]) = whereFrom; // partition ID of where 'from' will go
      
      if (from.size() < hiDegree)
        this->writeBuf(tid, to, e, bufferId, 0);
      else
        this->writeBuf(tid, to, e, nbufferId, from.size());
      //this->writeBuf(tid, to, from[i], nbufferId, from.size());
    }

    return NULL;
  }

  void* initRefineStructs() {
    //initialize the locks to be used for synchronization
    for(unsigned i=0; i<nReducers; ++i) {
      pthread_mutex_t mutex;
      pthread_mutex_init(&mutex, NULL);
      locks.push_back(mutex);

      //refineMap[i] = 0;
    }
    //initialize the data structures required in refinement phase
    fetchPIds = new std::set<unsigned>[nReducers];
    dTable = new std::map<unsigned, unsigned>[nReducers];
    bndIndMap = new std::map<unsigned, unsigned >[nReducers]; // TODO:move its declaration here to make it thread local
    pIdsCompleted = new std::vector<bool>[nReducers];
    markMax = new std::vector<unsigned>[nReducers];
    markMin = new std::vector<unsigned>[nReducers];
    //  std::map<unsigned, unsigned> maxPair = new std::map<unsigned, unsigned>[this->nReducers];
    //totalPECuts = new unsigned long long[nReducers];
    // fetchPIds = new std::set<unsigned>[nReducers];
  }
  void* beforeReduce(const unsigned tid) {
      //  unsigned int iters = 0;
     fprintf(stderr, "\nTID: %d,BEFORE Reducing values \n", tid);
    if(tid ==0){
      //this->gCopy(tid, gWhere); //performance
    }

    // fprintf(stderr, "\nTID: %d,BEFORE RefineINIT \n", tid);
    refineInit(tid);
  }

  //--------------------------------------------------
  void refineInit(const unsigned tid) {
    // fprintf(stderr, "\nTID: %d,INSIDE RefineINIT \n", tid);
    for (unsigned i = 0; i < nReducers; i++) {
      fetchPIds[tid].insert(i);  //partition ids - 0,1,2 .. 
      pIdsCompleted[tid].push_back(false);
    }
    //fprintf(stderr,"\ntTID %d, RefineINit fetchPID size: %d ----", tid, fetchPIds[tid].size());
  //  totalPECuts[tid] = 0;
    //bndSet = false;
 //   totalCuts = 0; 
  }
  
  
  void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container){
     efprintf(stderr, "\nTID: %d, Reducing values Container Size: %d", tid, container.size());
      unsigned hipart = tid;
      ComputeBECut(tid, gWhere, bndIndMap[tid], dTable[tid], container);
      // wait for other threads to compute edgecuts before calculating dvalues values
       efprintf(stderr, "\nTID: %d, Before BarEDGECUTS , nReducers: %d ", tid, nReducers);
      pthread_barrier_wait(&(barEdgeCuts)); 
      for(auto whereMax=tid; whereMax < this->getCols(); whereMax++){ //start TID loop

      efprintf(stderr, "\nTID: %d, Computing Gain  Container: %d", tid, container.size());
          int maxG = -1;     
          do{
            maxG = computeGain(tid, hipart, whereMax, markMax[hipart], markMin[hipart], container);
          //  fprintf(stderr, "\nTID: %d, MaxG > 0: %d", tid, maxG);
          } while(maxG > 0);  //end do

        //writePartInfo
        efprintf(stderr,"\nTID %d going to write part markMax size: %d ", tid, markMax[hipart].size());
        for(unsigned it=0; it<markMax[hipart].size(); it++){
          unsigned vtx1 = markMax[hipart].at(it);     //it->first;
          unsigned vtx2 = markMin[hipart].at(it);
          //        fprintf(stderr,"\nTID %d whereMax %d vtx1: %d vtx2: %d  ", tid, whereMax, vtx1, vtx2);
          /*pthread_mutex_lock(&locks[tid]);
          gWhere.at(vtx1) = whereMax;
          gWhere.at(vtx2) = hipart;
          pthread_mutex_unlock(&locks[tid]);
          */// pthread_mutex_unlock(&locks[tid]);
          //          fprintf(stderr, "\nTID: %d, Change Where ", tid);
          //changewhere
          where[hipart].at(vtx1) = vtx1; //gWhere[vtx1];
          where[hipart].at(vtx2) = vtx2; //gWhere[vtx2];  
          pthread_mutex_lock(&locks[tid]);
          where[whereMax].at(vtx1) = vtx1; //gWhere[vtx1];
          where[whereMax].at(vtx2) = vtx2; //gWhere[vtx2];
          pthread_mutex_unlock(&locks[tid]);
        }
      }
    pthread_barrier_wait(&(barWriteInfo));
    efprintf(stderr, "\nTID: %d, DONE ", tid);
    //}
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

  void* updateReduceIter(const unsigned tid) {

    ++iteration;
    if(iteration >= this->getIterations()){
      don = true;
      return NULL;
    }
    fprintf(stderr,"\nTID: %d, iteration: %d ----", tid, iteration);
//fprintf(stderr, "pagerank: thread %u iteration %d took %.3lf ms to process %llu vertices and %llu edges\n", tid, iteration, timevalToDouble(e) - timevalToDouble(s), nvertices, nedges);
//}

   return NULL;
  }

  void* afterReduce(const unsigned tid) {
      fprintf(stderr, "\nthread %u waiting for others to finish Refine\n", tid);

    //  if(tid == 0)
    //    this->gCopy(tid, gWhere);

      pthread_barrier_wait(&(barAfterRefine));

      if(tid == 0){
        clearRefineStructures();
      }
    return NULL;
  }
    
    //---------------
    void readAfterReduce(const unsigned tid) {
      // this->cRead(tid);
      don = false;
      // fprintf(stderr,"\nTID: %d COmputeEC TotalPECUTs: %d ", tid, totalPECuts[tid]);
      while(!this->getDone(tid)){
        InMemoryContainer<KeyType, ValueType>& container = this->cRead(tid);
        // fprintf(stderr, "\nTID: %d, Reading Container iSize: %d" , tid, container.size());

        ComputeBECut(tid, where[tid], bndIndMap[tid], dTable[tid], container);
      } 
    }

    //---------------
    void writeAfterReduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) {
      this->cWrite(tid); // container.size(), container.end());  
      //return container; 
      //   }
  }
  //--------------- 
  void ComputeBECut(const unsigned tid, const std::vector<unsigned long>& where, InMemTable& bndind, InMemTable& dTable, const InMemoryContainer<KeyType, ValueType>& inMemMap) {
    IdType dst;
    std::vector<unsigned> bndvert;
    efprintf(stderr, "\nTID: %d, Computing EdgeCuts COntainer Size: %d ", tid, inMemMap.size());

    for (InMemoryContainerConstIterator<KeyType, ValueType> it = inMemMap.begin(); it != inMemMap.end(); ++it) {
      //dst = it->first;
      //for (auto vit = it->second.begin(); vit != it->second.end(); ++vit){
      for(int k=0; k<it->second.size(); k++) {
        Edge e = it->second[k];
        dst = e.dst;
        bndvert.push_back(e.src); //Note: here src and dst are used opposite way src - adjacency list
      }
      //  refineMap[tid] = key; // assuming each thread writes at different index
      unsigned nbrs = bndvert.size();
      int countTopK=0;
      int costE = 0;  //count external cost of edge
      //compute the number of edges cut for every key-values pair in the map
      for(auto it = bndvert.begin(); it != bndvert.end(); ++it) { 
        IdType src = *it;
        if( where[src] != INIT_VAL && where[dst] != where[src] ) {
          //  fprintf(stderr,"\nTID: %d, where[%d]: %d != where[%d]: %d ", tid, src, where[src], dst, where[dst]);
         // totalPECuts[tid]++;
          costE++;
          bndind[src]++; // = costE ;     
        }
      }
      bndvert.clear();

      //calculate d-values
      unsigned costI = nbrs - costE; 
      unsigned ddst = costE - costI;      // External - Internal cost
       efprintf(stderr, "\nTID: %d, Calculate DVals dst: %d ddst: %d ", tid, dst, ddst);
      //dTable[dst] = ddst;
      dTable[dst] = ddst; 
    } //end Compute edgecuts main Loop
    //   pthread_barrier_wait(&(barCompute)); //wait for the dvalues from all the threads to be populated
    // fprintf(stderr, "\nTID: %d,Finished Computing EdgeCuts ****** ", tid);
  }

  //---------------
  unsigned computeGain(const unsigned tid, const unsigned hipart, const unsigned whereMax, std::vector<unsigned>& markMax, std::vector<unsigned>& markMin, const InMemoryContainer<KeyType, ValueType>& inMemMap){
    int maxG = 0;
    int maxvtx = -1, minvtx = -1; 
    efprintf(stderr, "\nTID: %d, Computing GAIN ", tid);
    for (auto it = dTable[hipart].begin(); it != dTable[hipart].end(); ++it) {
      //         fprintf(stderr, "\nTId %d dTable hipart Begin: %d, dTable hipartSize: %d, whereMax size: %d ----- " , tid, it->first, dTable[hipart].size(), dTable[whereMax].size());
      unsigned src = it->first; 
      std::vector<unsigned>::iterator it_max = std::find (markMax.begin(), markMax.end(), src);
      if(it_max != markMax.end()){
        continue;
      }
      else{
        auto it_map = inMemMap.find(src);
        if(it_map != inMemMap.end()){
          if(dTable[whereMax].size() > 0 ){
            //     fprintf(stderr, "\nBREAKING dTable size check TId %d ----- " , tid);
            // dont need this (interval based)     auto begin = std::next(dTable[whereMax].begin(), k);
            //fprintf(stderr, "\nTId %d dTable whereMax Begin: %d, dTable Size %d ----- " , tid, dTable[whereMax].begin(), dTable[whereMax].size());
            //         fprintf(stderr,"\nTID %d DTable[%d] values ", tid, whereMax);
            auto it_hi = dTable[whereMax].begin();
            for ( ; it_hi != dTable[whereMax].end(); ++it_hi) {
              unsigned dst = it_hi->first;
              //fprintf(stderr, "\nTID: %d, Computing Gain src: %d, dest: %d ", tid, src, dst);
              bool connect = 0;  
              unsigned dsrc = dTable[hipart][src];
              unsigned ddst = dTable[whereMax][dst];      
              if(dsrc >= 0 || ddst >= 0){
                // fprintf(stderr, "\nTID: %d, Computing Gain Positive d-val ", tid);
                std::vector<unsigned>::iterator it_min = std::find (markMin.begin(), markMin.end(), dst);
                if(it_min != markMin.end()){     // check if the dst is in the masked vertices
                  //              fprintf(stderr, "\nTID: %d, Computing Gain DST: %d is masked ", tid, dst);
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
               /*   if (std::find(it_map->second.begin(), it_map->second.end(), dst) == it_map->second.end()){
                    connect = 0;
                  }
                  else
                    connect = 1;
                    */
                  // check if src is a boundary vertex, then calculate gain
                  // auto it_bnd = bndIndMap[whereMax].find(src);
                  // if(it_bnd != bndIndMap[whereMax].end()){  
                  //fprintf(stderr, "\nTID: %d, Computing Gain SRC: %d is boundary vertex ", tid, src);
                  int currGain = -1;
                  if(!connect)
                    currGain = dsrc + ddst;
                  else
                    currGain = dsrc + ddst - 2;

                  //                 fprintf(stderr, "\nTID: %d, src: %d, dst: %d, Gain: %d MaxGain: %d ", tid, src, dst, currGain, maxG);

                  if(currGain > maxG){                                                                                            maxG = currGain;
                    //fprintf(stderr, "\nTID: %d, src: %d, dst: %d, MAXGain: %d ", tid, src, dst, currGain);
                    maxvtx = src;
                    minvtx = dst;
                  }
                } // else
                } // if condition dsrc > 0
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
    
    //-----------------------------------
    void clearRefineStructures(){
      for (unsigned i = 0; i < nReducers; i++){
        //  bndIndMap[i].clear();
        // refineMap[i].clear();
      }
      pthread_barrier_destroy(&barRefine);
      pthread_barrier_destroy(&barCompute);
      pthread_barrier_destroy(&barEdgeCuts);
      pthread_barrier_destroy(&barWriteInfo);
      pthread_barrier_destroy(&barClear);
      pthread_barrier_destroy(&barShutdown);
      pthread_barrier_destroy(&barAfterRefine);

      //  dTable->clear();
      markMax->clear();
      markMin->clear();
      fetchPIds->clear();
      pIdsCompleted->clear();
     // delete[] totalPECuts;
      delete[] bndIndMap;
      delete[] dTable;
      delete[] markMax;
      delete[] markMin;
      delete[] pIdsCompleted;
      delete[] where;
      delete[] fetchPIds;
    }
    //-----------------------------------
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
    }

    //-----------------------------------

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

};


template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
  to.insert(to.end(), from.begin(), from.end());
  return NULL;
}

//-------------------------------------------------
int main(int argc, char** argv)
{
  Go<IdType, Edge> go;


  if (argc < 8)
  {
  std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems> <optional - nvertices> <optional - partitions> <optional - max degree> <optional - partition output prefix>" << std::endl;

  return 0;
}

std::string folderpath = argv[1];
int gb = atoi(argv[2]);
nmappers = atoi(argv[3]);
nReducers = atoi(argv[4]);
int npartitions;
int hiDegree;
#ifdef USE_GOMR
nvertices = atoi(argv[7]);
npartitions = atoi(argv[8]);
outputPrefix = argv[10];
if(atoi(argv[9]) > 0)
  hiDegree = atoi(argv[9]);
else
hiDegree = 0;

#else
nvertices = -1;
hiDegree = -1;
npartitions = 2; ///partitions = nreducers -- iterations if not using GOMR
#endif

int batchSize = atoi(argv[5]);
int kitems = atoi(argv[6]);

assert(batchSize > 0);

pthread_barrier_init(&barRefine, NULL, nReducers);
pthread_barrier_init(&barCompute, NULL, nReducers);
pthread_barrier_init(&barEdgeCuts, NULL, nReducers);
pthread_barrier_init(&barWriteInfo, NULL, nReducers);
pthread_barrier_init(&barClear, NULL, nReducers);
pthread_barrier_init(&barShutdown, NULL, nReducers);
pthread_barrier_init(&barAfterRefine, NULL, nReducers);

go.writeInit(nReducers, nvertices);
go.init(folderpath, gb, nmappers, nReducers, nvertices, hiDegree, batchSize, kitems, npartitions);
go.initRefineStructs();
std::cout<<"\nCalling RUN" ;
double runTime = -getTimer();
go.run(); 
runTime += getTimer();

auto pr_time = max_element(std::begin(pr_times), std::end(pr_times));
std::cout << " Page Rank Processing time : " << *pr_time << " (msec)" << std::endl;

std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
//std::cout << "Total words: " << countTotalWords << std::endl;
return 0;
}

