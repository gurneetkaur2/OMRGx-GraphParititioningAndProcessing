#ifdef USE_ONE_PHASE_IO
#include "recordtype.h"
#else
#include "data.pb.h"
#endif

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

static int nvertices;
static int nmappers;
static int nReducers;
//static uint64_t countTotalWords = 0;
//pthread_mutex_t countTotal;
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through
__thread unsigned iteration = 0;
//__thread unsigned totalPECuts = 0;
static std::string outputPrefix = "";
static pthread_barrier_t barCompute;
static pthread_barrier_t barEdgeCuts;
static pthread_barrier_t barRefine;
static pthread_barrier_t barWriteInfo;
static pthread_barrier_t barAfterRefine;
static pthread_barrier_t barClear;
static pthread_barrier_t barShutdown;

template <typename KeyType, typename ValueType>
class Go : public MapReduce<KeyType, ValueType>
{
  // static thread_local uint64_t countThreadWords;
  //static thread_local std::vector<unsigned> prev; // = (nvertices, -1);
  //static thread_local std::vector<unsigned> next; // = (nvertices, -1);
  static thread_local double stime;
  static thread_local std::ofstream ofile;

  std::vector<pthread_mutex_t> locks;
  unsigned long long *totalPECuts;
  std::map<unsigned, unsigned>* dTable;
  std::map<unsigned, unsigned >* bndIndMap; // TODO:move its declaration here to make it thread local
  // std::map<unsigned, unsigned > refineMap;  // to store the vertices from all partitions to be refined with each other 
  std::vector<unsigned>* where; // = (nvertices, -1);
  std::vector<unsigned> gWhere; // = (nvertices, -1);
  std::vector<bool>* pIdsCompleted;
  std::vector<unsigned>* markMax;
  std::vector<unsigned>* markMin;
  std::set<unsigned>* fetchPIds;
  std::map<unsigned, unsigned> pIdStarted;

  unsigned totalCuts;

  public:

  void* beforeMap(const unsigned tid) {
    where = new std::vector<unsigned>[nReducers]; // nReducers cause problem here
    //next = new std::vector<double>[nmappers];
    fprintf(stderr, "TID: %d, nvert:  %d \n", tid, nvertices);
    for (unsigned i = 0; i<=nvertices; ++i) {
      // fprintf(stderr, "TID: %d, Inside where i: %d \n", tid, i);
      where[tid].push_back(INIT_VAL);
      // fprintf(stderr, "TID: %d,Before init gWhere \n", tid);
      if(tid==0){
        gWhere.push_back(-1);
      }
    }
    //  fprintf(stderr, "\n TID: %d, BEFORE key: %d prev: %f next: %f rank: %.2f \n", tid, i, prev[tid][i], next[tid][i], (1/nvertices));
    // fprintf(stderr, "TID: %d, After assigning init values \n", tid);
    return NULL;
  }
  //void* map(const unsigned tid, const unsigned fileId, const std::string& input)
  void* map(const unsigned tid, const std::string& input, const unsigned lineId)
  {
    //    fprintf(stderr, "TID: %d,Inside Map \n", tid);
    //pthread_barrier_wait(&(barClear));
    std::stringstream inputStream(input);
    unsigned to, token;
    std::vector<unsigned> from;

    //inputStream >> to;
    while(inputStream >> token){
      from.push_back(token);
    }
    //TODO: handle HID
    unsigned bufferId = hashKey(to) % nReducers;
    unsigned part = tid % nReducers;

    if(where[part].at(to) == INIT_VAL)
      where[part].at(to) = bufferId;

    for(unsigned i = 0; i < from.size(); ++i){
   //                  fprintf(stderr,"\nVID: %d FROM: %zu size: %zu", lineId, from[i], from.size());
      unsigned whereFrom = hashKey(from[i]) % nReducers;
      if(where[part].at(from[i]) == INIT_VAL)
        where[part].at(from[i]) = whereFrom; // partition ID of where 'from' will go

      //this->writeBuf(tid, to, from[i]);
      this->writeBuf(tid, lineId, from[i]);
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
    totalPECuts = new unsigned long long[nReducers];
    // fetchPIds = new std::set<unsigned>[nReducers];
   }

  void* beforeReduce(const unsigned tid) {
    //  unsigned int iters = 0;
    //copy local partition ids to gWhere
  //  fprintf(stderr, "\nTID: %d,BEFORE Reducing values \n", tid);
    if(tid ==0){
      this->gCopy(tid, gWhere);
      this->initRefineStructs();
    }

    refineInit(tid);
  }

//--------------------------------------------------
void refineInit(const unsigned tid) {
    for (unsigned i = 0; i < nReducers; i++) {
          fetchPIds[tid].insert(i);  //partition ids - 0,1,2 .. 
          pIdsCompleted[tid].push_back(false);
     }
//fprintf(stderr,"\ntTID %d, RefineINit fetchPID size: %d ----", tid, fetchPIds[tid].size());
  //   bndIndMap[tid].clear();
   //  dTable[tid].clear();
  //   readNext[tid] = 0;
  // clearMemorystructures(tid);
     totalPECuts[tid] = 0;
     //bndSet = false;
     totalCuts = 0; 
}

//--------------------------------------------------
  void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container){
    //countThreadWords += std::accumulate(values.begin(), values.end(), 0);
    long double sum = 0.0;
    // iterate each vertex neighbor in adjlist
    //fprintf(stderr, "\nTID: %d, Reducing values fetchPID Size: %d", tid, fetchPIds[tid].size());
    unsigned hipart = tid;
    // unsigned whereMax;
    for(auto it = fetchPIds[tid].begin(); it != fetchPIds[tid].end(); ++it) { 
    //for(auto wherei=0; wherei < nReducers; wherei++){ //start TID loop
      unsigned whereMax = *it; 
      fprintf(stderr, "\nTID: %d, WHEREMAX: %d ", tid, whereMax);
      if(whereMax == tid){
//      fprintf(stderr, "\nTID: %d pIdsCompleted[%d][%d]: %d ", tid, hipart, whereMax, pIdsCompleted[hipart][whereMax]);
        //pIdsCompleted[tid][*it] = true;
  //    fprintf(stderr, "\nTID: %d going to NEXT Iter", tid);
        continue;
      }
      else if (hipart < nReducers && whereMax >= nReducers){
          pIdsCompleted[tid][*it] = true;
          continue;
      }
      else if ( hipart >= nReducers && whereMax < nReducers) {
          pIdsCompleted[tid][*it] = true;
          continue;
      }
//      fprintf(stderr, "\nFINAL TID: %d, WHEREMAX: %d ", tid, whereMax);
      bool ret = this->checkPIDStarted(tid, hipart, whereMax);
//      fprintf(stderr, "\nTID: %d, refining with: %d, ret: %d ", tid, whereMax, ret);
      ComputeBECut(tid, gWhere, bndIndMap[tid], container);
      // wait for other threads to compute edgecuts before calculating dvalues values
//      fprintf(stderr, "\nTID: %d, Before BarEDGECUTS ", tid);
      pthread_barrier_wait(&(barEdgeCuts)); 

      fprintf(stderr, "\nTID: %d, Computing Gain  Container: %d", tid, container.size());
      if(ret == true){
        int maxG = -1;     
        do{
          maxG = computeGain(tid, hipart, whereMax, markMax[hipart], markMin[hipart], container);
    //  fprintf(stderr, "\nTID: %d, MaxG > 0: %d", tid, maxG);
        } while(maxG > 0);  //end do
      } // end if ret

   //   pthread_barrier_wait(&(barCompute)); 
      if(ret == true) {
        //writePartInfo
//        fprintf(stderr,"\nTID %d going to write part markMax size: %d ", tid, markMax[hipart].size());
        for(unsigned it=0; it<markMax[hipart].size(); it++){
          unsigned vtx1 = markMax[hipart].at(it);     //it->first;
          unsigned vtx2 = markMin[hipart].at(it);
  //        fprintf(stderr,"\nTID %d whereMax %d vtx1: %d vtx2: %d  ", tid, whereMax, vtx1, vtx2);
          pthread_mutex_lock(&locks[tid]);
          gWhere.at(vtx1) = whereMax;
          gWhere.at(vtx2) = hipart;
          pthread_mutex_unlock(&locks[tid]);
          // pthread_mutex_unlock(&locks[tid]);
//          fprintf(stderr, "\nTID: %d, Change Where ", tid);
          //changewhere
          where[hipart].at(vtx1) = gWhere[vtx1];
          where[hipart].at(vtx2) = gWhere[vtx2];  
      //    pthread_mutex_lock(&locks[tid]);
          where[whereMax].at(vtx1) = gWhere[vtx1];
          where[whereMax].at(vtx2) = gWhere[vtx2];
        //  pthread_mutex_unlock(&locks[tid]);
        }
      }
  //    fprintf(stderr, "\nTID: %d, Before BarWriteInfo ", tid);
    // pthread_barrier_wait(&(barWriteInfo));
    //  fprintf(stderr, "\nTID: %d BEFORE pIdsCompleted[%d][%d]: %d ", tid, hipart, whereMax, pIdsCompleted[hipart][whereMax]);
 //TODO: This should be set true after the entire iteration is complete-- problem addressed below by clear()
	 pIdsCompleted[hipart][whereMax] = true;
    //  fprintf(stderr, "\nTID: %d, AFTER pIdsCompleted: %d ", tid, pIdsCompleted[hipart][whereMax]);

    }  // end of tid loop

    // clearing up for next round of fetch from disk. It should be reset for each call to reduce operation so that each batch is refined against other when fetched from disk each time.
    pIdsCompleted[tid].clear();
    return NULL;
  }

  // void computeBECut(const unsigned tid, std::map<unsigned, unsigned> refineMap)

  void* updateReduceIter(const unsigned tid) {

      //fprintf(stderr, "\nTID: %d, UPDATE reduce ITer ", tid);
      pthread_barrier_wait(&(barCompute));

      refineInit(tid);
      clearMemorystructures(tid);

    ++iteration;
    if(iteration > this->getIterations()){
//      fprintf(stderr, "\nTID: %d, Iteration: %d Complete ", tid, iteration);
      don = true;
       // done.at(tid) = 1;
        //      break;
     }

    fprintf(stderr,"\nTID: %d, iteration: %d ----", tid, iteration);

    return NULL;
  }


  void* afterReduce(const unsigned tid) {
    //fprintf(stderr, "AfterReduce\n");
    fprintf(stderr, "\nthread %u waiting for others to finish Refine\n", tid);
    pthread_barrier_wait(&(barAfterRefine));
 
    if(tid == 0)
      this->gCopy(tid, gWhere);

    stime = 0.0;
    std::string fileName = outputPrefix + std::to_string(tid);
    printParts(tid, fileName.c_str());
    //    ofile.close();
    this->subtractReduceTimes(tid, stime);

    //pthread_barrier_wait(&(barClear));
    refineInit(tid);
    clearMemorystructures(tid);
    //bndIndMap[tid].clear();
    //dTable[tid].clear();
    // totalPECuts[tid] = 0; //will need to reset this once I figure out how to recalculate edge cuts
    //   bndSet = false;
    //cread(tid); //TODO need to find alternate way
      this->readAfterReduce(tid);
     pthread_barrier_wait(&(barRefine));

    if(tid == 0){
      totalCuts = 0;
      //  time_refine += getTimer();
      
      fprintf(stderr,"\n Total EdgeCuts: %d\n", this->countTotalPECut(tid));
      //fprintf(stderr, "pagerank: thread %u iteration %d took %.3lf ms to process %llu vertices and %llu edges\n", tid, iteration, timevalToDouble(e) - timevalToDouble(s), nvertices, nedges);
    }
    pthread_barrier_wait(&(barShutdown));
/*	 if (s == 0) {
            printf("Thread %ld passed barrier SHUTDOWN: return value was 0\n",
                    tid);

        } else if (s == PTHREAD_BARRIER_SERIAL_THREAD) {
            printf("Thread %ld passed barrier SHUTDOWN: return value was "
                    "PTHREAD_BARRIER_SERIAL_THREAD\n", tid);

            usleep(100000);
            printf("\n");

        }
	 else {        
            printf("\npthread_barrier_wait (%ld)", tid);
        }
 */
    if(tid == 0){
    clearRefineStructures();
    }
  return NULL;
  }

  //---------------
  void* readAfterReduce(const unsigned tid) {
   // this->cRead(tid);
   don = false;
   while(!this->getDone(tid)){
      InMemoryContainer<KeyType, ValueType>& container = this->cRead(tid);
   fprintf(stderr, "\nTID: %d, Reading Container iSize: %d" , tid, container.size());

      ComputeBECut(tid, gWhere, bndIndMap[tid], container);
   } 
}

  //---------------
  void writeAfterReduce(const unsigned tid, InMemoryContainer<KeyType, ValueType>& container) {
   this->cWrite(tid, container.size(), container.end());	
   //return container; 
//   }
}
  //---------------
  void ComputeBECut(const unsigned tid, const std::vector<unsigned>& where, InMemTable& bndind, const InMemoryContainer<KeyType, ValueType>& inMemMap) {
    IdType src;
    std::vector<unsigned> bndvert;
      fprintf(stderr, "\nTID: %d, Computing EdgeCuts ", tid);

    for (InMemoryContainerConstIterator<KeyType, ValueType> it = inMemMap.begin(); it != inMemMap.end(); ++it) {
      src = it->first;
      for (std::vector<unsigned>::const_iterator vit = it->second.begin(); vit != it->second.end(); ++vit){
        bndvert.push_back(*vit);
      }
      //  refineMap[tid] = key; // assuming each thread writes at different index
      unsigned nbrs = bndvert.size();
      int countTopK=0;
      int costE = 0;  //count external cost of edge
      //compute the number of edges cut for every key-values pair in the map
      for(auto it = bndvert.begin(); it != bndvert.end(); ++it) { 
        IdType dst = *it;
	//fprintf(stderr,"\nTID: %d, where[%d]: %d != where[%d]: %d ", tid, src, where[src], dst, where[dst]);
        if( where[dst] != INIT_VAL && where[src] != where[dst] ) {
          totalPECuts[tid]++;
          costE++;
          bndind[dst]++; // = costE ;     
        }
      }
      bndvert.clear();

      //calculate d-values
    // fprintf(stderr, "\nTID: %d, Calculate DVals ", tid);
      unsigned costI = nbrs - costE; 
      unsigned dsrc = costE - costI;      // External - Internal cost
      dTable[tid][src] = dsrc; 
    } //end Compute edgecuts main Loop
    //   pthread_barrier_wait(&(barCompute)); //wait for the dvalues from all the threads to be populated
  }

  //---------------
  unsigned computeGain(const unsigned tid, const unsigned hipart, const unsigned whereMax, std::vector<unsigned>& markMax, std::vector<unsigned>& markMin, const InMemoryContainer<KeyType, ValueType>& inMemMap){
    int maxG = 0;
    int maxvtx = -1, minvtx = -1; 
     // fprintf(stderr, "\nTID: %d, Computing GAIN ", tid);
    for (auto it = dTable[hipart].begin(); it != dTable[hipart].end(); ++it) {
        // fprintf(stderr, "\nTId %d dTable hipart Begin: %d, dTable Size %d ----- " , tid,dTable[hipart].begin(), dTable[hipart].size());
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
            for (auto it_hi = dTable[whereMax].begin(); it_hi != dTable[whereMax].end(); ++it_hi) {
              unsigned dst = it_hi->first;
                 // fprintf(stderr, "\nTID: %d, Computing Gain src: %d, dest: %d ", tid, src, dst);
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
                    if (std::find(it_map->second.begin(), it_map->second.end(), dst) == it_map->second.end()){
                        connect = 0;
                    }
                    else
                      connect = 1;
                    // check if src is a boundary vertex, then calculate gain
                   // auto it_bnd = bndIndMap[whereMax].find(src);
                    // if(it_bnd != bndIndMap[whereMax].end()){  
                    //fprintf(stderr, "\nTID: %d, Computing Gain SRC: %d is boundary vertex ", tid, src);
                    int currGain = -1;
                    if(!connect)
                      currGain = dsrc + ddst;
                    else
                      currGain = dsrc + ddst - 2;

       //             fprintf(stderr, "\nTID: %d, src: %d, dst: %d, Gain: %d ", tid, src, dst, currGain);

                    if(currGain > maxG){                                                                                            maxG = currGain;
                      maxvtx = src;
                      minvtx = dst;
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
     //  fprintf(stderr, "\nTID: %d, MASKING src: %d, dst: %d, Gain: %d ", tid, maxvtx, minvtx, maxG);
        markMax.push_back(maxvtx);
        markMin.push_back(minvtx);
        return maxG;
      }
     return -1;
  }


  void printParts(const unsigned tid, std::string fileName) {
    //       std::cout<<std::endl<<"Partition "<< tid <<std::endl;
    //  std::string outputPrefix = "testing";
    ofile.open(fileName);
    assert(ofile.is_open());
    //   std::cout<<"\nFIlename: "<< fileName <<std::endl;
    for(unsigned i = 0; i <= nvertices; ++i){
      if(gWhere[i] != -1 && (gWhere[i] == tid || gWhere[i] == tid % nReducers)){
        //      std::cout<<"\t"<<i << "\t" << gWhere[i]<< std::endl;
        ofile<<i << "\t" << gWhere[i]<< std::endl;
      }
    }
    ofile.close();
  }
 //---------------
              unsigned countTotalPECut(const unsigned tid) {
                //totalCuts = 0;
                for(unsigned i=0; i<nReducers; i++){
                  //for(auto i=fetchPIds.begin(); i != fetchPIds.end(); ++i){
                  totalCuts += totalPECuts[i];
                }

                return (totalCuts/2);
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
                  delete[] totalPECuts;
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
                void gCopy(const unsigned tid, std::vector<unsigned>& gWhere){
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
                      //       fprintf(stderr,"\nGWHERE[%d]: %d", j, gWhere[j]);
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
                thread_local std::ofstream Go<KeyType, ValueType>::ofile;
              template <typename KeyType, typename ValueType>
                thread_local double Go<KeyType, ValueType>::stime;

              template <typename KeyType, typename ValueType>
                void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
                  to.insert(to.end(), from.begin(), from.end());
                  return NULL;
                }

              //-------------------------------------------------
              int main(int argc, char** argv)
              {
                Go<unsigned, unsigned> go;

                //  std::string select = "";
                /*  while(true){
                    std::cout << "Please select `GOMR` for graph Processing or `OMR` for regular MR application" << std::endl;
                    std::cin >> select;
                    if (select == "OMR" | select == "GOMR")
                    break;
                    }*/
                //  std::cout << std::endl;

                if (argc < 8)
                {
                  //   std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems>" << std::endl;
                  //   return 0;
                  // }
                  // else if (select == "GOMR" && argc != 9) { 
                std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems> <optional - nvertices> <optional - partitions> <optional - partition output prefix>" << std::endl;

                return 0;
              }

              std::string folderpath = argv[1];
              int gb = atoi(argv[2]);
              nmappers = atoi(argv[3]);
              nReducers = atoi(argv[4]); // here nreducers = npartitions
              // int npartitions = 2;
              int npartitions;
              //int nvertices;
              //  if (select == "OMR")
              //    nvertices = -1;
#ifdef USE_GOMR
              nvertices = atoi(argv[7]);
              npartitions = atoi(argv[8]); //partitions
              outputPrefix = argv[9];
#else
              nvertices = -1;
              npartitions = 2; ///iterations if not using GOMR
#endif

              int batchSize = atoi(argv[5]);
              int kitems = atoi(argv[6]);

              assert(batchSize > 0);
              //pthread_mutex_init(&countTotal, NULL);

             // nReducers = nreducers;
    //initialize barriers
    pthread_barrier_init(&barRefine, NULL, nReducers);
    pthread_barrier_init(&barCompute, NULL, nReducers);
    pthread_barrier_init(&barEdgeCuts, NULL, nReducers);
    pthread_barrier_init(&barWriteInfo, NULL, nReducers);
    pthread_barrier_init(&barClear, NULL, nReducers);
    pthread_barrier_init(&barShutdown, NULL, nReducers);
    pthread_barrier_init(&barAfterRefine, NULL, nReducers);
              go.init(folderpath, gb, nmappers, nReducers, nvertices, batchSize, kitems, npartitions);

              double runTime = -getTimer();
              go.run(); 
              runTime += getTimer();

              std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
              //std::cout << "Total words: " << countTotalWords << std::endl;
              return 0;
              }

