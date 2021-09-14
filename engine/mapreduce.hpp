#include "mapreduce.h"
#include "mapwriter.hpp"
#include <utility> // for std::pair
#include <fstream>
#include <iostream> //GK
#include <pthread.h>
#include <atomic>
#include <vector>
#include <string>
#include <ctime>
#include <numeric>

//--------------------------------------------
// Helper NON-member Functions
// Facade for BSP style parallel execution for Mappers and then Reducers
void parallelExecute(void *(*func)(void *), void* arg, unsigned threads)
{
  pthread_t thread_handles[threads];
  std::pair<unsigned, void*> args[threads];

  for (unsigned i = 0; i < threads; i++)
  {
    args[i] = std::make_pair(i, arg);
    pthread_create(&thread_handles[i], NULL, func, &args[i]);
  }

  for (unsigned i = 0; i < threads; i++)
  {
    pthread_join(thread_handles[i], NULL);
  }

}

//---------------------------------------------
// Mapper driver
  template <typename KeyType, typename ValueType>
void* doMap(void* arg)
{
  double time_map = 0.0;
  unsigned tid = static_cast<unsigned>(static_cast<std::pair<unsigned, void*>*>(arg)->first);
  MapReduce<KeyType, ValueType> *mr = static_cast<MapReduce<KeyType, ValueType> *>(static_cast<std::pair<unsigned, void*>*>(arg)->second);
  MapWriter<KeyType, ValueType>& writer = mr->writer;
  // std::cout << "DoMap tid, *mr:" << tid << "\n";  //GK

  mr->beforeMap(tid);
/* read from single file
  std::ifstream infile(mr->inputFileName.c_str()); 
  assert(infile.is_open());
  fprintf(stderr,"\nTid: %d Input file: %s\n",tid, mr->inputFileName.c_str());
infile.seekg(std::ios::beg);
unsigned lineId = tid*mr->linesPerThread + tid ;//0, 4, 8
    //fprintf(stderr,"\n ********** TID: %d, lineID %d \n", tid, lineId+1);
        mr->end_read[tid] = (tid+1)*mr->linesPerThread + tid; //4, 7, 9

  std::string line;

  if(tid > 0){
    infile.seekg(std::ios::beg);
              for(int i=0; i < lineId; ++i){  //remove -1 if starting from 0
                          infile.ignore(std::numeric_limits<std::streamsize>::max(),'\n');
                                      }
    }

 //fprintf(stderr,"\n ********** TID: %d, lineID %d, end_read: %d \n", tid, lineId+1, mr->end_read[tid]);
  while(std::getline(infile, line, '\n')){
   // fprintf(stderr,"\n ********** TID: %d, lineID %d \n", tid, lineId+1);
    if( lineId < mr->nVertices && lineId <= mr->end_read[tid]){
      unsigned nbufferId = mr->setPartitionId(tid);
      mr->map(tid, line, ++lineId, nbufferId);     
    }                                                                                                           else
      break;
  }
*/
  // read from multiple files
     static std::atomic<unsigned> nextFileId(0);
     unsigned fileId = 0;
     while((fileId = nextFileId++) < mr->fileList.size()) {
      std::string fname = mr->inputFolder + "/" + mr->fileList.at(fileId);
      if(fileId % 1000 == 0) fprintf(stderr, "thread %u working on file %d which is %s\n", tid, fileId, fname.c_str());
      //fprintf(stderr, "thread %u working on file %d which is %s\n", tid, fileId, fname.c_str());
      std::ifstream infile(fname.c_str());
      ASSERT_WITH_MESSAGE(infile.is_open(), fname.c_str());
      std::string line;
      while(std::getline(infile, line)) {
       time_map -= getTimer();
       //NOTE: input file must be numbered for Graphs when using fileIds
#ifdef USE_GOMR
       unsigned nbufferId = mr->setPartitionId(tid);
       mr->map(tid, fileId, line, nbufferId, mr->hiDegree);
#else
       mr->map(tid, fileId, line);
#endif 
       time_map += getTimer();
      }
     }
   
  fprintf(stderr, "thread %u waiting for others to finish work\n", tid);
  time_map -= getTimer();
  pthread_barrier_wait(&(mr->barMap));

  if(writer.getWrittenToDisk()) {
    mr->writer.flushMapResidues(tid);
  }

  mr->afterMap(tid);

  time_map += getTimer();

  mr->map_times[tid] = time_map;

  return NULL;
}

//---------------------------------------------
// Reducer driver
  template <typename KeyType, typename ValueType>
void* doReduce(void* arg)
{
  double time_reduce = -getTimer();

  unsigned tid = static_cast<unsigned>(static_cast<std::pair<unsigned, void*>*>(arg)->first);
  MapReduce<KeyType, ValueType> *mr = static_cast<MapReduce<KeyType, ValueType> *>(static_cast<std::pair<unsigned, void*>*>(arg)->second);
  MapWriter<KeyType, ValueType>& writer = mr->writer; 

  mr->beforeReduce(tid);

  don = false;
  //fprintf(stderr,"\nTID: %d MR BEFORE Outer While **********", tid);
  while(!mr->getDone(tid)){
    fprintf(stderr,"\nTID: %d MR Outer While Don: %d **********", tid, don);
    don = true;
    mr->readInit(tid);

    while(true) {
      bool execLoop = mr->read(tid);
      //fprintf(stderr,"\nMR TID: %d Inner While Don: %d, ExecL: %d **********", tid, don, execLoop);
      if(execLoop == false) {
#ifdef USE_GOMR
        fprintf(stderr,"\nTID: %d LAST Batch  map size: %d", tid, writer.readBufMap[tid].size());
        assert(writer.readBufMap[tid].size() != 0);
        mr->reduce(tid, writer.readBufMap[tid]);
//	mr->cWrite(tid);
	writer.cWrite(tid, writer.readBufMap[tid].size(), writer.readBufMap[tid].end());
#else
        for(InMemoryContainerConstIterator<KeyType, ValueType> it = writer.readBufMap[tid].begin(); it != writer.readBufMap[tid].end(); ++it)
          mr->reduce(tid, it->first, it->second);
#endif
        break;
      }

      unsigned counter = 0; 
#ifdef USE_GOMR
      InMemoryContainer<KeyType, ValueType> refineMap;
#endif
      InMemoryContainerIterator<KeyType, ValueType> it;
      // send top-k (least) elements to reducer
      for (it = writer.readBufMap[tid].begin(); it != writer.readBufMap[tid].end(); ++it) {
        if (counter >= mr->kBItems){
#ifdef USE_GOMR
          mr->reduce(tid, refineMap);
          refineMap.clear();
#endif
          break;
        }
        // fprintf(stderr, "TID: %d,reducing  \n", tid);
#ifdef USE_GOMR
        refineMap[it->first] = it->second;
#else
        mr->reduce(tid, it->first, it->second);
#endif
        const KeyType& key = it->first;
        auto pos = writer.lookUpTable[tid].find(key);
        assert(pos != writer.lookUpTable[tid].end());
        const std::vector<unsigned>& lookVal = pos->second;
        for(unsigned val=0; val<lookVal.size(); val++)
        {
          writer.fetchBatchIds[tid].insert(lookVal[val]);
          writer.keysPerBatch[tid][lookVal[val]] += 1; 
        }
        //writer.lookUpTable[tid][key].clear();
        writer.lookUpTable[tid].erase(key);
        counter++;
      }

      writer.readBufMap[tid].erase(writer.readBufMap[tid].begin(), it);
    }

    //writer.readBufMap[tid].erase(writer.readBufMap[tid].begin(), writer.readBufMap[tid].end());
#ifdef USE_GOMR
	writer.cWrite(tid, writer.readBufMap[tid].size(), writer.readBufMap[tid].end());
#endif
    writer.readClear(tid);
    mr->updateReduceIter(tid);
  }
  //fprintf(stderr, "thread %u OUT of LOOPS\n", tid);
  mr->afterReduce(tid);

  time_reduce += getTimer();
  mr->reduce_times[tid] += time_reduce;

  fprintf(stderr, "thread %u finished reduction\n", tid);
  return NULL;
}

//---------------------------------------------
// In Memory Reducer driver
template <typename KeyType, typename ValueType>
void* doInMemoryReduce(void* arg) {
  double time_reduce = -getTimer();

  unsigned tid = static_cast<unsigned>(static_cast<std::pair<unsigned, void*>*>(arg)->first);
  MapReduce<KeyType, ValueType> *mr = static_cast<MapReduce<KeyType, ValueType> *>(static_cast<std::pair<unsigned, void*>*>(arg)->second);
  MapWriter<KeyType, ValueType>& writer = mr->writer;

  mr->beforeReduce(tid);
  don = false;
 // fprintf(stderr,"\nTID: %d InMem-MR BEFORE Outer While **********", tid);

  while(!mr->getDone(tid)){
   // fprintf(stderr,"\nTID: %d, InMem-MR Outer While Don: %d **********", tid, don);
    don = true;

    InMemoryReductionState<KeyType, ValueType> state = writer.initiateInMemoryReduce(tid); 

    InMemoryContainer<KeyType, ValueType> record;
/*#ifdef USE_GOMR
    InMemoryContainer<KeyType, ValueType> refineMap;
#endif
*/
    while(writer.getNextMinKey(&state, &record)) {
#ifdef USE_GOMR
      //refineMap[record.begin()->first] = record.begin()->second;
      writer.readBufMap[tid][record.begin()->first] = record.begin()->second;
#else
      mr->reduce(tid, record.begin()->first, record.begin()->second);
#endif
      record.clear();
    }
#ifdef USE_GOMR
  //    fprintf(stderr,"\nMR TID: %d Inner While Don: %d, readmap: %d **********", tid, don, writer.readBufMap[tid].size());
   // mr->reduce(tid, refineMap);
    mr->reduce(tid, writer.readBufMap[tid]);
#endif

    mr->updateReduceIter(tid);
  } //end outer while loop

  mr->afterReduce(tid);

  time_reduce += getTimer();
  mr->reduce_times[tid] += time_reduce;

  fprintf(stderr, "thread %u finished in-memory reduction\n", tid);

  return NULL;
}

//=============================================
// Member Functions
//+++++++++++++++++++++++++++++++++++++++++++++
//--------------------------------------------
  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::run()
{
  fprintf(stderr, "initializing");

  fprintf(stderr, "Init MR Writer in-Memory Buffers\n");
  double init_time = -getTimer();
  writer.initBuf(nMappers, nReducers, nVertices, hiDegree, batchSize, kBItems); // GK 
  fprintf(stderr, "Partitioning input for Parallel Reads\n");
    partitionInputForParallelReads();
end_read.resize(nMappers, 0);

  map_times.resize(nMappers, 0.0);
  reduce_times.resize(nReducers, 0.0);
  writeBuf_times.resize(nMappers, 0.0);
  flushResidues_times.resize(nMappers, 0.0);
  infinimem_read_times.resize(nReducers, 0.0);
  infinimem_write_times.resize(nMappers, 0.0);
  localCombinedPairs.resize(nMappers, uint64_t(0));
  infinimem_cread_times.resize(nReducers, 0.0);
  infinimem_cwrite_times.resize(nReducers, 0.0);

  init_time += getTimer();

  fprintf(stderr, "\nInitializing ..\n");
#ifdef USE_GOMR 
  writer.writeInit();
#endif

  fprintf(stderr, "Running Mappers\n");
  parallelExecute(doMap<KeyType, ValueType>, this, nMappers);

   //assert(false);
  if(!writer.getWrittenToDisk()) {
    fprintf(stderr, "Running InMemoryReducers\n");
    parallelExecute(doInMemoryReduce<KeyType, ValueType>, this, nReducers);
    writer.releaseMapStructures();
  } else {
    writer.releaseMapStructures();
    fprintf(stderr, "Running Reducers\n");
    parallelExecute(doReduce<KeyType, ValueType>, this, nReducers);
  }

  fprintf(stderr, "MR Task complete. Shutting down.\n");

  fprintf(stderr, "--------------------------------------\n");

  writer.shutdown();

  std::cout << "------- Final Time ---------" << std::endl;
  std::cout << " Init time : " << init_time << " (msec)" << std::endl;

  auto map_time = max_element(std::begin(map_times), std::end(map_times)); 
  std::cout << " Map time : " << *map_time << " (msec)" << std::endl;

  auto reduce_time = max_element(std::begin(reduce_times), std::end(reduce_times));
  std::cout << " Reduce time : " << *reduce_time << " (msec)" << std::endl;

  std::cout << " Map+Reduce time : " << ((*map_time) + (*reduce_time)) << " (msec)" << std::endl;
  std::cout << " Init+Map+Reduce time : " << ((*map_time) + (*reduce_time) + init_time) << " (msec)" << std::endl;

  uint64_t combinedPairs = std::accumulate(std::begin(localCombinedPairs), std::end(localCombinedPairs), uint64_t(0));
  std::cout << " Total combined pairs : " << combinedPairs << std::endl;

  auto writeBuf_time = max_element(std::begin(writeBuf_times), std::end(writeBuf_times)); 
  std::cout << " writeBuf time : " << *writeBuf_time << " (msec)" << std::endl;
  auto flushResidues_time = max_element(std::begin(flushResidues_times), std::end(flushResidues_times));
  std::cout << " flushResidues time : " << *flushResidues_time << " (msec)" << std::endl;

  auto infinimem_read_time = max_element(std::begin(infinimem_read_times), std::end(infinimem_read_times));
  std::cout << " InfiniMem Read time: " << *infinimem_read_time << " (msec)" << std::endl;
  auto infinimem_write_time = max_element(std::begin(infinimem_write_times), std::end(infinimem_write_times));
  std::cout << " InfiniMem Write time: " << *infinimem_write_time << " (msec)" << std::endl;
  
  auto infinimem_cread_time = max_element(std::begin(infinimem_cread_times), std::end(infinimem_cread_times));
  std::cout << " InfiniMem Sequential Read time: " << *infinimem_cread_time << " (msec)" << std::endl;

  auto infinimem_cwrite_time = max_element(std::begin(infinimem_cwrite_times), std::end(infinimem_cwrite_times));
  std::cout << " InfiniMem Sequential Write time: " << *infinimem_cwrite_time << " (msec)" << std::endl;


  std::cout << std::endl;
}


 //--------------------------------------------
 template <typename KeyType, typename ValueType>
 void MapReduce<KeyType, ValueType>::partitionInputForParallelReads() {
    // Get size of input file
      std::ifstream in(inputFileName.c_str(), std::ifstream::ate | std::ifstream::binary); assert(in.is_open());
        size_t fileSizeInBytes = in.tellg();
          fprintf(stderr, "fileSizeInBytes: %zu\n", fileSizeInBytes);
            //fprintf(stderr, "\nnumlines: %u\n", numLines);
              in.close();
                fprintf(stderr,"\n NumLines: %zu", nVertices);
                  linesPerThread = nVertices/nMappers;
                    fprintf(stderr,"\nLinesPerThread: %zu", linesPerThread);
                      bytesPerFile = fileSizeInBytes/nMappers + 1; //fileSizeInBytes/nThreads + 1;
 }

//--------------------------------------------  GK
template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::init(const std::string input, const unsigned g, const unsigned mappers, const unsigned reducers, const unsigned vertices, const unsigned hidegree, const unsigned bSize, const unsigned kItems, const unsigned iterations) {
  inputFolder = input;
  inputFileName = input;
  nMappers = mappers;
  nReducers = reducers;
  nVertices = vertices;
  hiDegree = hidegree;
  batchSize = bSize;
  kBItems = kItems;
  gb = g;
  //unsigned int nIterations = UINT_MAX;
  nIterations = iterations;

  getListOfFiles(inputFolder, &fileList);
  reduceFiles(inputFolder, &fileList, gb);
  std::cout << "Number of files: " << fileList.size() << std::endl;

  if(fileList.size() == 0) {
    std::cout << "No work to be done" << std::endl;
    exit(0);
  }

  printFileNames(inputFolder, &fileList, gb);
  std::cout << "Dataset size: " << gb << " GB" << std::endl;

  //setThreads(std::min(static_cast<unsigned>(fileList.size()), nThreads));
  nMappers = std::min(static_cast<unsigned>(fileList.size()), nMappers);
  nReducers = std::min(nMappers, nReducers);
  //template <typename KeyType, typename ValueType>
  std::cout << "nMappers: " << nMappers << std::endl;
  std::cout << "nReducers: " << nReducers << std::endl;
  std::cout << "batchSize: " << batchSize << std::endl;
  std::cout << "topk: " << kBItems << std::endl;

  pthread_barrier_init(&barMap, NULL, nMappers);
  pthread_barrier_init(&barReduce, NULL, nReducers);
}

//--------------------------------------------  GK
/*  template <typename KeyType, typename ValueType>
int MapReduce<KeyType, ValueType>::setPartitionId(const unsigned tid)
{
  return setPartitionId(tid);
}
*/
//--------------------------------------------  GK
  template <typename KeyType, typename ValueType>
int MapReduce<KeyType, ValueType>::getRows()
{
  return nMappers;
}

//--------------------------------------------  GK
  template <typename KeyType, typename ValueType>
int MapReduce<KeyType, ValueType>::getCols()
{
  return nReducers;
}

//--------------------------------------------  GK
  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::writeBuf(const unsigned tid, const KeyType& key, const ValueType& value, const unsigned nbufferId, const unsigned hidegree)
{
  writer.writeBuf(tid, key, value, nbufferId, hidegree);
}

//--------------------------------------------
/*
   template <typename KeyType, typename ValueType>
   bool MapReduce<KeyType, ValueType>::read(const unsigned tid, InMemoryContainer<KeyType, ValueType>& readBufMap, std::vector<int>& keysPerBatch, LookUpTable<KeyType>& lookUpTable, std::queue<int>& fetchBatchIds)
   {
   return writer.read(tid, readBufMap, keysPerBatch, lookUpTable, fetchBatchIds);
   }
 */

template <typename KeyType, typename ValueType>
bool MapReduce<KeyType, ValueType>::read(const unsigned tid) {
  return writer.read(tid);
}

template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::cWrite(const unsigned tid) {
  writer.cWrite(tid);
}

//--------------------------------------------
template <typename KeyType, typename ValueType>
InMemoryContainer<KeyType, ValueType>& MapReduce<KeyType, ValueType>::cRead(const unsigned tid) {
  return writer.cRead(tid);
}

//--------------------------------------------
template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::readInit(const unsigned tid) {
  writer.readInit(tid);
}

//--------------------------------------------

template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::subtractReduceTimes(const unsigned tid, double stime) {
  reduce_times[tid] -= stime;
}
