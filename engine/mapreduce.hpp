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
  //std::cout << "DoMap tid, *mr:" << tid << "\n";  //GK

  mr->beforeMap(tid);

  static std::atomic<unsigned> nextFileId(0);
  unsigned fileId = 0;
  while((fileId = nextFileId++) < mr->fileList.size()) {
    std::string fname = mr->inputFolder + "/" + mr->fileList.at(fileId);
    if(fileId % 1000 == 0) fprintf(stderr, "thread %u working on file %d which is %s\n", tid, fileId, fname.c_str());
    std::ifstream infile(fname.c_str());
    ASSERT_WITH_MESSAGE(infile.is_open(), fname.c_str());
    std::string line;
    while(std::getline(infile, line)) {
      time_map -= getTimer();
      mr->map(tid, fileId, line);
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

  mr->readInit(tid);

  while(true) {
    bool execLoop = mr->read(tid);
    if(execLoop == false) {
      for(InMemoryContainerConstIterator<KeyType, ValueType> it = writer.readBufMap[tid].begin(); it != writer.readBufMap[tid].end(); ++it)
        mr->reduce(tid, it->first, it->second);
      break;
    }

    unsigned counter = 0; 
    InMemoryContainerIterator<KeyType, ValueType> it;
    // send top-k (least) elements to reducer
    for (it = writer.readBufMap[tid].begin(); it != writer.readBufMap[tid].end(); ++it) {
      if (counter >= mr->kBItems)
        break;

      mr->reduce(tid, it->first, it->second);

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

  InMemoryReductionState<KeyType, ValueType> state = writer.initiateInMemoryReduce(tid); 

  InMemoryContainer<KeyType, ValueType> record;
  while(writer.getNextMinKey(&state, &record)) {
    mr->reduce(tid, record.begin()->first, record.begin()->second);
    record.clear();
  }

  mr->afterReduce(tid);

  time_reduce += getTimer();
  mr->reduce_times[tid] += time_reduce;

  fprintf(stderr, "thread %u finished in-memory reduction\n", tid);

  return NULL;
}

//=============================================
// Member Functions
//+++++++++++++++++++++++++++++++++++++++++++++
/*  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::setInput(const std::string input)
{
  inputFolder = input;
}
*/
/*
//--------------------------------------------
void MapReduce::parallelExecuteMappers(void *(*func)(void *), void* arg)
{
parallelExecute(func, arg, nMappers);
}

//--------------------------------------------
void MapReduce::parallelExecuteReducers(void *(*func)(void *), void* arg)
{
//std::cout << "Inside ParallelExecuteReducers..\n";
parallelExecute(func, arg, nReducers);
}
 */

//--------------------------------------------
  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::run()
{
  fprintf(stderr, "initializing");

  fprintf(stderr, "Init MR Writer in-Memory Buffers\n");
  double init_time = -getTimer();
  writer.initBuf(nMappers, nReducers, nVertices, batchSize, kBItems); // GK 

  map_times.resize(nMappers, 0.0);
  reduce_times.resize(nReducers, 0.0);
  writeBuf_times.resize(nMappers, 0.0);
  flushResidues_times.resize(nMappers, 0.0);
  infinimem_read_times.resize(nReducers, 0.0);
  infinimem_write_times.resize(nMappers, 0.0);
  localCombinedPairs.resize(nMappers, uint64_t(0));

  init_time += getTimer();

  fprintf(stderr, "Running Mappers\n");
  parallelExecute(doMap<KeyType, ValueType>, this, nMappers);

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

  //writer.shutdown();

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

  //        auto total_WBIf = max_element(std::begin(WBIf), std::end(WBIf)); 
  //	std::cout << " Total WBIf time : " << *total_WBIf << " (msec)" << std::endl;
  //        auto total_WBElse = max_element(std::begin(WBElse), std::end(WBElse)); 
  //	std::cout << " Total WBElse time : " << *total_WBElse << " (msec)" << std::endl;
  //      auto total_WBFind = max_element(std::begin(WBFind), std::end(WBFind)); 
  //	std::cout << " Total WBFind time : " << *total_WBFind << " (msec)" << std::endl;
  //        auto total_tLock = max_element(std::begin(tLock), std::end(tLock)); 
  //	std::cout << " Total tLock time : " << *total_tLock << " (msec)" << std::endl;
  std::cout << std::endl;
}

/*
//--------------------------------------------
  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::setMappers(const unsigned mappers)
{
  nMappers = mappers;
}

//--------------------------------------------
  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::setReducers(const unsigned reducers)
{
  nReducers = reducers;
}

//--------------------------------------------  GK
  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::setBatchSize(const unsigned bSize)
{
  batchSize = bSize;
}

//--------------------------------------------  GK
  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::setkItems(const unsigned kItems)
{
  kBItems = kItems;
}

template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::setGB(const unsigned g)
{
  gb = g;
}
*/

template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::init(const std::string input, const unsigned g, const unsigned mappers, const unsigned reducers, const unsigned vertices, const unsigned bSize, const unsigned kItems) {
  inputFolder = input;
  nMappers = mappers;
  nReducers = reducers;
  nVertices = vertices;
  batchSize = bSize;
  kBItems = kItems;
  gb = g;

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

  std::cout << "nMappers: " << nMappers << std::endl;
  std::cout << "nReducers: " << nReducers << std::endl;
  std::cout << "batchSize: " << batchSize << std::endl;
  std::cout << "topk: " << kBItems << std::endl;

  pthread_barrier_init(&barMap, NULL, nMappers);
  pthread_barrier_init(&barReduce, NULL, nReducers);
}

//--------------------------------------------  GK
  template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::writeBuf(const unsigned tid, const KeyType& key, const ValueType& value)
{
  writer.writeBuf(tid, key, value);
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

//--------------------------------------------
template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::readInit(const unsigned tid) {
  return writer.readInit(tid);
}

//--------------------------------------------

template <typename KeyType, typename ValueType>
void MapReduce<KeyType, ValueType>::subtractReduceTimes(const unsigned tid, double stime) {
  reduce_times[tid] -= stime;
}
