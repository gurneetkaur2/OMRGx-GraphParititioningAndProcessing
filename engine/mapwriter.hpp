#include "mapwriter.h"

#ifdef USE_ONE_PHASE_IO
#include "infinimem/onePhaseFileIO.hpp"
#else
#include "infinimem/twoPhaseFileIO.hpp"
#endif

#include <vector>
#include <ctype.h> // for tolower()
#include <algorithm> // for std::sort()
#include <stack> // for lookuptable
#include <fstream>  // GK
#include<iostream> //GK
#include<iterator> //GK
#include <utility>//GK

//pthread_mutex_t lock_buffer = PTHREAD_MUTEX_INITIALIZER;
//------------------------------------------------- GK
// Initialize in-memory Buffers
  template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::initBuf(unsigned nMappers, unsigned nReducers, unsigned nVertices, unsigned hidegree, unsigned bSize, unsigned kItems)
{
  //nBuffers = pow(buffers, 2); // number of buffers is square of number of threads
  //nBuffers = nMappers * nReducers;
  nVtces = nVertices;
  hiDegree = hidegree;
  nRows = nMappers;
  nCols = nReducers;
  writtenToDisk = false;
//  don = true;
  batchSize = bSize;
  kBItems = kItems;
  //cTotalKeys = new IdType[nBuffers];
  totalKeysInFile = new IdType[nCols];
  //nReadKeys = new IdType[nBuffers];
  totalCombined = new IdType[nCols];
  nItems = new IdType[nRows * nCols];
  readNext = new IdType[nCols];
//  prev = new std::vector<unsigned>[nCols];
//  next = new std::vector<unsigned>[nCols];

  outBufMap = new InMemoryContainer<KeyType, ValueType>[nRows * nCols];
  readBufMap = new InMemoryContainer<KeyType, ValueType>[nCols];
  lookUpTable = new LookUpTable<KeyType>[nCols];
  fetchBatchIds = new std::set<unsigned>[nCols];
  readNextInBatch = new std::vector<unsigned long long>[nCols];
  batchesCompleted = new std::vector<bool>[nCols];
  keysPerBatch = new std::vector<unsigned>[nCols];
  
  for (unsigned i=0; i<nRows * nCols; ++i) 
    nItems[i] = 0;
  
  for(unsigned i=0; i<nCols; ++i) {  
    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);
    locks.push_back(mutex);
    
    totalKeysInFile[i] = 0;
  }

  // setup FileIO
#ifdef USE_ONE_PHASE_IO
  io = new OnePhaseFileIO<RecordType>("/tmp/gkaur007/mrdata/", nCols, 0/*UNUSED*/);
#else
  io = new TwoPhaseFileIO<RecordType>("/tmp/gkaur007/mrdata/", nCols, 0/*UNUSED*/);
#endif
  cio = new TwoPhaseFileIO<RecordType>("/tmp/gkaur007/combdata/", nCols, 0/*UNUSED*/);
}

//-------------------------------------------------
  template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::releaseMapStructures()
{
  for (unsigned i = 0; i < nCols; i++)
    pthread_mutex_destroy(&locks[i]);

  //AK : Changed the counter to locks.size instead because it was giving seg- faults
  for (unsigned i = 0; i < nRows * nCols; i++) {
    outBufMap[i].clear();
  }

  //delete[] cTotalKeys;
  delete[] nItems;
  delete[] outBufMap;
}
//-------------------------------------------------
  template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::shutdown()
{
  delete io;
  delete cio;
  //AK : segfault clear
  //	outBufMap->clear();
  readBufMap->clear();
  for (unsigned i = 0; i < nCols; i++){
     //  readBufMap[i].clear();
       readNextInBatch[i].clear();
  //     prev[i].clear();
  //     next[i].clear();
  }

  //	delete[] cTotalKeys;
  //	delete[] nItems;
  //	delete[] outBufMap;
  delete[] readNext;
  delete[] totalKeysInFile;
  //delete[] nReadKeys;
  delete[] totalCombined;
  delete[] readBufMap;
  delete[] lookUpTable;
  delete[] fetchBatchIds;
  delete[] readNextInBatch;
 // delete[] prev;
 // delete[] next;
  delete[] batchesCompleted;
  delete[] keysPerBatch;
}

//------------------------------------------------- GK
// Initialize the next and prev arrays which will contain the values from next and prev iteration
//
template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::writeInit() {
    fprintf(stderr,"\n TID nParts %d vertices %d ", nCols, nVtces);
  //for (unsigned i = 0; i<nCols; ++i) {
    for (unsigned j = 0; j<=nVtces; ++j) {
         nNbrs.push_back(0); 
         //done.push_back(0); 
        // next[i].push_back(-1); 
        }
    // } 

}

//------------------------------------------------- GK
// Write the Map output to in-memory Buffer
// Gets Hashing function from Application Programmer
template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::writeBuf(const unsigned tid, const KeyType& key, const ValueType& value, const unsigned nbufferId, const unsigned hidegree) {
  double timeWBF = -getTimer();
  //unsigned bufferId = (tolower(word[0]) - 'a') % nCols; // values 0, 1, 2 at most = numThreads
  //unsigned bufferId = hashKey(key) % nCols; // values 0, 1, 2 at most = numThreads
 // int bufferId = -1;
 unsigned bufferId = nbufferId;
  if(bufferId == -1)
    bufferId = hashKey(key) % nCols;
  unsigned buffer = tid * nCols + bufferId;  // calculate the actual buffer to write in

  if(hidegree != 0){
    bufferId = pid++ % nCols;
    unsigned buffer = tid * nCols + bufferId;  // calculate the actual buffer to write in 
  }

/*#ifdef USE_GOMR
  if (nVtces > 0 ){
     unsigned part = tid % nCols;
    // unsigned vid = static_cast<unsigned>(key);    // key should be vertex number for graphs
     unsigned vid = key;
    //    fprintf(stderr, "\n1 --previous value of %d is %d \n", vid, (1/nVtces));
     if(prev[part].at(vid) == -1){
     	prev[part].at(vid) = 1 / nVtces;
      //  fprintf(stderr, "\nprevious value of %d is %d \n", vid, (1/nVtces));
  	}
  }
#endif
*/
/* // I may not need the below code because in mapreduce value would not necessary be a useful vertex
   // TODO: I can add this check for graph processing only where I can also add the PRank for the from vertex

  unsigned whereVal = hashKey(value) % nCols; 
    if(prev[part].at(value) == -1){
      prev[part].at(value) = whereVal;
  }
*/
//  	fprintf(stderr, "\nWB- start outbufmap size : %d\t buffer: %llu\t nItems: %u", outBufMap[buffer].size(),buffer, nItems[buffer]);
  
  if (outBufMap[buffer].size() >= batchSize)   
  {
//    fprintf(stderr, "thread %u flushing off buffer %llu to file %llu\n", tid, buffer, bufferId);
    
    infinimem_write_times[tid] -= getTimer();
    pthread_mutex_lock(&locks[bufferId]);
    writeToInfinimem(bufferId, totalKeysInFile[bufferId], outBufMap[buffer].size(), outBufMap[buffer]);
    totalKeysInFile[bufferId] += nItems[buffer];
    pthread_mutex_unlock(&locks[bufferId]);
    infinimem_write_times[tid] += getTimer();
    
    outBufMap[buffer].clear();
    nItems[buffer] = 0;
    writtenToDisk = true;
  }

  performWrite(tid, buffer, key, value);

  timeWBF += getTimer();
  writeBuf_times[tid] += timeWBF;
}

//-------------------------------------------
template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::flushMapResidues(const unsigned tid) {
  if(tid >= nCols) 
    return;
  
  fprintf(stderr, "thread %u doing flushMapResidues\n", tid);
  double frTime = -getTimer();
  assert(writtenToDisk);

  if(nRows == 1) {
    // write out entire single buffer
    infinimem_write_times[tid] -= getTimer();
    writeToInfinimem(tid, totalKeysInFile[tid], static_cast<unsigned>(outBufMap[tid].size()), outBufMap[tid]);
    infinimem_write_times[tid] += getTimer();
    outBufMap[tid].clear();
    totalKeysInFile[tid] += nItems[tid];
    nItems[tid] = 0;
  }
  else {
    // Always merge buffers from b2 to b1
    unsigned b1 = 0, b2 = 0;
    InMemoryContainerIterator<KeyType, ValueType> b2Iter, b2End;
    unsigned long long b2Merged = 0;
    bool findB1 = true, findB2 = true;
    unsigned i = 0;
    while(i < nRows - 1) {
      if(findB1) {
        b1 = (i * nCols) + tid;
        findB1 = false;
        ++i;
      }
      if(findB2) {
        b2 = (i * nCols) + tid;
        b2Iter = outBufMap[b2].begin();
        b2End = outBufMap[b2].end();
        b2Merged = 0;
        findB2 = false;
        ++i;
      }

      b2Merged += merge(outBufMap[b1], b1, tid, b2Iter, b2End);

      if(outBufMap[b1].size() == 0) {
        findB1 = true;
      }
      if(b2Iter == b2End) {
        findB2 = true;
      }
    }

    // exit conditions
    if(i == nRows - 1) {     // CASE 1
      // Last buffer needs to be merged/written
      if(findB1 && findB2) {            // CASE 1.1
        // this means i is the only remaining buffer. so write out i completely
        infinimem_write_times[tid] -= getTimer();
        writeToInfinimem(tid, totalKeysInFile[tid], outBufMap[i].size(), outBufMap[i]);
        infinimem_write_times[tid] += getTimer();
        outBufMap[i].clear();
        totalKeysInFile[tid] += nItems[i];
        nItems[i] = 0;
      }
      else if(findB1 || findB2) {       // CASE 1.2
        if(findB1) {
          b1 = (i * nCols) + tid;
          findB1 = false;
          ++i;
        } else { // this is: if(findB2)
          b2 = (i * nCols) + tid;
          b2Iter = outBufMap[b2].begin();
          b2End = outBufMap[b2].end();
          b2Merged = 0;
          findB2 = false;
          ++i;
        }  

        b2Merged += merge(outBufMap[b1], b1, tid, b2Iter, b2End);
        if(outBufMap[b1].size() == 0) {
          if(b2Iter != b2End) {
            // write out b2 from b2Iter to b2End
            infinimem_write_times[tid] -= getTimer();
            betterWriteToInfinimem(tid, totalKeysInFile[tid], outBufMap[b2].size() - b2Merged, b2Iter, b2End);
            infinimem_write_times[tid] += getTimer();
            outBufMap[b2].clear();
            totalKeysInFile[tid] += (nItems[b2] - b2Merged);
            nItems[b2] = 0;
          }
        } 
        if(b2Iter == b2End) {
          if(outBufMap[b1].size() != 0) {
            // write out b1 completely
            infinimem_write_times[tid] -= getTimer();
            writeToInfinimem(tid, totalKeysInFile[tid], outBufMap[b1].size(), outBufMap[b1]);
            infinimem_write_times[tid] += getTimer();
            outBufMap[b1].clear();
            totalKeysInFile[tid] += nItems[b1];
            nItems[b1] = 0;
          }
        }
      } else {                          // CASE 1.3
        // this is: if((!findB1) && (!findB2))
        // this can never happen
        assert(false);
      }
    } else if(i == nRows) {  // CASE 2
      // Either b1 or b2 need to be written out
      if(outBufMap[b1].size() > 0) {
        // write out b1 completely
        infinimem_write_times[tid] -= getTimer();
        writeToInfinimem(tid, totalKeysInFile[tid], outBufMap[b1].size(), outBufMap[b1]);
        infinimem_write_times[tid] += getTimer();
        outBufMap[b1].clear();
        totalKeysInFile[tid] += nItems[b1];
        nItems[b1] = 0;
      } else {  
        // this is: if(b2Iter != b2End)
        // write out b2 from b2Iter to b2End
        infinimem_write_times[tid] -= getTimer();
        betterWriteToInfinimem(tid, totalKeysInFile[tid], outBufMap[b2].size() - b2Merged, b2Iter, b2End);
        infinimem_write_times[tid] += getTimer();
        outBufMap[b2].clear();
        totalKeysInFile[tid] += (nItems[b2] - b2Merged);
        nItems[b2] = 0;
      }
    } else {                            // CASE 3
      // this can never happen
      assert(false);
    } // end of exit conditions
  }
  // KEVAL: END OF CRAZY ELSE CONDITION
  frTime += getTimer();
  flushResidues_times[tid] += frTime;
}

template <typename KeyType, typename ValueType>
unsigned long long MapWriter<KeyType, ValueType>::merge(InMemoryContainer<KeyType, ValueType>& toMap, unsigned whichMap, unsigned tid, InMemoryContainerIterator<KeyType, ValueType>& begin, InMemoryContainerConstIterator<KeyType, ValueType> end) {
  unsigned long long ct = 0;
  while(begin != end) {
    if(toMap.size() >= batchSize) {
      infinimem_write_times[tid] -= getTimer();
      writeToInfinimem(tid, totalKeysInFile[tid], toMap.size(), toMap);
      infinimem_write_times[tid] += getTimer();
      toMap.clear();
      totalKeysInFile[tid] += nItems[whichMap];
      nItems[whichMap] = 0; 
      return ct;
    }
    InMemoryContainerIterator<KeyType, ValueType> it = toMap.find(begin->first);
    if(it != toMap.end()) {
      combine(it->first, it->second, begin->second);
      ++localCombinedPairs[tid];
      //cTotalKeys[whichMap] += begin->second[0];
    } else {
      toMap.emplace(begin->first, begin->second);
      //cTotalKeys[whichMap] += begin->second[0];
      ++nItems[whichMap];
    }
    ++ct;
    ++begin;
  }
  return ct;
}


//------------------------------------------------- GK
// Write the key and its count to in-memory Buffer
//
  template <typename KeyType, typename ValueType> 
void MapWriter<KeyType, ValueType>::performWrite(const unsigned tid, const unsigned buffer, const KeyType& key, const ValueType& value)
{
  std::vector<ValueType> vals(1, value);
  InMemoryContainerIterator<KeyType, ValueType> it = outBufMap[buffer].find(key); 

  if(it != outBufMap[buffer].end())
  {
    combine(key, it->second, vals);
    #ifdef USE_GOMR
    // I do not need locks here because the threads will not share the same key
    nNbrs.at(key) += 1;
    #endif
    ++localCombinedPairs[tid];
    //cTotalKeys[buffer] += value;
    //			         fprintf(stderr, "\nWord added in map: %s, Value after add: %d, buffer: %llu outBufMap size: %d", out.word().c_str(), value, buffer, outBufMap[buffer].size());
  }
  else {
    outBufMap[buffer].emplace(key, vals); 
    #ifdef USE_GOMR
    nNbrs.at(key) = 1;
    #endif
    //cTotalKeys[buffer] += value;
    nItems[buffer]++;
    //		       fprintf(stderr, "\nWB - word added in map: %s\t buffer: %llu\t outBufMap size: %d", out.word().c_str(), buffer, outBufMap[buffer].size());
  }
}
//-------------------------------------------------  GK
// Convert map to data2 type

template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::writeToInfinimem(const unsigned buffer, const IdType startKey, unsigned noItems, const InMemoryContainer<KeyType, ValueType>& inMemMap) {
  //std::cout << "Inside WriteToInfinimem \n";
  RecordType* records = new RecordType[noItems]; // this can be initialized with noItems
  unsigned ct = 0;

  //		fprintf(stderr, "\nWTI- start -- Buffer: %llu \t startkey: %u\t noItems: %u\t InMemMap size: %d", buffer, startKey, noItems, inMemMap.size());

  for (InMemoryContainerConstIterator<KeyType, ValueType> it = inMemMap.begin(); it != inMemMap.end(); ++it)
  {
    records[ct].set_key(it->first);
#ifdef USE_ONE_PHASE_IO
    assert(it->second.size() == 1);
    records[ct].set_value(it->second[0]);
#else
    for (typename std::vector<ValueType>::const_iterator vit = it->second.begin(); vit != it->second.end(); ++vit)
      records[ct].add_values(*vit);
#endif

    ++ct;
    totalCombined[buffer]++;
  }
  assert(ct == noItems);
  // 2. Write to infinimem partition -- infinimem locks to guarantee correct writes 
  	 //fprintf(stderr, "\nWTI- Before writing -- Buffer: %llu \t startkey: %u\t noItems: %u\n", buffer, startKey, noItems);
  // busy wait
  //infinimem_write_times[buffer] -= getTimer();
  io->file_set_batch(buffer, startKey, noItems, records);
  //infinimem_write_times[buffer] += getTimer();
  
  delete[] records;
}

template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::betterWriteToInfinimem(const unsigned buffer, const IdType startKey, unsigned noItems, InMemoryContainerConstIterator<KeyType, ValueType> begin, InMemoryContainerConstIterator<KeyType, ValueType> end) {
  RecordType* records = new RecordType[noItems]; // this can be initialized with noItems
  unsigned ct = 0;

  for (InMemoryContainerConstIterator<KeyType, ValueType> it = begin; it != end; ++it) {
    records[ct].set_key(it->first);
#ifdef USE_ONE_PHASE_IO
    assert(it->second.size() == 1);
    records[ct].set_value(it->second[0]);
#else
    for (typename std::vector<ValueType>::const_iterator vit = it->second.begin(); vit != it->second.end(); ++vit)
      records[ct].add_values(*vit);
#endif

    ++ct;
    totalCombined[buffer]++;
  }

  assert(ct == noItems);
  
  //infinimem_write_times[buffer] -= getTimer();
  io->file_set_batch(buffer, startKey, noItems, records);
  //infinimem_write_times[buffer] += getTimer();

  delete[] records;
}

//-------------------------------------------------
// Initializes the readNextInBatch to start from for each batch
  template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::readClear(const unsigned tid)
{
  //for (unsigned i = 0; i < nCols; i++){
       readBufMap[tid].clear();
       readNextInBatch[tid].clear();
    fetchBatchIds[tid].clear(); 
    batchesCompleted[tid].clear();
    keysPerBatch[tid].clear();
    readNext[tid] = 0; 
 // }
}

//-------------------------------------------------
// Initializes the readNextInBatch to start from for each batch
  template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::readInit(const unsigned tid)
{
  unsigned j=0;
  for (unsigned long long i = 0; i <= totalKeysInFile[tid]; i+= batchSize)
  {
    //         fprintf(stderr,"\t i= %d", i);
    readNextInBatch[tid].push_back(i); // THE FOLLOWING DESCRIPTION IS WRONG: store the batch numbers 0,1,2,3 ..
    fetchBatchIds[tid].insert(j++); //store the batchIDs of top-k keys emitted
    batchesCompleted[tid].push_back(false); // to check which batch has been read completely
    keysPerBatch[tid].push_back(kBItems); // how many keys are read from each batch
  }
  totalCombined[tid] = 0;
}
//-------------------------------------------------
// Reads *ALL* data from disk into container
// TODO: Should be made size-oblivious ALONG with reading in batches
  template <typename KeyType, typename ValueType>
bool MapWriter<KeyType, ValueType>::read(const unsigned tid, InMemoryContainer<KeyType, ValueType>& readBufMap, std::vector<unsigned>& keysPerBatch, LookUpTable<KeyType>& lookUpTable, std::set<unsigned>& fetchBatchIds, std::vector<unsigned long long>& readNextInBatch, std::vector<bool>& batchesCompleted) {
  infinimem_read_times[tid] -= getTimer();
  RecordType* records = new RecordType[kBItems];
  unsigned batch = 0;
  for(auto it = fetchBatchIds.begin(); it != fetchBatchIds.end(); ++it) {
    batch = *it ;
    unsigned long long batchBoundary = std::min(static_cast<unsigned long long>((batch + 1) * batchSize), static_cast<unsigned long long>(totalKeysInFile[tid]));

    if (readNextInBatch[batch] >= batchBoundary)
    {
      batchesCompleted[batch] = true;
      continue;
    }

    keysPerBatch[batch] = std::min(keysPerBatch[batch], static_cast<unsigned>(batchBoundary - readNextInBatch[batch]));
    
    if (keysPerBatch[batch] > 0 && readNextInBatch[batch] < batchBoundary)
      io->file_get_batch(tid, readNextInBatch[batch], keysPerBatch[batch], records); 

    for (unsigned i = 0; i < keysPerBatch[batch]; i++)
    {
      lookUpTable[records[i].key()].push_back(batch);
      readBufMap[records[i].key()];
#ifdef USE_ONE_PHASE_IO
      readBufMap[records[i].key()].push_back(records[i].value());
#else
      for (unsigned k = 0; k < records[i].values_size(); k++)
        readBufMap[records[i].key()].push_back(records[i].values(k));
#endif
    }

    readNextInBatch[batch] += keysPerBatch[batch];
    keysPerBatch[batch] = 0;

    if (readNextInBatch[batch] >= batchBoundary)
      batchesCompleted[batch] = true;
  }

  fetchBatchIds.clear();
  
  bool ret = false;
  for (unsigned readBatch = 0; readBatch < batchesCompleted.size(); readBatch++)
    if (batchesCompleted[readBatch] == false) {
      ret = true;
      break;
    }

  infinimem_read_times[tid] += getTimer();

  delete[] records;
  return ret;
}

template <typename KeyType, typename ValueType>
bool MapWriter<KeyType, ValueType>::read(const unsigned tid) {
  return read(tid, readBufMap[tid], keysPerBatch[tid], lookUpTable[tid], fetchBatchIds[tid], readNextInBatch[tid], batchesCompleted[tid]);
}

template <typename KeyType, typename ValueType>
InMemoryReductionState<KeyType, ValueType> MapWriter<KeyType, ValueType>::initiateInMemoryReduce(unsigned tid) {
  InMemoryReductionState<KeyType, ValueType> state(nRows); 
  for(unsigned i=0; i<nRows; ++i) {
    state.begins[i] = outBufMap[tid + nCols * i].begin();
    state.ends[i] = outBufMap[tid + nCols * i].end();
  }

  return state;
}

template <typename KeyType, typename ValueType>
bool MapWriter<KeyType, ValueType>::getNextMinKey(InMemoryReductionState<KeyType, ValueType>* state, InMemoryContainer<KeyType, ValueType>* record) {
  std::vector<unsigned> minIds;
  KeyType minKey;
  bool found = false;
  
  for(unsigned i=0; i<nRows; ++i) {
    if(state->begins[i] == state->ends[i])
      continue;

    if(!found) {
      minKey = state->begins[i]->first;
      minIds.push_back(i);
      found = true;
    } else {
      if(state->begins[i]->first < minKey) {
        minKey = state->begins[i]->first;
        minIds.clear();
        minIds.push_back(i);
      } else if(state->begins[i]->first == minKey) {
        minIds.push_back(i);
      }
    }
  }

  if(!found)
    return false;

  std::vector<ValueType>& values = (*record)[minKey];
  for(typename std::vector<unsigned>::iterator it = minIds.begin(); it != minIds.end(); ++it) {
    for(typename std::vector<ValueType>::const_iterator vit = state->begins[*it]->second.begin(); vit != state->begins[*it]->second.end(); ++vit) 
      values.push_back(*vit);

    ++state->begins[*it];
  }

  return true;
}

template <typename KeyType, typename ValueType>
InMemoryContainer<KeyType, ValueType>& MapWriter<KeyType, ValueType>::cRead(const unsigned tid) {

  //fprintf(stderr, "\nDoRefine: Calling Read\n");
    bool execLoop;
       //Erase previously added data as I am returning later without erasing
    if(!getWrittenToDisk()){
    // for in-memory reads
	 don = true;
         return readBufMap[tid];
    }
   else{
    if(getWrittenToDisk() && readBufMap[tid].size() > 0)
  	readBufMap[tid].erase(readBufMap[tid].begin(), readBufMap[tid].end());

    execLoop = cDiskRead(tid);
    }

 //   fprintf(stderr, "\nExecloop is %d, TID %d, ContainerSize: %d ", execLoop, tid, readBufMap[tid].size());
  // fprintf(stderr,"\nCREAD totalCuts: %d\n", totalCuts);
    if(execLoop == false) {
	 don = true;
         return readBufMap[tid];
         //readAfterReduce(tid, readBufMap[tid]);
       //  if(tid == 0) countTotalPECut(tid);
       //  break;
    }

   // Read the kitems from infinimem and compute the edgecuts for that part
        don = false;
         return readBufMap[tid];
}
//--------------------------------------------------
template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::cWrite(const unsigned tid) {
     cWrite(tid, readBufMap[tid].size(), readBufMap[tid].end());
}

// for calling through applications
//--------------------------------------------------
template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::diskWriteContainer(const unsigned tid, const IdType startKey, unsigned noItems, InMemoryContainerConstIterator<KeyType, ValueType> begin, InMemoryContainerConstIterator<KeyType, ValueType> end) {
  unsigned buffer = tid % nCols;
// fprintf(stderr, "\nThread %d cWrite to partition %d noItems: %d \n", tid, buffer, noItems); 
    
  infinimem_cwrite_times[tid] -= getTimer();
    pthread_mutex_lock(&locks[buffer]);
  cWriteToInfinimem(buffer, startKey, noItems, begin, end);
    pthread_mutex_unlock(&locks[buffer]);
    infinimem_cwrite_times[tid] += getTimer();
  //  fprintf(stderr,"\n*********TID: %d Total Combined : %d \n\n", tid, totalCombined[buffer]); 
}
  
     //cWrite(tid, totalCombined[tid], readBufMap[tid].size(), readBufMap[tid].end());
//}
//--------------------------------------------------
template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::cWrite(const unsigned tid, unsigned noItems, InMemoryContainerConstIterator<KeyType, ValueType> end) {
  
  unsigned buffer = tid % nCols;
// fprintf(stderr, "\nThread %d cWrite to partition %d noItems: %d \n", tid, buffer, noItems); 
    
  infinimem_cwrite_times[tid] -= getTimer();
    pthread_mutex_lock(&locks[buffer]);
  cWriteToInfinimem(buffer, totalCombined[tid], noItems, readBufMap[tid].begin(), end);
    pthread_mutex_unlock(&locks[buffer]);
    infinimem_cwrite_times[tid] += getTimer();
  //  fprintf(stderr,"\n*********TID: %d Total Combined : %d \n\n", tid, totalCombined[buffer]); 
}
  
//--------------------------------------------------
template <typename KeyType, typename ValueType>
void MapWriter<KeyType, ValueType>::cWriteToInfinimem(const unsigned buffer, const IdType startKey, unsigned noItems, InMemoryContainerConstIterator<KeyType, ValueType> begin, InMemoryContainerConstIterator<KeyType, ValueType> end) {
  RecordType* records = new RecordType[noItems];
  unsigned ct = 0;

  for (InMemoryContainerConstIterator<KeyType, ValueType> it = begin; it != end; ++it) {
     records[ct].set_key(it->first);
//      fprintf(stderr,"\n BWTI- TID: %d, startKey: %d, Key: %d\t, Values: ", buffer, startKey, it->first); 

    for (std::vector<unsigned>::const_iterator vit = it->second.begin(); vit != it->second.end(); ++vit){
      records[ct].add_values(*vit);
    //  fprintf(stderr,"%d\t", *vit); 
      }
      ++ct;
      totalCombined[buffer]++;
  }

  assert(ct == noItems);
  cio->file_set_batch(buffer, startKey, noItems, records);
 delete[] records;
}

//========================= 
template <typename KeyType, typename ValueType>
bool MapWriter<KeyType, ValueType>::cDiskRead(const unsigned tid) {

  infinimem_cread_times[tid] -= getTimer();
  unsigned partition = tid;
    unsigned partbound = min(totalCombined[tid]-readNext[tid], kBItems);
  RecordType* parts = new RecordType[partbound];
 //fprintf(stderr,"\nREFINE tid: %d, totalCombined: %d, readNext[tid]: %d, keys to read: %d \n", tid, totalCombined[tid], readNext[tid], partbound);
 //for(unsigned ckey = readNextInBatch[partition]; ckey < totalCombined[partition]; ckey += batchSize){
    if (partbound > 0 && readNext[tid] < totalCombined[tid])
      cio->file_get_batch(tid, readNext[tid], partbound, parts);

  //  fprintf(stderr,"\nREFINE- tid: %d  BATCHSIZE: %d, Map size: %d, readNext: %d \n", tid, batchSize, refineMap[tid].size(), readNext[tid]);
    for (unsigned i = 0; i < partbound; i++) {
      readBufMap[tid][parts[i].key()];
//      fprintf(stderr,"\nREFINE- TID: %d, Key: %d\t Values: ", tid, parts[i].rank()); 

      for (unsigned k = 0; k < parts[i].values_size(); k++){
        readBufMap[tid][parts[i].key()].push_back(parts[i].values(k));
   //        fprintf(stderr,"\nREFINE - key: %d value: %d\n", parts[i].key(), parts[i].values_size());
     }
   }

    //fprintf(stderr,"\nREFINE - tid: %d, RefineMap size: %d", tid, refineMap[tid].size());
    readNext[tid] += partbound;
    //fprintf(stderr,"\nREFINE update-  tid: %d Total Combined: %d, readNext: %d \n", tid, totalCombined[tid], readNext[tid]);

  bool ret = false;
    if (readNext[tid] < totalCombined[tid]){
        ret = true;
  //      fprintf(stderr, "\nREFINE - still reading - tid: %d, readNext: %d, totalCombined: %d, ret: %d \n", tid, readNext[tid], totalCombined[tid], ret);
    }

  infinimem_cread_times[tid] += getTimer();
  delete[] parts;
  return ret;
}
//========================= 
