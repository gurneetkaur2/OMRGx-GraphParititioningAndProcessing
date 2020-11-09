#ifndef _FILE_IO_H__
#define _FILE_IO_H__

#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <assert.h>
#include <typeinfo> //for typeid()
#include "util.h"
#include <unistd.h>
#include <iostream>

#include <vector>

// Enable Largefile support (equivalent to O_LARGEFILE flag of open())
#define _FILE_OFFSET_BITS 64

// Logically split each per-thread data file into 
// __**__non-contiguous__**__ PARALLEL_ACCESS chunks
// only for ****READS****
#ifdef GRAPHCHI
#define PARALLEL_ACCESS 1
#else
#define PARALLEL_ACCESS 64
#endif
#define FILEIO_MAX_KEY 256

typedef int fileIoReturn_t;
typedef fileIoReturn_t fileIoReturn;

enum fileIoReturntypes
{
  FILEIO_ERROR = -1, FILEIO_SUCCESS = 0, FILEIO_NOTSTORED = 1, FILEIO_DATA_EXISTS = 2
};

//----------------------------------------------------------
template<typename T>
class FileIO
{
  private:
    std::string tmpFileLocation;
    IdType elementsPerFile;

  public:
    std::vector<unsigned> individual_read_counts;
    std::vector<unsigned> batch_read_counts;
    std::vector<unsigned> individual_write_counts;
    std::vector<unsigned> batch_write_counts;
    std::vector<double> individual_read_times;
    std::vector<double> batch_read_times;
    std::vector<double> individual_write_times;
    std::vector<double> batch_write_times;

    unsigned nthreads;

    //++++++++++++++++++++
#if defined(USE_METIS) || defined(GRAPHCHI)
    const std::vector<IdType>& bounds;
    FileIO(const char *filePath, unsigned threadCount, IdType ePerFile, const std::vector<IdType>& boundaries=NULL) : bounds(boundaries)
    {
#else
    FileIO(const char *filePath, unsigned threadCount, IdType ePerFile)
    {
#endif
      nthreads = threadCount;
      tmpFileLocation = filePath;
      elementsPerFile = ePerFile;

      for (unsigned i = 0; i < nthreads; ++i)
      {
        individual_read_counts.push_back(0);
        batch_read_counts.push_back(0);
        individual_write_counts.push_back(0);
        batch_write_counts.push_back(0);
        individual_read_times.push_back(0.0);
        batch_read_times.push_back(0.0);
        individual_write_times.push_back(0.0);
        batch_write_times.push_back(0.0);
      }
    }

    //++++++++++++++++++++
    virtual ~FileIO()
    {
      unsigned iwc = 0, irc = 0, bwc = 0, brc = 0;
      double irt = 0.0, iwt = 0.0, bwt = 0.0, brt = 0.0;
      for (unsigned i = 0; i < nthreads; i++)
      {
        fprintf(stderr, "%s: IWriteCount[%d]: %u, \tIReadCount[%d]: %u, BWriteCount[%d]: %u, \tBReadCount[%d]: %u\n",
            typeid(*this).name(), i, individual_write_counts[i], i, individual_read_counts[i], i, batch_write_counts[i],
            i, batch_read_counts[i]);
        fprintf(stderr,
            "%s: IWriteTimes[%d]: %.3lf, IReadTimes[%d]: %.3lf, BWriteTimes[%d]: %.3lf, BReadTimes[%d]: %.3lf\n",
            typeid(*this).name(), i, individual_write_times[i], i, individual_read_times[i], i, batch_write_times[i], i,
            batch_read_times[i]);
        iwc += individual_write_counts[i];
        bwc += batch_write_counts[i];
        iwt += individual_write_times[i];
        bwt += batch_write_times[i];
        irc += individual_read_counts[i];
        brc += batch_read_counts[i];
        irt += individual_read_times[i];
        brt += batch_read_times[i];
      }

      fprintf(stderr,
          "%s: Total IReads: %d, Total BReads: %d, Total I-read times: %.3lf, Average time/I-read: %.3lf, Total B-read times: %.3lf, Average time/B-read: %.3lf\n",
          typeid(*this).name(), irc, brc, irt, irt / irc, brt, brt / brc);
      fprintf(stderr,
          "%s: Total IWrites: %d, Total BWrites: %d, Total I-write times: %.3lf, Average time/I-write: %.3lf, Total B-write times: %.3lf, Average time/B-write: %.3lf\n",
          typeid(*this).name(), iwc, bwc, iwt, iwt / iwc, bwt, bwt / bwc);
    }
    ;

    //++++++++++++++++++++
    inline unsigned tid(IdType id) const
    {
#if defined(USE_METIS) || defined(GRAPHCHI)
      if(0 <= id && id < bounds[0]) return 0;
      for(size_t i=1; i<bounds.size(); i++)
      if(bounds[i-1] <= id && id < bounds[i]) return i;
      //return bounds.size()-1;
      assert(false);
#else
      //efprintf(stderr, "Key: %llu, Threads: %u, TID: %llu, EPF: %llu\n", id, nthreads, id/elementsPerFile, elementsPerFile);
      //assert((id/elementsPerFile) < nthreads);
      return id / elementsPerFile;
#endif
    }

    //++++++++++++++++++++
    inline void clearStats()
    {
      for (unsigned i = 0; i < nthreads; ++i)
      {
        individual_read_counts[i] = 0;
        individual_write_counts[i] = 0;
        individual_read_times[i] = 0.0;
        individual_write_times[i] = 0.0;

        batch_read_counts[i] = 0;
        batch_write_counts[i] = 0;
        batch_read_times[i] = 0.0;
        batch_write_times[i] = 0.0;
      }
    }

    inline int hash(IdType id) const
    {
      return id % PARALLEL_ACCESS;
    }
    inline const char *tmpFilePath() const
    {
      return tmpFileLocation.c_str();
    }

    virtual void init() = 0;
    virtual void shutdown() = 0;
    virtual void fileIoSetup() = 0;
    virtual void fileIoDestroy() = 0;

    virtual fileIoReturn_t file_set(const unsigned& tid, IdType key, const T& value) = 0;
    virtual fileIoReturn_t file_get(const unsigned& tid, IdType key, T& value) = 0;
    virtual fileIoReturn_t file_delete(const unsigned& tid, IdType key) = 0;
    virtual fileIoReturn_t file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems,
        T* arrayOfItems) = 0;
    virtual fileIoReturn_t file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems,
        T* arrayOfItems) = 0;
};

//----------------------------------------------------------
template<typename T>
class OnePhaseFileIO: public FileIO<T>
{
  public:
#if defined(USE_METIS) || defined(GRAPHCHI)
    OnePhaseFileIO(const char *loch, unsigned threadCount, IdType ePerFile, const std::vector<IdType>& boundaries = NULL);
#else
    OnePhaseFileIO(const char *loch, unsigned threadCount, IdType ePerFile);
#endif
    ~OnePhaseFileIO();

    void init();
    void shutdown();
    void fileIoSetup();
    void fileIoDestroy();

    fileIoReturn_t file_set(const unsigned& tid, IdType key, const T& value);
    fileIoReturn_t file_get(const unsigned& tid, IdType key, T& value);
    fileIoReturn_t file_delete(const unsigned& tid, IdType key);
    fileIoReturn_t file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems);
    fileIoReturn_t file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems);

  private:
    // dataFileMutex is locked for appends to the data file
    pthread_mutex_t **dataFileMutex; // [compthreads][PARALLEL_ACCESS]
    int *data_iofs;
    char *null_string;
};

//----------------------------------------------------------
template<typename T>
class TwoPhaseFileIO: public FileIO<T>
{
  public:
#if defined(USE_METIS) || defined(GRAPHCHI)
    TwoPhaseFileIO(const char *loch, unsigned threadCount, IdType ePerFile, const std::vector<IdType>& boundaries=NULL);
#else
    TwoPhaseFileIO(const char *loch, unsigned threadCount, IdType ePerFile);
#endif
    ~TwoPhaseFileIO();

    void init();
    void shutdown();
    void fileIoSetup();
    void fileIoDestroy();

    fileIoReturn_t file_set(const unsigned& tid, IdType key, const T& value);
    fileIoReturn_t file_get(const unsigned& tid, IdType key, T& value);
    fileIoReturn_t file_delete(const unsigned& tid, IdType key);
    fileIoReturn_t file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems);
    fileIoReturn_t file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems);

  private:
    int **data_ifs;
    int **meta_iofs;
    int *data_ofs;
    char *null_string;

    pthread_mutex_t *dataFileMutex; // [compthreads]
    pthread_mutex_t **metaFileMutex; // [compthreads][PARALLEL_ACCESS]
};

#endif // _FILE_IO_H__

