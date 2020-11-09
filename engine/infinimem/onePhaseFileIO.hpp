#include "fileIO.h"
#include "util.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_DATA_LENGTH (sizeof(T))

#if defined(USE_METIS) || defined(GRAPHCHI)
#define OFFSET_ADJUST(tid) ( ((tid == 0) ? 0 : this->bounds[tid-1]) )
#else
#define OFFSET_ADJUST(tid) (0)
#endif 

//----------------------------------------------------------
template<typename T>
#if defined(USE_METIS) || defined(GRAPHCHI)
OnePhaseFileIO<T>::OnePhaseFileIO(const char *loc, unsigned threadCount, IdType ePerFile, const std::vector<IdType>& boundaries)
  : FileIO<T>(loc, threadCount, ePerFile, boundaries) {
#else
OnePhaseFileIO<T>::OnePhaseFileIO(const char *loc, unsigned threadCount, IdType ePerFile)
  : FileIO<T>(loc, threadCount, ePerFile) {
#endif
  dataFileMutex = NULL; // [compthreads][PARALLEL_ACCESS]
  data_iofs = NULL;

  fprintf(stderr, "Initializing OnePhaseFileIO\n");
  fprintf(stderr, "MAX_DATA_LENGTH = %d\n", MAX_DATA_LENGTH);
  std::cout << "Number of MRFiles: " << this->nthreads << std::endl;

  init();
}

//----------------------------------------------------------
template<typename T>
OnePhaseFileIO<T>::~OnePhaseFileIO() {
  shutdown();
}

//----------------------------------------------------------
template<typename T>
void OnePhaseFileIO<T>::init() {
  /*
  char path[256];
  strcat(strcpy(path, "rm -rf "), this->tmpFilePath()); // Don't assume clean shutdown.
  int t = system(path); assert( t == 0 );

  strcat(strcpy(path, "mkdir -p "), this->tmpFilePath());
  t = system(path);
  */

  fileIoSetup();
}

//----------------------------------------------------------
template<typename T>
void OnePhaseFileIO<T>::shutdown() {
  char path[256];
  strcat(strcpy(path, "rm -rf "), this->tmpFilePath());
  //system(path); // Ignoring return; okay in this case
}

//----------------------------------------------------------
template<typename T>
void OnePhaseFileIO<T>::fileIoSetup() {
  char tpath[256];
  strcat(strcpy(tpath, "rm -rf "), this->tmpFilePath()); // Don't assume clean exit.
  int t = system(tpath); assert( t == 0 );

  // This assumption is key to the implementation:
  // When reading the file pointers from meta data file, we need to be able to convert 
  // it to a known numeric type. For instace, atoi or atol or just read in (see _get below)
  assert(std::numeric_limits<long>::max() == std::numeric_limits<std::streamoff>::max());

  strcat(strcpy(tpath, "mkdir -p "), this->tmpFilePath());
  strcat(tpath, "/data");
  t = system(tpath);

  null_string = (char *) calloc(MAX_DATA_LENGTH, sizeof(char)); // Also forces contents to be zero

  data_iofs = (int *)calloc(this->nthreads, sizeof(int));
  dataFileMutex = (pthread_mutex_t **)calloc(this->nthreads, sizeof(pthread_mutex_t *));
  assert((dataFileMutex != NULL) && (data_iofs != NULL));

  for(unsigned i=0; i<this->nthreads; i++) {
    dataFileMutex[i] = (pthread_mutex_t *)calloc(PARALLEL_ACCESS, sizeof(pthread_mutex_t));
    assert((dataFileMutex[i] != NULL));

    char path[256]; char suffix[256];
    sprintf(suffix, "/data/data_%d", i); // Note the i: we open PARALLEL_ACCESS number of FPs to the i-th data files
    data_iofs[i] = open(strcat(strcpy(path, this->tmpFilePath()), suffix), O_CREAT | O_LARGEFILE | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    assert(data_iofs[i] > 0);
    //fcntl(data_iofs[i], F_NOCACHE, 1);

    for(unsigned j=0; j<PARALLEL_ACCESS; j++) {
      pthread_mutex_init(&dataFileMutex[i][j], NULL);
    }
  }
}

//----------------------------------------------------------
template<typename T>
void OnePhaseFileIO<T>::fileIoDestroy() {
  for(unsigned i=0; i<this->nthreads; i++) {
    close(data_iofs[i]);
    for(unsigned j=0; j<PARALLEL_ACCESS; j++) {
      pthread_mutex_destroy(&dataFileMutex[i][j]);
    }
    free(dataFileMutex[i]);
  }
  free(dataFileMutex);
  free(data_iofs);
  free(null_string);
}

//----------------------------------------------------------
// Overwrite data in data_ file
template<typename T>
fileIoReturn_t OnePhaseFileIO<T>::file_set(const unsigned& tid, IdType key, const T& value) {
  //unsigned tid = this->tid(key);
  unsigned hid = this->hash(key);
  timeval s, e;
  gettimeofday(&s, NULL);
  this->individual_write_counts[tid]++;

  //efprintf(stderr, "TidL %u, Key: %llu, distance: %llu\n", tid, key, value.distance);
  pthread_mutex_lock( &dataFileMutex[tid][hid] );
  int err = pwrite(data_iofs[tid], &value, MAX_DATA_LENGTH, (key-OFFSET_ADJUST(tid))*MAX_DATA_LENGTH); assert(err == MAX_DATA_LENGTH);
  pthread_mutex_unlock( &dataFileMutex[tid][hid] );

  gettimeofday(&e, NULL);
  this->individual_write_times[tid] += timevalToDouble(e) - timevalToDouble(s);
  return FILEIO_SUCCESS;
}

//----------------------------------------------------------
// 0. Obtain a lock on appropriate mutex
// 1. seekg() to the right place in the data file & read
template<typename T>
fileIoReturn_t OnePhaseFileIO<T>::file_get(const unsigned& tid, IdType key, T& value) {
  timeval s, e;
  gettimeofday(&s, NULL);
  //unsigned tid = this->tid(key);
  unsigned hid = this->hash(key);
  this->individual_read_counts[tid]++;

  pthread_mutex_lock( &dataFileMutex[tid][hid] );
  int err = pread(data_iofs[tid], &value, MAX_DATA_LENGTH, (key-OFFSET_ADJUST(tid))*MAX_DATA_LENGTH); assert(err == MAX_DATA_LENGTH);
  pthread_mutex_unlock( &dataFileMutex[tid][hid] );
  //efprintf(stderr, "Tid: %u, Key: %llu, distance: %llu\n", tid, key, value.distance);

  gettimeofday(&e, NULL);
  this->individual_read_times[tid] += timevalToDouble(e) - timevalToDouble(s);
  return FILEIO_SUCCESS;
}

//----------------------------------------------------------
// Simply remove the entry from meta data file
template<typename T>
fileIoReturn_t OnePhaseFileIO<T>::file_delete(const unsigned& tid, IdType key) {
  //unsigned tid = this->tid(key);
  unsigned hid = this->hash(key);

  pthread_mutex_lock( &dataFileMutex[tid][hid] );
  char empty[MAX_DATA_LENGTH] = {0x0};
  int err = pwrite(data_iofs[tid], empty, MAX_DATA_LENGTH, (key-OFFSET_ADJUST(tid))*MAX_DATA_LENGTH); assert(err == 1);
  pthread_mutex_unlock( &dataFileMutex[tid][hid] );
  return FILEIO_SUCCESS;
}

//----------------------------------------------------------
template<typename T>
fileIoReturn_t OnePhaseFileIO<T>::file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems) {
  timeval s, e;
  gettimeofday(&s, NULL);
  //unsigned tid = this->tid(startKey);
  unsigned hid = this->hash(startKey);
  this->batch_write_counts[tid]++;

  // for(IdType i=startKey; i<startKey+nItems; i++)
  //   efprintf(stderr, "Tid: %u, Key: %llu, distance: %llu\n", tid, i, arrayOfItems[i].distance);
  pthread_mutex_lock( &dataFileMutex[tid][hid] ); //this could be funny business :/
  int err = pwrite(data_iofs[tid], arrayOfItems, MAX_DATA_LENGTH*nItems, (startKey-OFFSET_ADJUST(tid))*MAX_DATA_LENGTH); assert(err == (int)(nItems*MAX_DATA_LENGTH)); //problematic cast?
  pthread_mutex_unlock( &dataFileMutex[tid][hid] );

  gettimeofday(&e, NULL);
  this->batch_write_times[tid] += timevalToDouble(e) - timevalToDouble(s);
  return FILEIO_SUCCESS;
}

//----------------------------------------------------------
template<typename T>
fileIoReturn_t OnePhaseFileIO<T>::file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems) {
  timeval s, e;
  gettimeofday(&s, NULL);
  //unsigned tid = this->tid(startKey);
  unsigned hid = this->hash(startKey);
  this->batch_read_counts[tid]++;

  pthread_mutex_lock( &dataFileMutex[tid][hid] ); //this could be funny business :/
  int err = pread(data_iofs[tid], arrayOfItems, nItems*MAX_DATA_LENGTH, (startKey-OFFSET_ADJUST(tid))*MAX_DATA_LENGTH); assert(err == (int)(nItems*MAX_DATA_LENGTH));
  // for(IdType i=startKey; i<startKey+nItems; i++)
  //   efprintf(stderr, "Tid: %u, Key: %llu, distance: %llu\n", tid, i, arrayOfItems[i-startKey].distance);
  pthread_mutex_unlock( &dataFileMutex[tid][hid] );

  gettimeofday(&e, NULL);
  this->batch_read_times[tid] += timevalToDouble(e) - timevalToDouble(s);
  return FILEIO_SUCCESS;
}

