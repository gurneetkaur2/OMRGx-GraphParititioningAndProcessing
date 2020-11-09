#ifndef __HM_UTIL_H_
#define __HM_UTIL_H_

#include <limits>
#include <iomanip>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <tr1/random>

#include <google/protobuf/stubs/common.h>
typedef ::google::protobuf::uint64 IdType;

#define MAX_ID_VALUE (ULONG_MAX)

extern std::tr1::mt19937 eng;

// enhanced fprintf using a variadic macro
#ifdef DEBUG
#define efprintf(ofile, fmt, ...) fprintf(stderr, "%s(): %d | "fmt, __func__, __LINE__, __VA_ARGS__);
//#define efprintf(ofile, fmt, ...) fprintf(stderr, "%s::%s(): %d | "fmt, __FILE__, __func__, __LINE__, __VA_ARGS__);
#else
#define efprintf(...) ;
#endif

char *itoa(int i, char *tmp);
unsigned numDigits(std::streamoff);
std::string zeroPadded(std::streamoff);
double timevalToDouble(timeval &t); 
double tmDiff(timeval &s, timeval &e);
unsigned randomNumber(int floor, int ceiling);
IdType min(IdType, IdType);

void printCallStack();

// Computing the digits using this method with ::max() gives 20 characters
// Fetching the digits using ::digits gives 18. To be safe, sticking with ::max() method
#define MAX_CHARS_STREAMOFF ( numDigits(std::numeric_limits<std::streamoff>::max()) )

#endif //__HM_UTIL_H_

