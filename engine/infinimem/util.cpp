#include "util.h"

#include <iostream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <cassert>

#include <execinfo.h> // for backtrace
#include <stdlib.h>

std::tr1::mt19937 eng;

//------------------------------------------------------------------
char *itoa(int i, char *tmp) {
  sprintf(tmp, "%d", i);
  return tmp;
}

//------------------------------------------------------------------
// Implicitly assumes unsigned numbers; does not count any leading sign
unsigned numDigits(std::streamoff num) {
  int ndigits = 0;
  while (num) {
    num /= 10;
    ndigits++;
  }
  return ndigits;
}

//------------------------------------------------------------------
std::string zeroPadded(std::streamoff num) {
  std::ostringstream ss;
  ss << std::setw( MAX_CHARS_STREAMOFF ) << std::setfill( '0' ) << num;
  return ss.str();
}

//------------------------------------------------------------------
double tmDiff(timeval &s, timeval&e) {
  return timevalToDouble(e) - timevalToDouble(s);
}

//------------------------------------------------------------------
double timevalToDouble(timeval &t) {
  return t.tv_sec*1000.0 + t.tv_usec/1000.0;
}

//------------------------------------------------------------------
unsigned randomNumber(int floor, int ceiling) {
  std::tr1::uniform_int<int> unif(floor, ceiling-1);
  return unif(eng);
}

//------------------------------------------------------------------
IdType min(IdType a, IdType b) {
  return a<b ? a : b;
}

//------------------------------------------------------------------
// http://www.gnu.org/software/libc/manual/html_node/Backtraces.html
// To see function names, on command line pass OPTS+=-rdynamic
void printCallStack() {
  const unsigned CALL_STACK_SIZE = 50;
  void *array[CALL_STACK_SIZE];
  char **strings;

  size_t size = backtrace(array, CALL_STACK_SIZE);
  strings = backtrace_symbols(array, size);

  for(size_t i = 0; i < size; i++) {
    printf("%s\n", strings[i]);

    // Uncomment these and replace sssp with the name of this binary
    // char syscom[256];
    // sprintf(syscom,"addr2line %p -e sssp", array[i]); //last parameter is the name of this app
    // system(syscom);
  }
  printf("-----------------------\n");
  free(strings);
}

