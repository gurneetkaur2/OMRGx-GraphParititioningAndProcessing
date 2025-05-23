#ifndef __GRAPH_H_
#define __GRAPH_H_

#include <climits>

#define MAX_VERTEX_VALUE (ULLONG_MAX)

//------------------------------------------------------------------
// Warning: for OnePhaseFileIO, do not use STL structures with variable sizes
typedef struct _edgeType {
  IdType src;
  IdType dst;

  double vRank;
  double rank; // of src
  IdType numNeighbors; // of src
} Edge;

struct edgeCompare {
  bool operator() (const Edge& a, const Edge& b) {
    if (a.src == b.src) return (a.dst <= b.dst);
    return (a.src <= b.src);
  }
} EdgeCompare;

struct vertexCompare {
  bool operator() (const Edge* a, const Edge* b) {
    if (a->dst == b->dst) return (a->src <= b->src);
    return (a->dst <= b->dst);
  }
} VertexCompare;

//------------------------------------------------------------------

#endif // __GRAPH_H_


