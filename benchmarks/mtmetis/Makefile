TARGET      := mtmetis.bin
#TARGET_ONE  := wordcount-one.bin
CPP         := g++
CC          := gcc
CFLAGS      := -w -MMD -std=c++11 
#CFLAGS      := -Wall -MMD
LIBS        := -lm -lpthread -ltcmalloc -lstdc++ #-Wl,-rpath=/home/gkaur007/opt
LIBPATH     := ../../engine
LDLIBS      := #-L/home/gkaur007/opt/ -L/home/gkaur007/opt/lib/ -Wl,-R/home/gkaur007/opt/lib/

PROTOC      := protoc
PROTO_FLAGS := #-L /home/gkaur007/opt/lib
PROTO_LIBS  := -lprotobuf 

RM          := rm -rf
OBJS        := data.pb.o $(LIBPATH)/util.o main.o
#OBJS_ONE    := $(LIBPATH)/util.o main-one.o

OPTS        := -DUSE_GOMR #from command line (eg: OPTS+=...)

-include    $(OBJS:%.o=%.d)

all: CFLAGS += -O3
#all: $(TARGET) $(TARGET_ONE)
all: $(TARGET) 

debug: CFLAGS += -DDEBUG -g3
#debug: $(TARGET) $(TARGET)
debug: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $(OPTS) $^ $(LDLIBS) $(LIBS) $(PROTO_LIBS) $(INC) -o $@

#$(TARGET_ONE): $(OBJS_ONE)
#	$(CC) $(CFLAGS) $(OPTS) $^ $(LDLIBS) $(LIBS) $(PROTO_LIBS) $(INC) -DUSE_ONE_PHASE_IO -o $@

%.pb.cc: %.proto
	$(PROTOC) -I=. --cpp_out=. $<

%.pb.o: %.pb.cc %.pb.h
	$(CC) $(CFLAGS) $(OPTS) -c $< -o $@

%.o: %.cpp %.h
	$(CC) $(CFLAGS) $(OPTS) -c $< -o $@

%.o: %.cpp
	$(CC) $(CFLAGS) $(OPTS) -c $< -o $@

#main-one.o: main.cpp
#	$(CC) $(CFLAGS) $(OPTS) -c $< -DUSE_ONE_PHASE_IO -o $@

clean:
	$(RM) $(LIBPATH)/*.o *.o $(LIBPATH)/*.d *.d $(TARGET) $(TARGET_ONE) *.pb.*

# Dependencies:
# 1. Protocol buffers v2.5.0
# 2. Google's tcmalloc v2.1 --> lubunwind 0.99-beta

