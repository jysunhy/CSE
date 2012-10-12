// this is the lock server
// the lock client has a similar interface
// author: sun haiyang, 1120379021
#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <map>
#include <vector>
using namespace std;


class mutex_pool {
public:
  mutex_pool() {
    
  }
  ~mutex_pool() {
    for(unsigned int i=0; i<buf.size();i++) {
      pthread_mutex_destroy(buf[i]);
      delete buf[i];
    }
  }
  pthread_mutex_t* get() {
    pthread_mutex_t* result=NULL;
    if(buf.size()>0) {
      result=buf.back();
      buf.pop_back();
    }
    if(!result) {
      result=new pthread_mutex_t;
      pthread_mutex_init(result,NULL);
    }
    return result;
  }
  void release(pthread_mutex_t* mutex) {
    buf.push_back(mutex);
  }
private:
  vector<pthread_mutex_t*>buf;
  int size;
};


class lock_server {
 protected:
  pthread_mutex_t *mtx;
  int nacquire;
  mutex_pool mpool;
  map<lock_protocol::lockid_t, int> stattable;
  map<lock_protocol::lockid_t, pthread_mutex_t*> mutextable;
 public:
  lock_server();
  ~lock_server();
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
  void print_table() {
    for(map<lock_protocol::lockid_t, int>::iterator iter = stattable.begin(); iter != stattable.end(); iter++) {
      printf("%llu:%d;",iter->first,iter->second);
    }
    printf("\n");
  }
};

#endif 
