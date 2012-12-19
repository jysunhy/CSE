#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <queue>
using namespace std;

class lock_server_cache {
 private:
  int nacquire;
  mutex_pool mpool;
  pthread_mutex_t *mtx;
  map<lock_protocol::lockid_t,queue<string> > list;
  //map<lock_protocol::lockid_t,string> retrying;
  void print_queue(lock_protocol::lockid_t lid) {
  //  pthread_mutex_lock(mtx);
    cout<<"queue size :"<<list[lid].size()<<endl;
    queue<string> tmp(list[lid]);
    while(!tmp.empty()) {
      cout<<tmp.front()<<" ";
      tmp.pop();
    }
    cout<<endl;
  //  pthread_mutex_unlock(mtx);
  }
  //map<lock_protocol::lockid_t,string> revoking;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
