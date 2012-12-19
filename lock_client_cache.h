// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"
#include <vector>
#include <map>
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
    cout<<"release lock\n";
    buf.push_back(mutex);
  }
private:
  vector<pthread_mutex_t*>buf;
  int size;
};


// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  enum lcstatus {NONE, FREE, LOCKED, ACQUIRING, RELEASING};
  pthread_mutex_t *mtx;
  mutex_pool mpool;
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  map<lock_protocol::lockid_t,lcstatus> _stats_map;
  //map<lock_protocol::lockid_t,int> _gen_map;
  map<lock_protocol::lockid_t,int> _cnt_map;
  map<lock_protocol::lockid_t,pthread_mutex_t*> _lock_map;
  map<lock_protocol::lockid_t,int> _acq_map;
  //map<lock_protocol::lockid_t,int> _rel_map;
  //map<lock_protocol::lockid_t,pthread_mutex_t*> _none_lock_map;
  //map<lock_protocol::lockid_t,pthread_mutex_t*> _call_lock_map;
  //int incre_gen(lock_protocol::lockid_t lid){
  //  _gen_map[lid]++;
  //  return _gen_map[lid];
  //}
 public:
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache();
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
};


#endif
