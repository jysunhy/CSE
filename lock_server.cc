// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  mtx=mpool.get();
}

lock_server::~lock_server()
{
  mpool.release(mtx);
  for(map<lock_protocol::lockid_t,pthread_mutex_t*>::iterator iter=mutextable.begin();iter!=mutextable.end();iter++) {
    if((*iter).second)
      mpool.release((*iter).second);
  }
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(mtx);
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  if(!stattable.count(lid)) {
    stattable[lid]=0;
    mutextable[lid]=NULL;
  }
  r=stattable[lid];
  pthread_mutex_unlock(mtx);
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(mtx);
  lock_protocol::status ret = lock_protocol::OK;
  printf("acquire request from clt %d\n", clt);
  //printf("acquire request from clt %llu:%d\n", lid,clt);
  if(!stattable.count(lid)) {
    stattable[lid]=0;
    mutextable[lid]=NULL;
  }
  if(!mutextable[lid])
    mutextable[lid]=mpool.get();
  //printf("haiyang stat %llu:%d before acquire\n", lid, stattable[lid]);
  //print_table();
  stattable[lid]++;
  pthread_mutex_unlock(mtx);
  pthread_mutex_lock(mutextable[lid]);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(mtx);
  lock_protocol::status ret = lock_protocol::OK;
  //printf("release request from clt %llu:%d\n", lid,clt);
  printf("release request from clt %d\n", clt);
  if(!stattable.count(lid))
    ret=lock_protocol::RETRY;
  else if(!mutextable[lid])
    ret=lock_protocol::RETRY;
  else {
    pthread_mutex_unlock(mutextable[lid]);
    //print_table();
    stattable[lid]--;
    if(!stattable[lid]) {
      mpool.release(mutextable[lid]);
      mutextable[lid]=NULL;
    }
  }
  pthread_mutex_unlock(mtx);
  return ret;
}
