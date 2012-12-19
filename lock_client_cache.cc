// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();
  mtx = mpool.get();
}
lock_client_cache::~lock_client_cache(){
/*    int r;
    map<lock_protocol::lockid_t,lcstatus>::iterator iter = _stats_map.begin();
    for(;iter!=_stats_map.end();iter++) {
      if(iter->second != NONE)
        cl->call(lock_protocol::release, iter->first, id, r);
    }
*/
}
lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  int r;
  ret = lock_protocol::OK;
  pthread_mutex_lock(mtx);
  if(_stats_map.count(lid) == 0 )
    _stats_map[lid]=NONE;
  if(_cnt_map.count(lid) == 0 )
    _cnt_map[lid]=0;
  if(_lock_map.count(lid) == 0)
    _lock_map[lid]=mpool.get();
  else if(_lock_map[lid] == NULL)
    _lock_map[lid]=mpool.get();
  if(_acq_map.count(lid) == 0)
    _acq_map[lid]=0;
  _cnt_map[lid]++;
  pthread_mutex_unlock(mtx);
 acq:
  pthread_mutex_lock(_lock_map[lid]);
  pthread_mutex_lock(mtx);
  if(_lock_map.count(lid) == 0)
    _lock_map[lid]=mpool.get();
  else if(_lock_map[lid] == NULL)
    _lock_map[lid]=mpool.get();
  if(_stats_map[lid]==FREE) {
    _stats_map[lid]=LOCKED;
    goto rret;
  }else if(_stats_map[lid]==LOCKED) {
    //tprintf("%s LOCKED cannot be locked again\n",id.c_str());
    ret = lock_protocol::IOERR;
    goto rret;
  }else if(_stats_map[lid]==RELEASING) {
    goto rret;
  }else if(_stats_map[lid]==ACQUIRING) {
    pthread_mutex_unlock(_lock_map[lid]);
    //_cnt_map[lid]--;
    pthread_mutex_unlock(mtx);
    goto acq;
  }else if(_stats_map[lid]==NONE) {
    _stats_map[lid]=ACQUIRING;
    //tprintf("%s will send acquire %llu\n",id.c_str(),lid);
    pthread_mutex_unlock(mtx);
    //pthread_mutex_lock(_call_lock_map[lid]);
    ret = cl->call(lock_protocol::acquire, lid, id, r);
    //pthread_mutex_unlock(_call_lock_map[lid]);
    pthread_mutex_lock(mtx);
    if(_stats_map[lid]==RELEASING) {
      pthread_mutex_unlock(mtx);
      goto acq;
      //pthread_mutex_lock(_lock_map[lid]);
      //goto rret;
    }else if(_stats_map[lid]==ACQUIRING) {
      if(ret==lock_protocol::OK) { 
        _stats_map[lid]=LOCKED;
        goto rret;
      }else if(ret==lock_protocol::RETRY) {
        pthread_mutex_unlock(mtx);
        goto acq;
      }else {
        //tprintf("%s call acquire return bad\n",id.c_str());
      }
    }else if(_stats_map[lid]==NONE) {
        pthread_mutex_unlock(mtx);
        goto acq;
    }else if(_stats_map[lid]==FREE) {
        pthread_mutex_unlock(mtx);
        goto acq;
    }else if(_stats_map[lid]==LOCKED) {
        pthread_mutex_unlock(mtx);
        goto acq;
    }else {
      //tprintf("%s call acquire state back error\n",id.c_str());
    }
  }
 rret:
  _acq_map[lid]++;
  pthread_mutex_unlock(mtx);
  return ret;
}
lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int r;
  pthread_mutex_lock(mtx);
  //_rel_map[lid]++;
  //_cnt_map[lid]--;
  if(_stats_map[lid]==RELEASING) {
    _acq_map[lid]=0;
    _stats_map[lid]=NONE;
    pthread_mutex_unlock(_lock_map[lid]);
    pthread_mutex_unlock(mtx);
    cl->call(lock_protocol::release,lid, id, r);
    pthread_mutex_lock(mtx);
    _cnt_map[lid]--;
    if(_cnt_map[lid]==0) {
      mpool.release(_lock_map[lid]);
      _lock_map[lid]=NULL;
    }
    //incre_gen(lid);
    pthread_mutex_unlock(mtx);
  }else {
    _stats_map[lid]=FREE;
    pthread_mutex_unlock(_lock_map[lid]);
    _cnt_map[lid]--;
    if(_cnt_map[lid]==0) {
      mpool.release(_lock_map[lid]);
      _lock_map[lid]=NULL;
    }
    pthread_mutex_unlock(mtx);
  }
  return lock_protocol::OK;

}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  int r;
revk:
  pthread_mutex_lock(mtx);
  if(_stats_map[lid]==NONE) {
    //cout<<id<<" lock "<<lid<<" is not acquired"<<endl;
    pthread_mutex_unlock(mtx);
    return rlock_protocol::RPCERR;
  }
  if(_stats_map[lid]==RELEASING) {
    //cout<<id<<" lock "<<lid<<" receive revoke twice"<<endl;
    pthread_mutex_unlock(mtx);
    return rlock_protocol::RPCERR;
  }
  if(_stats_map[lid]==ACQUIRING) {
   // tprintf("%s receive revoke for lock %llu with client status ACQUIRING\n",id.c_str(),lid);
    //_stats_map[lid]=RELEASING;
    pthread_mutex_unlock(mtx);
    return rlock_protocol::RPCERR;
    //return rlock_protocol::OK;
  }
  if(_stats_map[lid]==FREE) {
    tprintf("cnt map for %llu is %d\n",lid,_cnt_map[lid]);
    tprintf("acq map for %llu is %d\n",lid,_acq_map[lid]);
    if(_acq_map[lid]==0 && _cnt_map[lid]>0) {
      tprintf("%s receive revoke for lock %llu with client status FREE but not used\n",id.c_str(),lid);
      _stats_map[lid]=RELEASING;
      pthread_mutex_unlock(mtx);
      //sleep(1);
      //goto revk;
    }else 
    {
    //tprintf("%s receive revoke for lock %llu with client status FREE\n",id.c_str(),lid);
    _stats_map[lid]=NONE;
    _acq_map[lid]=0;
    //pthread_mutex_lock(_call_lock_map[lid]);
    //pthread_mutex_unlock(mtx);
    //tprintf("%s will send release for lock %llu\n",id.c_str(),lid);
    cl->call(lock_protocol::release, lid, id, r);
    //pthread_mutex_unlock(_call_lock_map[lid]);
    //tprintf("%s sent release for lock %llu\n",id.c_str(),lid);
    //pthread_mutex_lock(mtx);
    //incre_gen(lid);
    pthread_mutex_unlock(mtx);
    }
  }else {
    //tprintf("%s receive revoke for lock %llu with client status LOCKED\n",id.c_str(),lid);
    _stats_map[lid]=RELEASING;
    pthread_mutex_unlock(mtx);
  }
  //cout<<id<<" revoke returns;"<<endl;
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(mtx);
 // tprintf("%s get retry %llu command\n",id.c_str(),lid);
  if(_stats_map[lid]==LOCKED) {
   // tprintf("%s receive retry for lock %llu with client status LOCK\n",id.c_str(),lid);
    _stats_map[lid]=FREE;
    pthread_mutex_unlock(_lock_map[lid]);
    //_cnt_map[lid]--;
  }else if(_stats_map[lid]==ACQUIRING){
    //tprintf("%s receive retry for lock %llu with client status ACQUIRING\n",id.c_str(),lid);
    _stats_map[lid]=FREE;
    pthread_mutex_unlock(_lock_map[lid]);
    //_cnt_map[lid]--;
  }else if(_stats_map[lid]==RELEASING){
    pthread_mutex_unlock(_lock_map[lid]);
    //_cnt_map[lid]--;
    //tprintf("%s receive retry for lock %llu with client status RELEASING\n",id.c_str(),lid);
  }else if(_stats_map[lid]==FREE) {
    //tprintf("%s receive retry for lock %llu with client status FREE\n",id.c_str(),lid);
  }else if(_stats_map[lid]==NONE){
    //tprintf("%s receive retry for lock %llu with client status NONE\n",id.c_str(),lid);
  }
  pthread_mutex_unlock(mtx);
  //cout<<id<<" retry returns;"<<endl;
  return ret;
}



