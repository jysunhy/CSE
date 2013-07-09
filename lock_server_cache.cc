// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

int debug=1;

lock_server_cache::lock_server_cache()
{
  mtx=mpool.get();
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  if(debug)
    tprintf("acquire in from %s for %llu\n",id.c_str(),lid);
  int r;
  lock_protocol::status ret = lock_protocol::OK;
  handle h(id);
  rpcc *cl = h.safebind();
  pthread_mutex_lock(mtx);
  //if(retrying.count(lid)==0)
  //  retrying[lid]="";
  //if(revoking.count(lid)==0)
  //  revoking[lid]="";
  if(cl){
    if(list.count(lid)==0) {
      list[lid]=queue<string>();
      list[lid].push(id);
      if(debug) {
        tprintf("new lock acquire from %s for %llu\n",id.c_str(),lid);
        tprintf("pushed to queue\n");
        print_queue(lid);
      }
    }else {
      if(list[lid].size()==0) {
        list[lid].push(id);
        if(debug)
          tprintf("acquire when empty from %s for %llu\n",id.c_str(),lid);
        if(debug)
          tprintf("pushed to queue\n");
        print_queue(lid);
      }else if(list[lid].size()==1) {
        if(debug)
          tprintf("acquire when one before from %s for %llu\n",id.c_str(),lid);
        if(debug)
          print_queue(lid);
        string curid = list[lid].front();
        {
          {
            if(debug)
              tprintf("revoke front to this\n");
            list[lid].push(id);
            pthread_mutex_unlock(mtx);
            handle curh(curid);
            rpcc* curcl = curh.safebind();
            if(!curcl)
              tprintf("rpcc client error in revoking\n");
            if(debug)
              tprintf("sending revoke to %s\n",curid.c_str());
            //while(curcl->call(rlock_protocol::revoke,lid,r)!=rlock_protocol::OK)
            if(curcl->call(rlock_protocol::revoke,lid,r)!=rlock_protocol::OK)
            {
              //sleep(1);
              cout<<"**resending revoke to "<<curid<<endl;
            }
            if(debug)
              tprintf("sent revoke to %s\n",curid.c_str());
            pthread_mutex_lock(mtx);
        //    revoking[lid]="";
            ret=lock_protocol::RETRY;
          }
        }
      }else {
        list[lid].push(id);
        ret=lock_protocol::RETRY;
      }
    }
  }else {
    ret = lock_protocol::IOERR;
  }
  pthread_mutex_unlock(mtx);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  if(debug)
    tprintf("release in from %s for %llu\n",id.c_str(),lid);
  int tmp;
  lock_protocol::status ret = lock_protocol::OK;
  handle h(id);
  rpcc *cl = h.safebind();
  pthread_mutex_lock(mtx);
  if(debug)
    print_queue(lid);
  if(cl){
    if(list.count(lid)==0) {
      if(debug)
        tprintf("lid not exist\n");
      ret=lock_protocol::IOERR;
    }else {
      if(list[lid].size()==0) {
        if(debug)
          tprintf("lid list empty\n");
        ret=lock_protocol::IOERR;
      }else  {
        if(id!=list[lid].front())
        {
          if(debug)
            cout<<id<<" release "<<id<<" not the head"<<list[lid].front()<<endl;
          if(debug)
            print_queue(lid);
          ret=lock_protocol::IOERR;
        }else {
          int oldsize;
          //retrying[lid]="";
          list[lid].pop();
          if(debug)
            print_queue(lid);
          oldsize=list[lid].size();
          if(list[lid].size()!=0) {
            string curid = list[lid].front();
            //retrying[lid]=curid;
            if(debug)
              tprintf("sending retry to %s \n",curid.c_str());
            pthread_mutex_unlock(mtx);
            handle curh(curid);
            rpcc* curcl = curh.safebind();
            curcl->call(rlock_protocol::retry,lid,tmp);
            pthread_mutex_lock(mtx);
            if(debug)
              tprintf("sent retry to %s \n",curid.c_str());
            if(list[lid].size()>1) {
              if(oldsize==1)
              {
                if(debug)
                  tprintf("wait new acquire to revoke %s\n",curid.c_str());
              }else
              {
                if(debug)
                  tprintf("revoke immedietely after retry\n");
                if(debug)
                  tprintf("sending revoke to %s\n",curid.c_str());
                pthread_mutex_unlock(mtx);
                //sleep(1);
                //while(curcl->call(rlock_protocol::revoke,lid,r)!=rlock_protocol::OK) 
                if(curcl->call(rlock_protocol::revoke,lid,r)!=rlock_protocol::OK) 
                {
                  tprintf("**resending revoke to %s\n",curid.c_str());
                  //sleep(1);
                }
                pthread_mutex_lock(mtx);
                if(debug)
                  tprintf("sent revoke to %s\n",curid.c_str());
                //revoking[lid]="";
              }
            }
            //retrying[lid]="";
          }
        }
      }
    }
  }
  pthread_mutex_unlock(mtx);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

