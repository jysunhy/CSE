// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}
extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid) {
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  pthread_mutex_lock(&mutex);
  if(_dirty.count(eid)==0) {
    goto ret;
  }
  if(!_dirty[eid]) {
    goto ret;
  }
  if(_cont.count(eid)==0) {
    pthread_mutex_unlock(&mutex);
    ret = cl->call(extent_protocol::remove, eid, r);
    pthread_mutex_lock(&mutex);
    goto ret;
  }else {
    pthread_mutex_unlock(&mutex);
    ret = cl->call(extent_protocol::put, eid, _cont[eid], r);
    pthread_mutex_lock(&mutex);
    goto ret;
  }
 ret:
  if(_cont.count(eid)!=0) {
    _cont.erase(_cont.find(eid));
  }
  if(_attr.count(eid)!=0) {
    _attr.erase(_attr.find(eid));
  }
  if(_dirty.count(eid)!=0) {
    _dirty.erase(_dirty.find(eid));
    //_dirty[eid]=0;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}
extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mutex);
  if(_cont.count(eid)!=0) {
    buf=_cont[eid];
    _attr[eid].atime=time(0);
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if(_dirty.count(eid)==0)
    _dirty[eid]=0;
  /*if(_dirty[eid]==1)
  {
    ret=extent_protocol::NOENT;
    pthread_mutex_unlock(&mutex);
    return ret;
  }*/
  //can add small improvement for get local removed file
    pthread_mutex_unlock(&mutex);
  ret = cl->call(extent_protocol::get, eid, buf);
    pthread_mutex_lock(&mutex);
  if(ret==extent_protocol::OK) {
    _cont[eid]=buf;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mutex);
  if(_dirty.count(eid)==0)
    _dirty[eid]=0;
  if(_attr.count(eid)!=0) {
    attr=_attr[eid];
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  /*if(_dirty[eid]==1)
  {
    ret=extent_protocol::NOENT;
    pthread_mutex_unlock(&mutex);
    return ret;
  }*/
  pthread_mutex_unlock(&mutex);
  ret = cl->call(extent_protocol::getattr, eid, attr);
  pthread_mutex_lock(&mutex);
  if(ret==extent_protocol::OK)
    _attr[eid]=attr;
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mutex);
  if(_dirty.count(eid)==0)
    _dirty[eid]=0;
  if(_attr.count(eid)==0) {
    _attr[eid].ctime=time(0);
    _attr[eid].atime=time(0);
    _attr[eid].mtime=time(0);
    _attr[eid].size=buf.size();
  }else {
    _attr[eid].mtime=time(0);
    _attr[eid].ctime=time(0);
    _attr[eid].size=buf.size();
  }
  if(_cont.count(eid)==0)
  {
    _cont[eid] = buf;
    _dirty[eid]=1;
  }else if(_cont[eid]!=buf) {
    _cont[eid] = buf;
    _dirty[eid]=1;
  }
  //_dirty[eid]=1;
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mutex);
  if(_dirty.count(eid)==0)
    _dirty[eid]=0;
  if(_cont.count(eid)==0 && _attr.count(eid)==0) {
    //VERIFY(_attr.count(eid)==0);
    _dirty[eid]=1;
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if(_cont.count(eid)!=0)
    _cont.erase(_cont.find(eid));
  if(_attr.count(eid)!=0)
    _attr.erase(_attr.find(eid));
  _dirty[eid]=1;
  pthread_mutex_unlock(&mutex);
  return ret;
}
