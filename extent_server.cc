// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
  int tmp;
  put(1,"",tmp);
}



int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  printf("put %llu \n%s\n****************\n",id,buf.c_str());
  pthread_mutex_lock(&mutex);
  if(_attr.count(id)==0) {
    _attr[id].ctime=time(0);
    _attr[id].atime=time(0);
    _attr[id].mtime=time(0);
    _attr[id].size=buf.size();
  }else {
    _attr[id].mtime=time(0);
    _attr[id].size=buf.size();
  }
  _cont[id] = buf;
  pthread_mutex_unlock(&mutex);
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("get %llu\n",id);
  pthread_mutex_lock(&mutex);
  if(_cont.count(id)==0) {
    pthread_mutex_unlock(&mutex);
    printf("unfound %llu\n",id);
    return extent_protocol::NOENT;
  }
  buf=_cont[id];
  printf("set buf to %s\n",buf.c_str());
  _attr[id].atime=time(0);
  pthread_mutex_unlock(&mutex);
  printf("get %llu %s\n*****************\n",id,buf.c_str());
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("getattr %llu\n",id);
  pthread_mutex_lock(&mutex);
  if(_attr.count(id)==0) {
    pthread_mutex_unlock(&mutex);
    printf("unfound id %llu\n",id);
    return extent_protocol::NOENT;
  }
  a=_attr[id];
  pthread_mutex_unlock(&mutex);
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("remove %llu\n",id);
  pthread_mutex_lock(&mutex);
  if(_cont.count(id)==0) {
    pthread_mutex_unlock(&mutex);
    return extent_protocol::NOENT;
  }
  _cont.erase(_cont.find(id));
  _attr.erase(_attr.find(id));
  pthread_mutex_unlock(&mutex);
  
  return extent_protocol::IOERR;
}

