// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  shy_lock_release_user* _lru = new shy_lock_release_user();
  _lru->set_ec(ec);
  ((lock_client_cache*)lc)->_lru = _lru; 
  //lc = new lock_client(lock_dst);
  srand(time(0));

}
yfs_client::~yfs_client() {
  delete ec;
  delete lc;
}
yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}
std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}
string yfs_client::map2string(const map<string,yfs_client::dirent>& input) {
  std::ostringstream ost;
  map<string,yfs_client::dirent>::const_iterator iter=input.begin();
  for(; iter!= input.end(); iter++) {
    ost<<iter->second.name<<"\n"<<iter->second.inum<<"\n";
  }
  return ost.str();
}
map<string,yfs_client::dirent> yfs_client::string2dir(string content) {
  map<string,yfs_client::dirent> result;
  istringstream ist(content);
  string name;
  inum num;
  dirent tmp;
  while(ist>>name>>num) {
    tmp.name=name;
    tmp.inum=num;
    result[name]=tmp;
  }
  return result;
}
string yfs_client::append2dir(string old, yfs_client::dirent tail) {
  ostringstream ost;
  ost<<old;
  ost<<tail.name<<"\n"<<tail.inum<<"\n";
  return ost.str();
}

yfs_client::inum yfs_client::generate(bool isfile) {
  inum result(0);
  if(isfile)
    result=1;
  else
    result=0;
  for(int i = 0; i < 31; i++) {
    result = result << 1;
    if(rand()%2) {
      result+=1;
    }
  }
  return result;
}
bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  lock_protocol::lockid_t mutex=inum;
  lc->acquire(mutex);
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the file lock

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:
  lc->release(mutex);

  return r;
}

/*
int
yfs_client::setfile(inum inum, fileinfo fin)
{
  int r = OK;
  printf("setfile %016llx -> sz %llu\n", inum, fin.size);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  a.atime = fin.atime;
  a.mtime = fin.mtime;
  a.ctime = fin.ctime;
  a.size = fin.size;
  if (ec->setattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

 release:
  return r;
}
*/

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  lock_protocol::lockid_t mutex=inum;
  lc->acquire(mutex);
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  lc->release(mutex);
  return r;
}


int
yfs_client::createfile(inum parent, string name, inum& result)
{
  lock_protocol::lockid_t mutex=parent;
  lc->acquire(mutex);
  int r = OK;
  string content,newcontent;
  extent_protocol::attr a;
  inum num;
  dirent tmp;
  if(isfile(parent)) {
    r = IOERR;
    goto release;
  }  
  if(ec->get(parent,content)!=extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  if(string2dir(content).count(name)!=0) {
    r = EXIST;
    goto release;
  }
  while(true) {
    num=generate(1);
    if(ec->getattr(num, a) != extent_protocol::OK)
      break;
  }
  tmp.inum=num;
  tmp.name=name;
  result=num;
  ec->put(num,"");
  newcontent=append2dir(content,tmp);
  printf("new content:%s\n",newcontent.c_str());
  ec->put(parent,newcontent);
 release:
  lc->release(mutex);
  return r;
}
/*
int yfs_client::createroot() {
  pthread_mutex_lock(&mutex);
  ec->put(0x00000001,"");
  pthread_mutex_unlock(&mutex);
  return OK;
}
*/
int
yfs_client::createdir(inum parent, string name, inum& result)
{
  lock_protocol::lockid_t mutex=parent;
  lc->acquire(mutex);
  int r = OK;
  string content;
  string newid;
  extent_protocol::attr a;
  inum num;
  dirent tmp;
  if(isfile(parent)) {
    r = IOERR;
    goto release;
  }  
  if(ec->get(parent,content)!=extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  if(string2dir(content).count(name)!=0) {
    r = EXIST;
    goto release;
  }
  while(true) {
    num=generate(0);
    if(ec->getattr(num, a) != extent_protocol::OK)
      break;
  }
  tmp.inum=num;
  tmp.name=name;
  result=num;
  ec->put(parent,append2dir(content,tmp));
  ec->put(num,"");
 release:
  lc->release(mutex);
  return r;
}

int yfs_client::readfile(inum fnum, string& result) {
  lock_protocol::lockid_t mutex=fnum;
  lc->acquire(mutex);
  int r = OK;
  if(!isfile(fnum)) {
    r = IOERR;
    goto release;
  }  
  if(ec->get(fnum,result)!=extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
 release:
  lc->release(mutex);
  return r;
}

int
yfs_client::writefile(inum fnum, const char* buf, size_t size, off_t off) {
  lock_protocol::lockid_t mutex=fnum;
  lc->acquire(mutex);
  int r = OK;
  string tmp;
  if(!isfile(fnum)) {
    r = IOERR;
    goto release;
  }  
  r=ec->get(fnum,tmp);
  if(r != yfs_client::OK){
    r = IOERR;
    goto release;
  }
  if(off+size<=tmp.size()) {
    tmp.replace(off,size,buf,size);
  }else if(off<=tmp.size()) {
    tmp.replace(off,tmp.size()-off,buf,size);
  }else {
    int oldsize=tmp.size();
    for(int i = 0; i < off-oldsize;i++) {
      tmp.push_back('\0');
    }
    tmp.replace(tmp.size(),0,buf,size);
  }
  ec->put(fnum,tmp);
 release:
  lc->release(mutex);
  return r;
}

int
yfs_client::writefile(inum fnum, string content)
{
  lock_protocol::lockid_t mutex=fnum;
  lc->acquire(mutex);
  //pthread_mutex_lock(&mutex);
  int r = OK;
  if(!isfile(fnum)) {
    r = IOERR;
    goto release;
  }  
  ec->put(fnum,content);
 release:
  lc->release(mutex);
  //pthread_mutex_unlock(&mutex);
  return r;
}
int yfs_client::readdir(inum dir, map<string,dirent>& result) {
  lock_protocol::lockid_t mutex=dir;
  lc->acquire(mutex);
  //pthread_mutex_lock(&mutex);
  int r = OK;
  result.clear();
  string content;
  if(isfile(dir)) {
    r = IOERR;
    goto release;
  }  
  if(ec->get(dir,content)!=extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  result=string2dir(content);
 release:
  lc->release(mutex);
  //pthread_mutex_unlock(&mutex);
  return r;
}

int yfs_client::lookup(inum dir, string name, dirent& result) {
  lock_protocol::lockid_t mutex=dir;
  lc->acquire(mutex);
  //pthread_mutex_lock(&mutex);
  printf("in lookup %llu/%s\n",dir,name.c_str());
  int r = OK;
  string content;
  if(isfile(dir)) {
    r = IOERR;
    goto release;
  }  
  if(ec->get(dir,content)!=extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  printf("content:%s\n",content.c_str());
  if(string2dir(content).count(name)==0)
    r=IOERR;
  else {
    result=string2dir(content)[name];
  }
 release:
  //pthread_mutex_unlock(&mutex);
  lc->release(mutex);
  printf("look up returns %d\n",r);
  return r;
}

int yfs_client::rmfile(inum dir,string name) {
  lock_protocol::lockid_t mutex=dir;
  lc->acquire(mutex);
  //pthread_mutex_lock(&mutex);
  printf("in rmfiles %llu/%s\n",dir,name.c_str());
  int r = OK;
  string content;
  map<string,dirent> buf_map;
  if(!isdir(dir)) {
    r = IOERR;
    goto release;
  }  
  if(ec->get(dir,content)!=extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  buf_map=string2dir(content);
  if(buf_map.count(name)==0)
    r=IOERR;
  else {
    buf_map.erase(buf_map.find(name));
    ec->put(dir,map2string(buf_map));
    ec->remove(buf_map[name].inum);
  }
 release:
  lc->release(mutex);
  //pthread_mutex_unlock(&mutex);
  return r;
}

int yfs_client::setattr(inum inum,size_t to_size) {
  lock_protocol::lockid_t mutex=inum;
  lc->acquire(mutex);
  int r = OK;
  string old;
  if(!isfile(inum)){
    r = IOERR;
    goto release;
  }
  r=ec->get(inum,old);
  if(r != yfs_client::OK){
    r = IOERR;
    goto release;
  }
  if(to_size < old.size())
    ec->put(inum,old.substr(0,to_size));
  else if(to_size > old.size()) {
    int oldsize=old.size();
    for(unsigned int i = 0; i < to_size-oldsize; i++)
      old.push_back('\0');
    ec->put(inum,old);
  }
  
 release:
  lc->release(mutex);
  return r;
}
