// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
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
  srand(time(0));

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
  int r = OK;
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
  int r = OK;

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
  return r;
}


int
yfs_client::createfile(inum parent, string name, inum& result)
{
  pthread_mutex_lock(&mutex);
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
  pthread_mutex_unlock(&mutex);
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
  pthread_mutex_lock(&mutex);
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
  pthread_mutex_unlock(&mutex);
  return r;
}

int yfs_client::readfile(inum fnum, string& result) {
  pthread_mutex_lock(&mutex);
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
  pthread_mutex_unlock(&mutex);
  return r;
}

int
yfs_client::writefile(inum fnum, string content)
{
  pthread_mutex_lock(&mutex);
  int r = OK;
  if(!isfile(fnum)) {
    r = IOERR;
    goto release;
  }  
  ec->put(fnum,content);
 release:
  pthread_mutex_unlock(&mutex);
  return r;
}
int yfs_client::readdir(inum dir, map<string,dirent>& result) {
  pthread_mutex_lock(&mutex);
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
  pthread_mutex_unlock(&mutex);
  return r;
}

int yfs_client::lookup(inum dir, string name, dirent& result) {
  pthread_mutex_lock(&mutex);
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
  pthread_mutex_unlock(&mutex);
  printf("look up returns %d\n",r);
  return r;
}

