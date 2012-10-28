#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>

#include "lock_protocol.h"
#include "lock_client.h"
#include <map>
using namespace std;

class yfs_client {
  extent_client *ec;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  static map<string,dirent> string2dir(string);
  static string append2dir(string,dirent);
  static inum generate(bool);
 // int createroot();
  pthread_mutex_t mutex;
 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int createfile(inum,string,inum&);
  int createdir(inum,string,inum&);

  int readfile(inum,string&);
  int writefile(inum,string);

  int readdir(inum,map<string,dirent>&);
  int lookup(inum,string,dirent&);
  
};

#endif 
