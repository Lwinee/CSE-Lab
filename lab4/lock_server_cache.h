#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <map>
#include <set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
  pthread_mutex_t mutex;  
  enum lock_server_cache_status {FREE, LOCKED, LOCKED_WAIT, RETRYING};
  struct lock_status{
	  lock_status(){};
	  ~lock_status(){};
	  std::string owner;
	  std::set<std::string> waiting_set;
	  lock_server_cache_status status;
	  std::string retrying_client;
  };
  std::map<lock_protocol::lockid_t,lock_status> lock_server_map;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
