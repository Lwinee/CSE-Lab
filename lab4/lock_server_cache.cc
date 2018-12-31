// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache():
	nacquire (0)
{
	pthread_mutex_init(&mutex,NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &)
{
  pthread_mutex_lock(&mutex);
  //printf("enter server acquire\n");
  lock_protocol::status ret = lock_protocol::OK;
  bool revoke_bool = false;
  if(lock_server_map.find(lid) == lock_server_map.end()){
	  lock_status newLock;
	  newLock.owner = id;
	  newLock.status = LOCKED;
	  newLock.retrying_client = "";
	  lock_server_map[lid] = newLock;
	  pthread_mutex_unlock(&mutex);
	  return ret;
  }	
  lock_server_cache_status status = lock_server_map[lid].status;
  switch(status){
	  case FREE:
		  {
			  //printf("server acquire:lock is free\n");
			  lock_server_map[lid].owner = id;
			  lock_server_map[lid].status = LOCKED;
			  break;
		  }
	  case LOCKED:
		  {
			  //printf("server acquire:lock is locked\n");
			  lock_server_map[lid].waiting_set.insert(id);
			  lock_server_map[lid].status = LOCKED_WAIT;
			  ret = lock_protocol::RETRY;
			  revoke_bool = true;
			  break;
		  }
	  case LOCKED_WAIT:
		  {
			  //printf("server acquire:lock is locked-wait\n");
			  lock_server_map[lid].waiting_set.insert(id);
			  ret = lock_protocol::RETRY;
			  break;
		  }
	  case RETRYING:
		  {
			  //printf("server acquire:lock is retrying\n");
			  std::string retryingId = lock_server_map[lid].retrying_client;
			  if(id == retryingId)
			  {
				  lock_server_map[lid].waiting_set.erase(lock_server_map[lid].waiting_set.find(retryingId));
				  lock_server_map[lid].owner = id;
				  //printf("server acquire:waitingset_size:%d\n",lock_server_map[lid].waiting_set.size());
				  if(lock_server_map[lid].waiting_set.size() == 0)
					  lock_server_map[lid].status = LOCKED;
				  else
				  {
					  lock_server_map[lid].status = LOCKED_WAIT;
					  revoke_bool = true;
				  }
				  lock_server_map[lid].retrying_client = ' ';
			  }
			  else
			  {
				  lock_server_map[lid].waiting_set.insert(id);
				  ret = lock_protocol::RETRY;
			  }
			  break;
		  }
  }
  //revoke
  if(revoke_bool){  
	  //printf("server acquire:revoke bool is true\n");
	  handle h(lock_server_map[lid].owner);
	  rpcc *cl = h.safebind();
	  if(cl)
	  {
		  pthread_mutex_unlock(&mutex);
		  int r = cl->call(rlock_protocol::revoke,lid,r);
		  pthread_mutex_lock(&mutex);
	  }
	  else
		  printf("Bind failed\n");
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int &r)
{ 
	pthread_mutex_lock(&mutex);
	lock_protocol::status ret = lock_protocol::OK;
	//printf("enter server release\n");
	if((lock_server_map[lid].status == FREE)||(lock_server_map[lid].status == RETRYING))
	{
		printf("server release:the lock has freed.");
    }
    else if(lock_server_map[lid].status == LOCKED)
	{
		lock_server_map[lid].status = FREE;
	    lock_server_map[lid].owner = ' ';
    }
	else if(lock_server_map[lid].status == LOCKED_WAIT)
	{
		lock_server_map[lid].status = RETRYING;
		//retry
		std::set<std::string>::iterator iter = lock_server_map[lid].waiting_set.begin(); 
		lock_server_map[lid].retrying_client = *iter;
		handle h(*iter);
		rpcc *cl = h.safebind();
		if(cl)
		{ 
			pthread_mutex_unlock(&mutex);
			int r = cl->call(rlock_protocol::retry,lid,r);
			pthread_mutex_lock(&mutex);
		}
		else
			printf("Bind failed\n");
	} 
	pthread_mutex_unlock(&mutex);
	return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

