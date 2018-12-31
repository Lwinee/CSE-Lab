// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include <unistd.h>


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  pthread_mutex_init(&mutex,NULL);
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  char hname[100];
  VERIFY(gethostname(hname, sizeof(hname)) == 0);
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
	pthread_mutex_lock(&mutex);
	//printf("enter client acquire acquireid:%d\n",lid);
	int ret = lock_protocol::OK;
	bool acquire_bool = false;
	if(lock_client_map.find(lid) == lock_client_map.end()){
		lock_status newLock;
		newLock.revoked = false;
		newLock.retryed = false;
		newLock.status = NONE;
		pthread_cond_init(&newLock.retryqueue,NULL);
		pthread_cond_init(&newLock.waitqueue,NULL);
		newLock.wait_num = 0;
		lock_client_map[lid] = newLock;
	}
	lock_client_cache_status status = lock_client_map[lid].status;	
	//printf("client acquire:lock status:%d\n",lock_client_map[lid].status);
	switch(status){
		case NONE:
			{	
				lock_client_map[lid].status = ACQUIRING;
				acquire_bool = true;
				break;
			}
		case FREE:
			{
				lock_client_map[lid].status = LOCKED;
				break;
			}
		case ACQUIRING:
		case LOCKED:
		case RELEASING:
			{
				lock_client_map[lid].wait_num += 1;
				//printf("client acquire:wait_num:%d\n",lock_client_map[lid].wait_num );
				while(lock_client_map[lid].status == ACQUIRING || lock_client_map[lid].status == LOCKED || lock_client_map[lid].status == RELEASING){
					pthread_cond_wait(&lock_client_map[lid].waitqueue ,&mutex); 
				}	
				lock_client_map[lid].wait_num -= 1;
				if(lock_client_map[lid].status == FREE){
					lock_client_map[lid].status = LOCKED;
				}
				if(lock_client_map[lid].status == NONE){
					acquire_bool = true;
					lock_client_map[lid].status = ACQUIRING;
				}
				break;
			}
	}
	while(acquire_bool)
	{
		//printf("client acquire:acquire bool is true\n");
		int r= -1;
		pthread_mutex_unlock(&mutex);
		ret = cl->call(lock_protocol::acquire,lid,id,r);
		pthread_mutex_lock(&mutex);
		if(ret == lock_protocol::OK)
		{	
			//printf("client require:acquire success\n");
			lock_client_map[lid].status = LOCKED;
			break;
		}
		else if(ret == lock_protocol::RETRY)
		{
			//receive retry before RETRY
			if(lock_client_map[lid].retryed == true)
			{
				lock_client_map[lid].retryed = false;
				lock_client_map[lid].status = ACQUIRING;
				acquire_bool = true;
				continue;
			}
			while(lock_client_map[lid].retryed == false)
			{
				pthread_cond_wait(&lock_client_map[lid].retryqueue ,&mutex);
			}
			//printf("client acquire: a retry has waked\n");
			lock_client_map[lid].retryed = false;
			acquire_bool = true;
		}
	}
	pthread_mutex_unlock(&mutex);
	return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{	
	//printf("client release:enter release,release id:%d\n",lid);
	pthread_mutex_lock(&mutex); 
	int ret = rlock_protocol::OK;
	//printf("client release:wait_num:%d\n",lock_client_map[lid].wait_num);
	//printf("client release:status:%d\n",lock_client_map[lid].status);
	if(lock_client_map.find(lid) == lock_client_map.end()){
		printf("no such lock!\n");
		pthread_mutex_unlock(&mutex);
		return ret;
	}
	//other threads are waiting
	if(lock_client_map[lid].wait_num > 0){
		lock_client_map[lid].status = FREE;
		pthread_cond_signal(&lock_client_map[lid].waitqueue);
		pthread_mutex_unlock(&mutex);
		return ret;
	}
	//wait_num=0 & revoked=true
	if(lock_client_map[lid].revoked == true)
	{
		//printf("client release:lock is revoked\n");
		lock_client_map[lid].status = RELEASING;	
		int r = -1;
		pthread_mutex_unlock(&mutex);
		ret = cl->call(lock_protocol::release,lid,id,r);
		pthread_mutex_lock(&mutex);
		lock_client_map[lid].status = NONE;
		pthread_cond_signal(&lock_client_map[lid].waitqueue);
		lock_client_map[lid].revoked = false;
	}
	//wait_num=0 & revoked=false
	else{
		lock_client_map[lid].status = FREE;
	}
	pthread_mutex_unlock(&mutex);
    return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{	
	//printf("enter revoke_handler revokeid= %d\n",lid);
	pthread_mutex_lock(&mutex);
    int ret = rlock_protocol::OK;
	//printf("client revoke handler:status:%d\n",lock_client_map[lid].status);
	if(lock_client_map[lid].status == FREE){
		//printf("client revoke:release soon\n");
		lock_client_map[lid].status = RELEASING;
		int r = -1;
		pthread_mutex_unlock(&mutex);
		ret = cl->call(lock_protocol::release,lid,id,r);
		pthread_mutex_lock(&mutex);
		lock_client_map[lid].status = NONE;	
		pthread_cond_signal(&lock_client_map[lid].waitqueue);
	}
	else{ 
		//printf("client revoke:set revoke=true\n");
		lock_client_map[lid].revoked = true;
	}
	pthread_mutex_unlock(&mutex);
    return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
	//printf("enter retry_handler,retryid=%d\n",lid);
	pthread_mutex_lock(&mutex);
    int ret = rlock_protocol::OK;
	pthread_cond_signal(&lock_client_map[lid].retryqueue);
	lock_client_map[lid].retryed = true;
	pthread_mutex_unlock(&mutex);
    return ret;
}



