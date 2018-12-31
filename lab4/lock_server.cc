// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
	pthread_mutex_init(&mutex,NULL);
	pthread_cond_init(&cond,NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("acquire lockID:%d\n",(int)lid);
  lock_protocol::status ret = lock_protocol::OK;
 // Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  std::map<lock_protocol::lockid_t,int>::iterator iter;
  iter=locks.find(lid);
  if(iter==locks.end()){
	printf("no such num lock and create a new lock\n");
	  locks[lid]=1;
  }
  else{
	  while(locks[lid]==1){
		printf("wait for valid lock\n");
		  pthread_cond_wait(&cond,&mutex);
	  }
	printf("wait success earn lock\n");
	  locks[lid]=1;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("release lockID:%d\n",(int)lid);
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  std::map<lock_protocol::lockid_t,int>::iterator iter;
  iter=locks.find(lid);
  if(iter==locks.end()){
	  ret=lock_protocol::NOENT;
  }
  else{
	  locks[lid]=0;
	  pthread_cond_signal(&cond);
	printf("success release lockID:%d\n",(int)lid);
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}
