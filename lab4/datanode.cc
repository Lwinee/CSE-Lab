#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
  ec = new extent_client(extent_dst);

  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);
  if (!ConnectToNN()) {
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode()) {
    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }

  /* Add your initialization here */
  NewThread(this, &DataNode::send, 1);

  return 0;
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
	printf("datanode:enter ReadBlock,bid=%d,len=%d,offset=%d\n",(int)bid,(int)len,(int)offset);
	fflush(stdout);
	string full_buf;
	int r = ec->read_block(bid, full_buf);
	printf("datanode:ReadBlock,full_buf:%s\n",full_buf.c_str());
	fflush(stdout);
	buf = full_buf.substr(offset,offset+len);
	printf("datanode:ReadBlock,buf:%s\n",buf.c_str());
	fflush(stdout);
	if(r == extent_protocol::OK)
		return true;
	return false;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
  /* Your lab4 part 2 code */
	printf("datanode:enter WriteBlock,bid=%d,len=%d,offset=%d\n",(int)bid,(int)len,(int)offset);
	fflush(stdout);
	printf("datanode:WriteBlock:buf=%s\n",buf.c_str());
	fflush(stdout);
	string full_buf;
	int r = ec->read_block(bid, full_buf);
	printf("datanode:WriteBlock:(old)=%s\n",full_buf.c_str());
	fflush(stdout);
	string temp = full_buf.substr(0,offset)+buf.substr(0,len)+full_buf.substr(offset+len,BLOCK_SIZE);
	printf("datanode:WriteBlock:(new)=%s\n",temp.c_str());
	fflush(stdout);
	r = ec->write_block(bid, temp);
	if(r == extent_protocol::OK)
		return true;
    return false;
}

void DataNode::send(int time_period){
	while(true){
		SendHeartbeat();
		sleep(time_period);
	}
}


