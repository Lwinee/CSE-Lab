#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"
#include "time.h"
using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
	std::list<blockid_t> block_ids;
	ec->get_block_ids(ino, block_ids);
	extent_protocol::attr a;
	ec->getattr(ino, a);
	std::list<LocatedBlock> list;
	std::list<blockid_t>::iterator iter;
	int index=0;
	for (iter=block_ids.begin();iter!=block_ids.end();iter++)
	{
		int size = 0;
		if((int)(index+1)*BLOCK_SIZE < (int)a.size)
			size = BLOCK_SIZE;
		else
			size = a.size - index*BLOCK_SIZE;
		std::list<DatanodeIDProto> full_LiveList = GetDatanodes();
		full_LiveList.push_back(master_datanode);
		LocatedBlock namenode(*iter,index*BLOCK_SIZE,size,full_LiveList);
		list.push_back(namenode);
		index++;
		/*written_blocks*/
		std::list<DatanodeIDProto>::iterator it;
		for (it = full_LiveList.begin(); it != full_LiveList.end(); it++){
			DataNodeList[*it].written_blocks.insert(*iter);
		}
	}
	
	return list;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
	int r = ec->complete(ino, new_size);
	if(r == extent_protocol::OK){
		lc->release(ino);
		return true;
	}
	lc->release(ino);
    	return false;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
	blockid_t bid;
	int r = ec->append_block(ino,bid);
	if (r != extent_protocol::OK)
		throw HdfsException("Not implemented");
	/*written_blocks*/
	std::list<DatanodeIDProto> full_LiveList = GetDatanodes();
	full_LiveList.push_back(master_datanode);
	LocatedBlock namenode = LocatedBlock(bid, 0, 0, full_LiveList);
	std::list<DatanodeIDProto>::iterator it;
	for (it = full_LiveList.begin(); it != full_LiveList.end(); it++){
		DataNodeList[*it].written_blocks.insert(bid);
	}
	return namenode;
    	
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
	bool r = yfs->rename(src_dir_ino, src_name, dst_dir_ino, dst_name);
  	return r;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
	int r = yfs->mkdir(parent, name.c_str(), mode, ino_out);
	if(r == extent_protocol::OK)
		return true;
    	return false;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {	
	int r = yfs->create(parent, name.c_str(), mode, ino_out);
	if(r == extent_protocol::OK){
		lc->acquire(ino_out);
		return true;
	}
	return false;
}

bool NameNode::Isfile(yfs_client::inum ino) {
	bool r = yfs->isfile(ino);
   	return r;
}

bool NameNode::Isdir(yfs_client::inum ino) {
	bool r = yfs->isdir(ino);
	return r;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
	int r = yfs->getfile(ino, info);
	if(r == extent_protocol::OK)
		return true;
	return false;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
	int r = yfs->getdir(ino, info);
	if(r == extent_protocol::OK)
		return true;
	return false;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
	int r = yfs->readdir(ino, dir);
	if(r == extent_protocol::OK)
		return true;
	return false;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
	int r = yfs->unlink(parent, name.c_str());
	if(r == extent_protocol::OK)
		return true;
    	return false;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
	unsigned int now=(unsigned int)time(NULL);
	if(DataNodeList.count(id) == 0)
		return;
	if (DataNodeList[id].live == false){	
		DataNodeList[id].live = true;
		LiveList.push_back(id);
	}
	DataNodeList[id].last_time =now;
	return;
}

void NameNode::RegisterDatanode(DatanodeIDProto id) 
{
	struct DataNodeInfo new_node_info;
	DataNodeList[id] = new_node_info;
	Replicate_Block(id);
	DataNodeList[id].live = true;
	DataNodeList[id].last_time = (unsigned int)time(NULL);
	LiveList.push_back(id);
	NewThread(this,&NameNode::Monitor,id);
}

void NameNode::Monitor(DatanodeIDProto id){
	while(1){
		unsigned int now = (unsigned int)time(NULL);
		if (now - DataNodeList[id].last_time >= 5){
			DataNodeList[id].live = false;
			LiveList.remove(id);
			return;
		}
	}

}

void NameNode::Replicate_Block(DatanodeIDProto id){
	set<blockid_t> master = DataNodeList[master_datanode].written_blocks;
	std::set<blockid_t>::iterator it;
	DataNodeList[id].written_blocks.clear();
	for(it = master.begin();it!=master.end(); it++){
		bool r = ReplicateBlock(*it,master_datanode,id);
		if (r){
			DataNodeList[id].written_blocks.insert(*it);
		}
	}
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
	return LiveList;
}
