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
#include "lock_client_cache.h"


using namespace std;

yfs_client::yfs_client(std::string extent_dst,std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}

void
yfs_client::acquire(inum inum){
	lc->acquire(inum);
}
void
yfs_client::release(inum inum){
	lc->release(inum);
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

bool
yfs_client::isfile(inum inum)
{ 
    printf("yfs:enter isfile\n");
    fflush(stdout);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");	
        return false;
    }
    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);	
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */
bool
yfs_client::issymlink(inum inum)
{
    printf("yfs:enter issymink\n");
    fflush(stdout);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");	
        return false;
    }

    if (a.type == extent_protocol::T_SYLINK) {
        printf("issymlink: %lld is a link\n", inum);	
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
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
    printf("yfs:enter getfile\n");
    fflush(stdout);
    int r = OK;
    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
	return r;
    }
    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);
    return r;
}
int
yfs_client::getsymlink(inum inum, symlinkinfo &fin)
{
    printf("yfs:enter getsymlink\n");
    fflush(stdout);
    int r = OK;
    printf("getlink %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
	return r;
    }
    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getlink %016llx -> sz %llu\n", inum, fin.size);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    printf("yfs:enter getdir\n");
    fflush(stdout);
    int r = OK;
    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
	return r;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
	printf("yfs:enter setattr\n");
	fflush(stdout);
	int r = OK;
    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
	if(ino<=0||ino>1024||size<0){
		return IOERR;
	}
	std::string file;
	ec->get(ino,file);
	file.resize(size);
	ec->put(ino,file);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
	printf("yfs:enter create\n");
	fflush(stdout);
        int r = OK;
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
	bool flag;
	inum inode_num;
	unlock_lookup(parent,name,flag,inode_num);	
	if(flag){
		return EXIST;
	}
	printf("yfs:create:parent ,num is:%d\n",(int)parent);
	fflush(stdout);
	ec->create(extent_protocol::T_FILE,ino_out);
	printf("yfs:create:create new inode ,num is:%d,name is %s\n",(int)ino_out,name);
	fflush(stdout);
	std::string parent_content;
	ec->get(parent,parent_content);
	printf("yfs:create:parent content(old):%s\n",parent_content.c_str());
	fflush(stdout);
	std::stringstream os;
	os<<parent_content<<name<<'\r'<<ino_out<<'\r';
	ec->put(parent,os.str());

	std::string new_parent_content;
	ec->get(parent,new_parent_content);
	printf("yfs:create:parent content(new):%s\n",new_parent_content.c_str());
	fflush(stdout);
    	return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
	printf("yfs:enter mkdir,parent:%d,name:%s\n",(int)parent, name);
	fflush(stdout);
        int r = OK;
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */	
	bool flag;
	inum inode_num;
	unlock_lookup(parent,name,flag,inode_num);
	if(flag){	
		return EXIST;
	}
	ec->create(extent_protocol::T_DIR,ino_out);
	std::string parent_content;
	ec->get(parent,parent_content);
	std::stringstream os;
	os<<parent_content<<name<<'\r'<<ino_out<<'\r';
	ec->put(parent,os.str());
	return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
	
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
	printf("yfs:enter lookup,parent:%d,filename:%s\n",(int)parent,name);
	fflush(stdout);
	int r = OK;
	found=false;
	std::list<dirent> dir_file_content;
	if(unlock_readdir(parent,dir_file_content)!=OK){
		return IOERR;
	}
	std::list<dirent>::iterator iter;
	for(iter=dir_file_content.begin();iter!=dir_file_content.end();++iter){
		if(iter->name==name){
			ino_out=iter->inum;
			printf("yfs:lookup:find it,inode num:%d\n",(int)ino_out);
			fflush(stdout);
			found=true;
			break;
		}
	}
    	return r;
}

int
yfs_client::unlock_lookup(inum parent, const char *name, bool &found, inum &ino_out)
{	
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
	printf("yfs:enter unlock_lookup,parent:%d,filename:%s\n",(int)parent,name);
	fflush(stdout);
	int r = OK;
	found=false;
	std::list<dirent> dir_file_content;
	if(unlock_readdir(parent,dir_file_content)!=OK){
		return IOERR;
	}
	std::list<dirent>::iterator iter;
	for(iter=dir_file_content.begin();iter!=dir_file_content.end();++iter){
		if(iter->name==name){
			ino_out=iter->inum;
			found=true;
			break;
		}
	}
    	return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
	printf("yfs:enter readdir,dir num = %d\n",(int)dir);
	fflush(stdout);
    	int r = OK;
    /*
     * your code goes here.
     note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
	std::string content;
	ec->get(dir,content);
	printf("yfs:unlock_readdir:content:%s\n",content.c_str());
	fflush(stdout);
	std::stringstream ss(content);
	std::string read_file_name;
	std::string read_inode_num;
	while(getline(ss,read_file_name,'\r'))
	{
		dirent entry;
		entry.name=read_file_name;
		getline(ss,read_inode_num,'\r');
		std::stringstream is(read_inode_num);
		is>>entry.inum;	
		list.push_back(entry);	
	}
    	return r;
}
int
yfs_client::unlock_readdir(inum dir, std::list<dirent> &list)
{
	printf("yfs:enter unlock_readdir dir num = %d\n",(int)dir);
	fflush(stdout);
    	int r = OK;
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
	std::string content;
	ec->get(dir,content);
	printf("yfs:unlock_readdir:content:%s\n",content.c_str());
	fflush(stdout);
	std::stringstream ss(content);
	std::string read_file_name;
	std::string read_inode_num;
	while(getline(ss,read_file_name,'\r'))
	{
		dirent entry;
		entry.name=read_file_name;
		getline(ss,read_inode_num,'\r');
		std::stringstream is(read_inode_num);
		is>>entry.inum;	
		list.push_back(entry);	
		
	}
    	return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
	printf("yfs:enter read\n");
	fflush(stdout);
    	int r = OK;
    /*
     * your code goes here.
     * note: read using ec->get().
     */
	if(ino<=0||ino>1024||size<0||off<0){
		return IOERR;
	}
	std::string file;
	ec->get(ino,file);
	if(off>=(int)file.size()){
		return r;
	}
	if((file.size()-off)>size){
		data=file.substr(off,size);
	}
	else{
		data=file.substr(off,file.size()-off);
	}
    	return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
	printf("yfs:enter write,inum:%d\n",(int)ino);
	fflush(stdout);
	
	if(ino<=0||ino>1024||size<0||off<0)
		return IOERR;
    	int r = OK;
	std::string file;
	ec->get(ino,file);
	printf("yfs:write:old content:%s\n",file.c_str());
	fflush(stdout);
	extent_protocol::attr a;
    	ec->getattr(ino, a); 
	if(a.size>=(size+off)){
		file = file.substr(0,off)+string(data,size)+file.substr(off+size);
	}
	else if((unsigned)off>=a.size){
		file.resize(off,'\0');
		file.append(data,size);
	}
	else{
		file=file.substr(0,off)+string(data,size);
	}
	printf("yfs:write:new content:%s\n",file.c_str());
	fflush(stdout);
	ec->put(ino,file);	
        return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
	printf("yfs:enter unlink,parent:%d,name:%s\n",(int)parent, name);
	fflush(stdout);
    	int r = OK;
    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
	std::list<dirent> inode_list;
	unlock_readdir(parent,inode_list);
	inum inode_num;
	std::list<dirent>::iterator iter;
	for(iter=inode_list.begin();iter!=inode_list.end();iter++){
		if(iter->name==name){
			inode_num=iter->inum;
			break;
		}
	}
	extent_protocol::attr a;
    	ec->getattr(inode_num, a); 
	if (a.type == extent_protocol::T_DIR) {
		std::string dir_content;
		ec->get(inode_num,dir_content);
		if(dir_content.length()!=0)
			return IOERR;
	}
	ec->remove(inode_num);
	inode_list.erase(iter);
	std::stringstream os;
	std::string parent_content;
	for(iter=inode_list.begin();iter!=inode_list.end();iter++){	
		os<<parent_content<<iter->name<<'\r'<<iter->inum<<'\r';
	}
	ec->put(parent,os.str());
	
	std::string content;
	ec->get(parent,content);
	printf("yfs:unlink:parent content(new):%s\n",content.c_str());
	fflush(stdout);
    	return r;
}

int yfs_client::symlink(inum parent,const char* link,const char* name,inum& ino_out)
{
	printf("yfs:enter symlink\n");
	fflush(stdout);
	bool flag;
	inum ino;
	unlock_lookup(parent,name,flag,ino);
	if(flag){
		return EXIST;
	}
	ec->create(extent_protocol::T_SYLINK,ino_out);
	ec->put(ino_out,link);
	std::string parent_content;
	//add to  parent
	ec->get(parent,parent_content);
	std::stringstream os;
	os<<parent_content<<name<<'\r'<<ino_out<<'\r';
	ec->put(parent,os.str());	
	return OK;
}

int yfs_client::readlink(inum ino,std::string& path)
{
	printf("yfs:enter readlink\n");
	fflush(stdout);
	if(ino<=0||ino>1024){
		return IOERR;
	}
	ec->get(ino,path);
	return OK;
}

bool yfs_client::rename(inum src_dir_ino, std::string src_name, inum dst_dir_ino, std::string dst_name)
{
	printf("yfs:enter rename,src_dir_ino:%d,src_name:%s,dst_dir_ino:%d,dst_name:%s\n",(int)src_dir_ino,src_name.c_str(), (int)dst_dir_ino, dst_name.c_str());
	fflush(stdout);
	std::list<dirent> inode_list;
	unlock_readdir(src_dir_ino,inode_list);
	inum inode_num;
	std::list<dirent>::iterator iter;
	for(iter=inode_list.begin();iter!=inode_list.end();iter++){
		if(iter->name==src_name){
			inode_num=iter->inum;
			break;
		}
	}
	inode_list.erase(iter);
	std::stringstream os;
	std::string parent_content;
	for(iter=inode_list.begin();iter!=inode_list.end();iter++){	
		os<<parent_content<<iter->name<<'\r'<<iter->inum<<'\r';
	}
	ec->put(src_dir_ino,os.str());
	std::string parent_content2;
	ec->get(dst_dir_ino,parent_content2);
	std::stringstream os2;
	os2<<parent_content2<<dst_name<<'\r'<<inode_num<<'\r';
	ec->put(dst_dir_ino,os2.str());
	return true;
}
