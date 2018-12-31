#include "inode_manager.h"
#include "time.h"
#include <list>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
	if(id<0||id>=BLOCK_NUM||buf==NULL){
		return;
	}
	memcpy(buf,blocks[id],BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
	if(id<0||id>=BLOCK_NUM||buf==NULL)
	{
		return;
	}
	memcpy(blocks[id],buf,BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.

/*
void
block_manager::free_block(uint32_t id)
{
 
	if (id < 0 || id >= BLOCK_NUM){
		return;
	}
	char temp[BLOCK_SIZE];
	d->write_block(id, temp);
	using_blocks.erase(id);
  	return;
}*/

blockid_t
block_manager::alloc_block()
{
  
	blockid_t first_data_block=IBLOCK(1024,BLOCK_NUM)+1;
	blockid_t last_bitmap_block=BBLOCK(BLOCK_NUM);
	blockid_t first_bitmap_block=BBLOCK(first_data_block);
	int block_num;
	bool flag=false;
	for(int i=(int)first_bitmap_block;i<=(int)last_bitmap_block;i++){
		char buf[BLOCK_SIZE];
		read_block(i,buf);
		for(int j=(i==2?1034:0);j<BPB;j++){
			int byteIdx=j/8;
			int bitIdx=j%8;
			if(((buf[byteIdx]>>(7-bitIdx))&1)==1){
				continue;
			}
			else{
				block_num=j+(i-first_bitmap_block)*BPB+1;
				buf[byteIdx]=(1<<(7-bitIdx))|buf[byteIdx];
				write_block(i,buf);
				flag=true;
				break;
			}
		}
		if(flag)
			break;
	}
	return block_num;
}

void
block_manager::free_block(uint32_t id)
{
 
	int bitmap_num=BBLOCK(id);
	int bitmapIdx;
	char* buf=(char*)malloc(sizeof(char)*BLOCK_SIZE);

	if(id%(BLOCK_SIZE*8)==0){
		bitmapIdx=BLOCK_SIZE*8-1;
	}
	else{
		bitmapIdx=id%(BLOCK_SIZE*8)-1;
	}
	int byteIdx=bitmapIdx/8;
	int bitIdx=bitmapIdx%8;

	read_block(bitmap_num,buf);
	buf[byteIdx]=(~(1<<(7-bitIdx))&buf[byteIdx]);
	write_block(bitmap_num,buf);
	char temp[BLOCK_SIZE];
	d->write_block(id, temp);
	return;
}




// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
			
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
	printf("im:enter alloc_inode\n");
	fflush(stdout);
	unsigned int now=(unsigned int)time(NULL);
	struct inode* new_inode=(inode*)malloc(sizeof(struct inode));
	new_inode->type=type;
	new_inode->size=0;
	new_inode->ctime=now;
	new_inode->mtime=now;
	new_inode->atime=now;
	int inodeIdx=1;
	while(get_inode(inodeIdx)!=NULL){
		inodeIdx++;
		printf("im:alloc_inode,inodeIdx:%d\n",inodeIdx);
		fflush(stdout);
	}
	put_inode(inodeIdx,new_inode);
	free(new_inode);
	return inodeIdx;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
	printf("im:enter free_inode inum:%d\n",(int)inum);
	fflush(stdout);
	if(inum<=0||inum>1024)
		return;
	struct inode* inode=get_inode(inum);
	if(inode==NULL)
		return;
	inode->type=0;
	unsigned int now=time(NULL);
	inode->mtime=now;
	inode->ctime=now;
	put_inode(inum,inode);
	free(inode);
	return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum <= 0 || inum > INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + (inum - 1)%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{

  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + (inum - 1)%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
	printf("im:enter read_file\n");
	fflush(stdout);
	struct inode* inode=get_inode(inum);
	if(inode==NULL)
		return;
	blockid_t indirect_buf[BLOCK_SIZE/sizeof(blockid_t)];
	*buf_out=(char*)malloc(inode->size);
	int sum_block_num=(inode->size+BLOCK_SIZE-1)/BLOCK_SIZE;
	printf("im:read_file:block_num:%d\n",sum_block_num);
	fflush(stdout);
	//direct
	for(int i=0;i<(sum_block_num<=32?sum_block_num:32);i++){
		if(i==(sum_block_num)-1){
			char bufTemp[BLOCK_SIZE];
			bm->read_block(inode->blocks[i],bufTemp);
			memcpy(*buf_out+i*BLOCK_SIZE,bufTemp,inode->size-BLOCK_SIZE*i);
		}
		else{
			bm->read_block(inode->blocks[i],*buf_out+i*BLOCK_SIZE);
		}
		printf("im:read_file:block_content:%s\n",*buf_out);
		fflush(stdout);
	}

	//indirect
	if(sum_block_num>32){
		bm->read_block(inode->blocks[32],(char*)indirect_buf);
		for(int i=0;i<(sum_block_num-32);i++){
			printf("i:%d\n",i);
			if(i==sum_block_num-32-1){
				char bufTemp[BLOCK_SIZE];
				bm->read_block(indirect_buf[i],bufTemp);
				memcpy(*buf_out+(32+i)*BLOCK_SIZE,bufTemp,inode->size-BLOCK_SIZE*(32+i));
			}
			else{
				bm->read_block(indirect_buf[i],*buf_out+(32+i)*BLOCK_SIZE);
			}
		}
	}
	unsigned int now=(unsigned int)time(NULL);
	inode->atime=now;
	*size=inode->size;
	put_inode(inum,inode);
	free(inode);
	return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
	struct inode* inode=get_inode(inum);
	if(inode==NULL)
		return;
	int pre_num=(inode->size+BLOCK_SIZE-1)/BLOCK_SIZE;
	int new_num;
	if(size<=(int)MAXFILE*512){
		new_num=(size+BLOCK_SIZE-1)/BLOCK_SIZE;	
	}
	else{
		new_num=MAXFILE;
	}

	blockid_t indirect_buf[BLOCK_SIZE/sizeof(blockid_t)];
	//smaller
	if(pre_num>new_num){
		for(int i=new_num;i<(pre_num<=NDIRECT?pre_num:NDIRECT);i++){
			bm->free_block(inode->blocks[i]);
		}
		if(pre_num>NDIRECT){
			bm->read_block(inode->blocks[NDIRECT],(char*)indirect_buf);
			for(int i=(new_num<=NDIRECT?0:new_num-NDIRECT);i<pre_num-NDIRECT;i++){
				bm->free_block(indirect_buf[i]);
			}
			if(new_num<=NDIRECT){
				bm->free_block(inode->blocks[NDIRECT]);
			}
		}
	}
    //bigger
	if(pre_num<new_num){
		for(int i=pre_num;i<(new_num<=NDIRECT?new_num:NDIRECT);i++)
		{
			int block_num=bm->alloc_block();
			inode->blocks[i]=block_num;

		}
		if(new_num>NDIRECT){
			if(pre_num<=32){
				int block_num=bm->alloc_block();
				inode->blocks[NDIRECT]=block_num;
			}
			else{
				bm->read_block(inode->blocks[NDIRECT],(char*)indirect_buf);
			}
			for(int i=(pre_num<=NDIRECT?0:pre_num-NDIRECT);i<new_num-NDIRECT;i++){
				int block_num=bm->alloc_block();
				indirect_buf[i]=block_num;
				bm->write_block(inode->blocks[NDIRECT],(char*)indirect_buf);
			}	
		}
	}
	//direct
	for(int i=0;i<(new_num<32?new_num:32);i++){
		if(i==(new_num)-1){
			char bufTemp[BLOCK_SIZE];
			memset(bufTemp,0,BLOCK_SIZE);
			memcpy(bufTemp,buf+i*BLOCK_SIZE,size-BLOCK_SIZE*i);
			bm->write_block(inode->blocks[i],bufTemp);
		}
		else{
			bm->write_block(inode->blocks[i],buf+i*BLOCK_SIZE);
		}
	}
	//indirect
	if(new_num>32){
		bm->read_block(inode->blocks[32],(char*)indirect_buf);
		for(int i=0;i<(new_num-32);i++){
			if(i==(new_num-32)-1){
				char bufTemp[BLOCK_SIZE];
				memset(bufTemp,0,BLOCK_SIZE);
				memcpy(bufTemp,buf+(32+i)*BLOCK_SIZE,size-BLOCK_SIZE*(32+i));
				bm->write_block(indirect_buf[i],bufTemp);
			}
			else{
				bm->write_block(indirect_buf[i],buf+(32+i)*BLOCK_SIZE);
			}
		}
	}
	unsigned int now=(unsigned int)time(NULL);
	inode->atime=now;
	inode->mtime=now;
	inode->ctime=now;
	inode->size=size;
	put_inode(inum,inode);
	free(inode);
	return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
	struct inode* inode_get=get_inode(inum);
	if(inode_get==NULL)
	{
		return;
	}
	a.type=inode_get->type;
	a.size=inode_get->size;
	a.atime=inode_get->atime;
	a.ctime=inode_get->ctime;
	a.mtime=inode_get->mtime;
	free(inode_get);
	
	return;

	
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
	printf("im:enter remove_file,inum:%d\n",(int)inum);
	fflush(stdout);
	struct inode* inode=get_inode(inum);
	if(inode==NULL)
		return;
	free_inode(inum);
	int block_num=(inode->size+BLOCK_SIZE-1)/BLOCK_SIZE;
	int direct_num=(block_num<=32?block_num:32);
	printf("im:enter remove_file,block_num:%d\n",block_num);
	fflush(stdout);
	for(int i=0;i<direct_num;i++){
		bm->free_block(inode->blocks[i]);
	}
	if(block_num>32){
		blockid_t indirect_buf[BLOCK_SIZE/sizeof(blockid_t)];
		bm->read_block(inode->blocks[32],(char*)indirect_buf);
		for(int i=0;i<(block_num-32);i++){
			bm->free_block(indirect_buf[i]);
		}
		bm->free_block(inode->blocks[32]);
	}
	free(inode);
	return;
}




void
inode_manager::append_block(uint32_t inum, blockid_t &bid)
{
	printf("im:enter append_block,inum:%d\n",(int)inum);
	fflush(stdout);
	struct inode* inode = get_inode(inum);
	if(inode == NULL){
		return;
	}
	bid = bm->alloc_block();
	int pre_num = (inode->size+BLOCK_SIZE-1)/BLOCK_SIZE;
	printf("im:append_block:pre_num:%d\n", pre_num);
	fflush(stdout);
	if(pre_num < NDIRECT){
		printf("im:append_block:<NDIRECT\n");
		fflush(stdout);
		inode->blocks[pre_num] = bid;
	}
	else{	
		blockid_t indirect_buf[BLOCK_SIZE/sizeof(blockid_t)];
		if(pre_num == NDIRECT){
			int indirect_block_num = bm->alloc_block();
			inode->blocks[NDIRECT] = indirect_block_num;
			indirect_buf[0] = bid;
		}		
		else{
			bm->read_block(inode->blocks[NDIRECT],(char*)indirect_buf);
			indirect_buf[pre_num-NDIRECT]=bid;
		}
		bm->write_block(inode->blocks[NDIRECT], (char*)indirect_buf);
	}
	inode->size += BLOCK_SIZE;
	put_inode(inum,inode);
	free(inode);
	return;	
	
}	   


void
inode_manager::get_block_ids(uint32_t inum, std::list<blockid_t> &block_ids)
{
	
	printf("im:enter get_block_ids\n");
	fflush(stdout);
	struct inode* inode = get_inode(inum);
	if(inode == NULL){
		return;
	}
	int block_num = (inode->size+BLOCK_SIZE-1)/BLOCK_SIZE;
	for(int i = 0;i<(block_num <= NDIRECT?block_num:NDIRECT);i++){
		block_ids.push_back(inode->blocks[i]);
	}
	if(block_num > NDIRECT){
		blockid_t indirect_buf[BLOCK_SIZE/sizeof(blockid_t)];
		bm->read_block(inode->blocks[NDIRECT],(char*)indirect_buf);
		for(int i=0;i<block_num-NDIRECT;i++){
			block_ids.push_back(indirect_buf[i]);	
		}	
	}
	return;
	
}

void
inode_manager::read_block(blockid_t id, char buf[BLOCK_SIZE])
{
	
	printf("im:enter read_block\n");
	fflush(stdout);
	bm->read_block(id,buf);
	return;
}

void
inode_manager::write_block(blockid_t id, const char buf[BLOCK_SIZE])
{
	printf("im:enter write_block\n");
	fflush(stdout);
	bm->write_block(id,buf);
	return;
}

void
inode_manager::complete(uint32_t inum, uint32_t size)
{
	printf("im:enter complete\n");
	fflush(stdout);
	struct inode* inode=get_inode(inum);
	if( inode == NULL){
		return;
	}
	unsigned int now=(unsigned int)time(NULL);
	inode->atime=now;
	inode->mtime=now;
	inode->ctime=now;
	inode->size=size;
	put_inode(inum,inode);
	free(inode);
	return;
}

