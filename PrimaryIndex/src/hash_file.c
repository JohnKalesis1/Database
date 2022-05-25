#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "../include/bf.h"
#include "../include/hash_file.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

typedef struct hash_file_Desc  {
  char filename[20];
  int block_file_indexDesc;
} FILE_indexDesc;

typedef struct block_id {
  char filename[20];
  int block_num;
  int local_depth;
  int num_of_records;
} BLOCK_ID;

static int open_files_counter;
static FILE_indexDesc* openfiles[MAX_OPEN_FILES];
static BLOCK_ID** hash_table;
static int global_depth;


unsigned int reverseBits(unsigned int n)
{
    unsigned int rev = 0;
     
    // traversing bits of 'n' from the right
    int iter=0;
    while (iter<32) {
        // if current bit is '1'
        if (n & 1 == 1)  {
            rev=rev+(unsigned int)pow(2,31-iter);
        }
        // bitwise right shift
        // 'n' by 1
        iter++;
        n=n>>1;        
    }
    return rev;
}

int hash_bits(Record record, int depth)  {
  unsigned int id = reverseBits((unsigned int)record.id);
  id = id >> (32-depth);
  int hash_result = (int)(id & ((unsigned int)pow(2, depth+1)-(unsigned int)1));
  return hash_result;
}

HT_ErrorCode HT_Init() {
  hash_table=NULL;
  open_files_counter=0;
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {
  BF_Block *block;
  BF_Block_Init(&block);
  global_depth = depth;
  if (BF_CreateFile(filename)!=BF_OK) {
    return HT_ERROR;
  }
  int fd1;
  if (BF_OpenFile(filename, &fd1)!=BF_OK) {
    return HT_ERROR;
  }
  int var0=0;
  int var1=1;
  if (BF_AllocateBlock(fd1, block) != BF_OK) {//1st block, block_number=0 
    return HT_ERROR;
  }
  char* data;
  data=BF_Block_GetData(block);
  memcpy(data,(char*)&global_depth,sizeof(int));//data[0]=global_depth
  memcpy(data+sizeof(int),(char*)&var0,sizeof(int));//data[1]=number_of_blocks_with_hash_indexes
  BF_Block_SetDirty(block);
  BF_UnpinBlock(block);

  for(int i = 0; i < 2; i++){                           //local depth of starting blocks is 1 so hash table is initialised with two blocks
    if (BF_AllocateBlock(fd1, block) != BF_OK) {
      return HT_ERROR;
    } 
    char* data;
    data=BF_Block_GetData(block);
    memcpy(data,(char*)&var1,sizeof(int));//data[0]=local_depth which starts at one
    memcpy(data+sizeof(int),(char*)&var0,sizeof(int));//data[1]=number_of_records which starts at zero
    BF_Block_SetDirty(block);
    if(BF_UnpinBlock(block) != BF_OK){
      return HT_ERROR;
    }
  }
  /////////////////////////////////////////////////////
  
  hash_table = malloc((int)pow(2, depth)*sizeof(BLOCK_ID*));//create our hash table in main memory, which will be stored into as many blocks required when we call HT_CloseIndex
  for (int i = 0; i < (int)pow(2, depth); i++) {
  
    hash_table[i] = malloc(sizeof(BLOCK_ID));
    hash_table[i]->local_depth = 1;
    strcpy(hash_table[i]->filename, filename); 
    hash_table[i]->num_of_records = 0;
    
    if (i < ((int)pow(2, depth)/2)) {                   
      hash_table[i]->block_num = 1;
    }
    else {
      hash_table[i]->block_num = 2;
    } 

  }
  if (BF_CloseFile(fd1) != BF_OK) {
    return HT_ERROR;
  }
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  char success=0;
  for (int i = 0; i < MAX_OPEN_FILES; i++) {
    if ( openfiles[i]==NULL || strcmp(openfiles[i]->filename, fileName)==0) {
      openfiles[i]=malloc(sizeof(FILE_indexDesc*));
      open_files_counter++;
      strcpy(openfiles[i]->filename, fileName);
      *indexDesc = i;
      int fd1;
      if (BF_OpenFile(fileName, &fd1) != BF_OK) {
        return HT_ERROR;
      } 
      openfiles[i]->block_file_indexDesc=fd1;
      success=1;
      break;
    }
  }
  if (hash_table==NULL)  {//there is no hash table in memory, thus we must read from Hard Drive and create one from the blocks which contain hash indexes
    char* data;
    BF_Block* block;
    BF_Block_Init(&block);
    BF_GetBlock(*indexDesc,0,block);//read 1st block, it will guide us as to how many and where are the blocks containing hash indexes
    data=BF_Block_GetData(block);
    global_depth=*(int*)data;

    int size_of_array=(int)pow(2, global_depth);

    hash_table=malloc(size_of_array*sizeof(BLOCK_ID*));
    
    int num_of_hash_blocks=*(int*)(data+sizeof(int));

    int hash_counter=0;

    BF_Block* block2;
    BF_Block_Init(&block2);
    for (int j=0;j<num_of_hash_blocks;j++)  {
      int hash_block=*(int*)(data+2*sizeof(int)+j*sizeof(int));
      BF_GetBlock(*indexDesc,hash_block,block2);
      char* hash_array=BF_Block_GetData(block2);
      for (int k=0;k<size_of_array/num_of_hash_blocks;k++)  {//where k is the counter for each individual block, while hash_counter holds the current index which we will fill with the contents of the array at position k
        hash_table[hash_counter]=malloc(sizeof(BLOCK_ID));
        hash_table[hash_counter]->block_num=*(int*)(hash_array+sizeof(int)*k);//this will be done num_of_hash_blocks*(size_of_array/num_of_hash_blocks) so in the end we will have a hash table the size of size_of_array
        hash_counter++;
      }
      BF_UnpinBlock(block2);
    }
    BF_UnpinBlock(block);
    for (int i=0;i<size_of_array;i++)  {//so that we can access them easier
      char* data;
      BF_GetBlock(*indexDesc,hash_table[i]->block_num,block);
      data=BF_Block_GetData(block);
      strcpy(hash_table[i]->filename,fileName);
      hash_table[i]->local_depth=*(int*)(data);
      hash_table[i]->num_of_records=*(int*)(data+sizeof(int));
      BF_UnpinBlock(block);
    }
  }
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  if(openfiles[indexDesc] == NULL){
    return HT_ERROR;    
  } 
    int var;
    char* data;
    BF_Block* block;
    BF_Block_Init(&block);
    BF_GetBlock(indexDesc,0,block);//get 1st block so that we not only change its contents, but know how many blocks of hash indexes exist on Hard Drive, and thus how many more we should allocate in case the amount of blocks needed exceeds the one that the current number of blocks on the HD 
    data=BF_Block_GetData(block);

    memcpy(data,(char*)&global_depth,sizeof(int));//renew global depth
    int num_hash_blocks_HD=*(int*)(data+sizeof(int));//number of hash blocks on HD 
    int size_of_array=(int)pow(2, global_depth);

    int num_hash_blocks_MainMemory=(size_of_array*sizeof(int))/BF_BLOCK_SIZE;//since each position of the array will hold one integere when placed in memory, if we were to divide the array into sets of 512 bytes,we would get (size_of_array*(sizeof(int) bytes))/(512 bytes) as the number of blocks
    if (num_hash_blocks_MainMemory==0)  {//in case size of array is not enough to fill one block, the division would return 0, while 1 is the nuber we want
      num_hash_blocks_MainMemory=1;
    }    
    int hash_counter=0;
    BF_Block* block2;
    BF_Block_Init(&block2);
    char* hash_array;
    for (int j=0;j<num_hash_blocks_HD;j++)  {//first of all we need to replace the entries of the hash blocks that already exist on the Hard Drive.

      int hash_block=*(int*)(data+2*sizeof(int)+j*sizeof(int));
      BF_GetBlock(indexDesc,hash_block,block2);
      hash_array=BF_Block_GetData(block2);
     
      for (int k=0;k<size_of_array/num_hash_blocks_MainMemory;k++)  {
        memcpy(hash_array+k*sizeof(int),(char*)&hash_table[hash_counter]->block_num,sizeof(int));

        hash_counter++;
      }
      BF_Block_SetDirty(block2);
      BF_UnpinBlock(block2);
    }
    
    for (int j=num_hash_blocks_HD;j<num_hash_blocks_MainMemory;j++)  {//in case we have a bigger array than the one the current number of blocks can store, we create new blocks
      BF_AllocateBlock(indexDesc,block2);
      int block_num;
      BF_GetBlockCounter(indexDesc,&block_num);//newest created block is at get_block_counter()-1 position
      var=block_num-1;

      memcpy(data+2*sizeof(int)+j*sizeof(int),(char*)&var,sizeof(int));//remember to inform about a new hash block in the array of hash blocks in the 1st block

      hash_array=BF_Block_GetData(block2);

      for (int k=0;k<size_of_array/num_hash_blocks_MainMemory;k++)  {
        memcpy(hash_array+k*sizeof(int),(char*)&hash_table[hash_counter]->block_num,sizeof(int));

        hash_counter++;
      }
      BF_Block_SetDirty(block2);
      BF_UnpinBlock(block2);
    }
    memcpy(data+sizeof(int),(char*)&num_hash_blocks_MainMemory,sizeof(int));//renew the number of blocks which are now stored on HD
    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
  

  if (BF_CloseFile(indexDesc) != BF_OK) {
    return HT_ERROR;
  }
  hash_table=NULL;
  openfiles[indexDesc] = NULL;
  return HT_OK;
}






HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  
  if (openfiles[indexDesc] == NULL) {
    return HT_ERROR;    
  }
  int hash = hash_bits(record, global_depth);
  BF_Block *block;
  BF_Block_Init(&block);
  if (BF_GetBlock(indexDesc, hash_table[hash]->block_num, block) != BF_OK) {
    return HT_ERROR;
  }
  if (hash_table[hash]->num_of_records < 2*sizeof(int)) {                                   //if block has available space
    //insert entry to block
    int rec = hash_table[hash]->num_of_records;
    char* data;
    data = BF_Block_GetData(block);
    memcpy(data+2*sizeof(int) + (rec)*sizeof(Record), (char*)&record, sizeof(Record));    //records will start after the 8th bit of data
    int var=hash_table[hash]->num_of_records+1;
    memcpy(data+sizeof(int),(char*)&var,sizeof(int));
    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
    for (int i = 0; i < (int)pow(2, global_depth); i++){                                  //update records number in each cell of hash_table that has the same block id
      if (hash_table[i]->block_num == hash_table[hash]->block_num){
        hash_table[i]->num_of_records++;
      }
    }
  }
  else {//no space, overflow
    if (global_depth == hash_table[hash]->local_depth){

      //double the size of the hash table
      int old_global = global_depth;
      global_depth++;
      BLOCK_ID ** temp = malloc((int)pow(2, global_depth) *sizeof(BLOCK_ID*));
      
      for (int i = 0; i < (int)pow(2, global_depth); i++) {
        temp[i] = malloc(sizeof(BLOCK_ID));
      }
      printf("OVERFLOW from %d to %d\n", old_global,global_depth);
      int i = 0;
      while (i < (int)pow(2, old_global)) {
        for (int j = 2*i; j < (2*i + (int) pow(2, global_depth - hash_table[i]->local_depth)); j++) {               //double the cells that have the same block id
          temp[j]->block_num = hash_table[i]->block_num;
          strcpy(temp[j]->filename, hash_table[i]->filename);
          temp[j]->local_depth = hash_table[i]->local_depth;
          temp[j]->num_of_records = hash_table[i]->num_of_records;
        }
        i += (int) pow(2, old_global - hash_table[i]->local_depth);
      }
      
      //free old hash table
      for (int i = 0; i < (int)pow(2, old_global); i++) {
        free(hash_table[i]);
      }
      free(hash_table);

      hash_table = temp;                                                      //update hash_table to new
      HT_InsertEntry(indexDesc, record);                                      //call insert entry to either insert the record or go to case local_depth<global_depth
    }
    else { //local < global
      char* data;
      data = BF_Block_GetData(block);
      int size = hash_table[hash]->num_of_records;
      Record* array[size];                                                       //store block records into an array
      for (int i = 0; i < size; i++) {
        array[i] = (Record*)(data+2*sizeof(int) + i * sizeof(Record));
      }

      if (BF_UnpinBlock(block) != BF_OK) {
        return HT_ERROR;
      }
      
      BF_Block *block2;
      BF_Block_Init(&block2);
      if (BF_AllocateBlock(indexDesc, block2) != BF_OK) {                           //allocate the new block
        return HT_ERROR;
      } 
      if (BF_UnpinBlock(block2) != BF_OK){
        return HT_ERROR;
      }
      
      int start = 0;
      int loops;
      int success = 0;
      int locald = hash_table[hash]->local_depth;
      for (int i = 0; i < (int)pow(2, global_depth); i++){                                  
        if (hash_table[i]->block_num == hash_table[hash]->block_num) {                  //find the first cell that points the same block id with hash cell
          success = 1;
          loops = (int)pow(2, (global_depth - hash_table[i]->local_depth));
          start = i;
          for (int j = i; j < i + loops; j++) {
            if (j >= (int)(i+ (loops/2))) {  
              int blocks_num;
              if (BF_GetBlockCounter(indexDesc, &blocks_num) != BF_OK) {
                return HT_ERROR;
              }
              hash_table[j]->block_num = blocks_num - 1;
              hash_table[j]->num_of_records = 0;
              hash_table[j]->local_depth = locald + 1;
              if (j==(int)(i+(loops/2)))  {
                BF_GetBlock(indexDesc,hash_table[j]->block_num,block);
                data=BF_Block_GetData(block);
                int var=0;
                memcpy(data+sizeof(int),(char*)&var,sizeof(int));                     //number of records
                var=hash_table[j]->local_depth;
                memcpy(data,(char*)&var,sizeof(int));                                 //local_depth
                BF_Block_SetDirty(block);
                BF_UnpinBlock(block);
              }
            }
            else {
              hash_table[j]->local_depth = locald + 1;
              hash_table[j]->num_of_records = 0;
              if (j==i)  {
                BF_GetBlock(indexDesc,hash_table[j]->block_num,block);
                data=BF_Block_GetData(block);
                int var=0;
                memcpy(data+sizeof(int),(char*)&var,sizeof(int));
                var=hash_table[j]->local_depth;
                memcpy(data,(char*)&var,sizeof(int));
                BF_Block_SetDirty(block);
                BF_UnpinBlock(block);
              }
            }
          }
        }
        if (success == 1) {
          break;
        }
      }
      for (int i = 0; i < size; i++) {
        HT_InsertEntry(indexDesc, *array[i]);                                 //re insert hash bloc records
      }
      HT_InsertEntry(indexDesc, record);                                      //and the new record
    }

  }
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  if (openfiles[indexDesc] == NULL) {
    return HT_ERROR;    
  }
  if (id==NULL)  {  //print all entries
    BF_Block* block;
    BF_Block_Init(&block);
    int blocks_num;
    if (BF_GetBlockCounter(indexDesc, &blocks_num) != BF_OK) {
      return HT_ERROR;
    }
    BF_PrintError(BF_GetBlock(indexDesc,0,block));
    char* data=BF_Block_GetData(block);

    int num_exclude_blocks=*(int*)(data+sizeof(int));

    int exclude_array[num_exclude_blocks];
    for (int i=0;i<num_exclude_blocks;i++)  {

      exclude_array[i]=*(int*)(data+2*sizeof(int)+i*sizeof(int));
    }

    for (int i=1;i<blocks_num;i++)  {
    char stop=0;
      for (int j=0;j<num_exclude_blocks;j++)  {
        if (exclude_array[j]==i)  {
          stop=1;
        }
      }
      if (stop!=1) {
        if (BF_GetBlock(indexDesc,i,block) != BF_OK) {
          return HT_ERROR;
        }
        char* data;
        data = BF_Block_GetData(block);
        printf("Block %d: has local %d\n",i,*(int*)(data));
        for (int k = 0; k < *(int*)(data+sizeof(int)); k++) {
          Record* record = (Record*)(data+2*sizeof(int) + k * sizeof(Record));
          printf("Id: %d, Name: %s, Surname: %s, City: %s\n", record->id, record->name, record->surname, record->city);
        }
        BF_UnpinBlock(block);
      } 
    }
  }
  else {
    Record rec;
    rec.id=*id;
    int hash=hash_bits(rec,global_depth);
    BF_Block* block;
    BF_Block_Init(&block);
    BF_GetBlock(indexDesc,hash_table[hash]->block_num,block);
    char *data;
    data=BF_Block_GetData(block);
    for (int i=0;i<*(int*)(data+sizeof(int));i++)  {
      Record* record=(Record*)(data+2*sizeof(int)+i*sizeof(Record));
      if (record->id==*id)  {
        printf("Id: %d, Name: %s, Surname: %s, City: %s\n", record->id, record->name, record->surname, record->city);
      }
    }
  }
  return HT_OK;
}

HT_ErrorCode HT_HashStatistics(char* filename)  {
  int fd;
  int opened_file=0;
  if (hash_table==NULL)  {
    opened_file=1;
    HT_OpenIndex(filename,&fd);
  }
  int indexDesc;
  for (int i=0;i<MAX_OPEN_FILES;i++) {
    if (openfiles[i]!=NULL && strcmp(openfiles[i]->filename,filename)==0)  {
      indexDesc=openfiles[i]->block_file_indexDesc;
    }
  }
  int blocks_num;
  BF_GetBlockCounter(indexDesc,&blocks_num);
  printf("Number of blocks in file %d:\n",blocks_num);
  int min=999;
  int max=-999;
  float average=0.0;
  BF_Block* block;
  BF_Block_Init(&block);
  BF_GetBlock(indexDesc,0,block);
  char* data=BF_Block_GetData(block);
  int num_exclude_blocks=*(int*)(data+sizeof(int));
  int exclude_array[num_exclude_blocks];
  for (int i=0;i<num_exclude_blocks;i++)  {
    exclude_array[i]=*(int*)(data+2*sizeof(int)+i*sizeof(int));
  }
  for (int i=1;i<blocks_num;i++)  {
    char stop=0;
    for (int j=0;j<num_exclude_blocks;j++)  {
      if (exclude_array[j]==i)  {
        stop=1;
      }
    }
    if (stop!=1) {
      if (BF_GetBlock(indexDesc,i,block) != BF_OK) {
        return HT_ERROR;
      }
      char* data;
      data = BF_Block_GetData(block);
      int num_of_records=*(int*)(data+sizeof(int));
      if (num_of_records<min)  {
        min=num_of_records;
      }
      if (num_of_records>max)  {
        max=num_of_records;
      }
      average+=(float)num_of_records;
      BF_UnpinBlock(block);
    } 
  }
  printf("Minimun number of records in bucket:%d\nMaximum number of records in bucket:%d\nAverage number of records in buckets:%f\n",min,max,average/blocks_num);
  if (opened_file)  {
    HT_CloseFile(fd);
  }
  return HT_OK;

}