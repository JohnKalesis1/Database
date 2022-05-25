#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include "bf.h"
#include "sht_file.h"

#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}


static int open_files_counter;
extern FILE_indexDesc* openfiles[MAX_OPEN_FILES];
static BLOCK_ID*** hash_table;
static int* global_depth;
static int hash_in_memory = -1;


int hash_bits(Record rec, int global_depth);

int hash_func(char* attribute, int attrLen, int depth) {
  int sum = 0;
  for (int i = 0; i < 4; i++) {                                        //get the first 4 bytes of string
    sum = sum + (int)attribute[i];
  }

  Record rec;
  rec.id = sum;

  return hash_bits(rec, depth);
}

HT_ErrorCode SHT_Init() {
  hash_table = NULL;
  open_files_counter = 0;
  
  return HT_OK;
}



HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth,char *fileName ) {
  fflush(NULL);
  BF_Block *block;
  
  BF_Block_Init(&block);
  if (BF_CreateFile(sfileName) != BF_OK) {
    return HT_ERROR;
  }

  int fd1;
  if (BF_OpenFile(sfileName, &fd1) != BF_OK) {
    return HT_ERROR;
  }

  int var0 = 0;
  int var1 = 1;
  if (BF_AllocateBlock(fd1, block) != BF_OK) {                        //1st block, block_number = 0 
    return HT_ERROR;
  }

  char* data;
  data = BF_Block_GetData(block);
  memcpy(data, (char*)&attrLength, sizeof(int));
  memcpy(data + sizeof(int), attrName, attrLength);
  
  char filename[20];
  memcpy(filename, fileName, 20);
  memcpy(data + sizeof(int) + attrLength, filename, 20);
  memcpy(data + sizeof(int) + 20 + attrLength, (char*)&depth, sizeof(int));
  memcpy(data + sizeof(int) + 20 + attrLength + sizeof(int), (char*)&var0, sizeof(int));
  
  BF_Block_SetDirty(block);
  BF_UnpinBlock(block);

  for (int i = 0; i < 2; i++) {                                      //no matter global depth, local depth always 
    if (BF_AllocateBlock(fd1, block) != BF_OK) {                     //starts at 1 so 2 blocks to be created at start
      return HT_ERROR;
    } 
    char* data;
    data = BF_Block_GetData(block);

    memcpy(data, (char*)&var1, sizeof(int));                        //data[0] = local_depth which starts at one
    memcpy(data + sizeof(int), (char*)&var0, sizeof(int));          //data[1] = number_of_records which starts at zero
    
    BF_Block_SetDirty(block);
    if (BF_UnpinBlock(block) != BF_OK) {
      return HT_ERROR;
    }
  }

  if (BF_CloseFile(fd1) != BF_OK) {
    return HT_ERROR;
  }
  return HT_OK;
}

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc) {
  char already_exists=0;
  char success = 0; 
  int twin;
  for (int i = 0; i < MAX_OPEN_FILES; i++) {                        //add file to openfiles array
    if (openfiles[i] != NULL && strcmp(openfiles[i]->filename,sfileName)==0)  {
      already_exists=1;
      twin=i;
    }
  }
  for (int i = 0; i < MAX_OPEN_FILES; i++) {                        //add file to openfiles array

    if (openfiles[i] == NULL) {
      if (already_exists)  {
        openfiles[i] = malloc(sizeof(FILE_indexDesc));
        memcpy(openfiles[i]->filename, sfileName, 20);
        open_files_counter++;
        openfiles[i]->block_file_indexDesc =openfiles[twin]->block_file_indexDesc;
        openfiles[i]->hash_table_pointer=openfiles[twin]->hash_table_pointer;
        openfiles[i]->global_depth=openfiles[twin]->global_depth;
        return HT_OK;  
      }
      else {

        openfiles[i] = malloc(sizeof(FILE_indexDesc));

        memcpy(openfiles[i]->filename, sfileName, 20);
        open_files_counter++;
        int fd1;
        if (BF_OpenFile(sfileName, &fd1) != BF_OK) {
          return HT_ERROR;
        } 

        openfiles[i]->block_file_indexDesc = fd1;
        *indexDesc = i;
      
        success = 1;
        break;
      }
    }
    
  }
  //if (hash_table==NULL)  {//there is no hash table in memory, thus we must read from Hard Drive and create one from the blocks which contain hash indexes
  BLOCK_ID*** shash_table = malloc(sizeof(BLOCK_ID**));
  char* data;
  
  BF_Block* block;
  BF_Block_Init(&block);
  BF_GetBlock(openfiles[*indexDesc]->block_file_indexDesc, 0, block);         //read 1st block, it will guide us as to how many and where are the blocks containing hash indexes
  
  data = BF_Block_GetData(block);
  int attrLen = *(int*)(data);
  int *sglobal_depth = malloc(sizeof(int));
  *sglobal_depth=*(int*)(data + sizeof(int) + attrLen + 20);

  int size_of_array = (int)pow(2, (double)(*sglobal_depth));

  *shash_table = malloc(size_of_array * sizeof(BLOCK_ID*));
  
  int num_of_hash_blocks = *(int*)(data + attrLen + sizeof(int) + 20 + sizeof(int));
  int hash_counter = 0;
  
  BF_Block* block2;
  BF_Block_Init(&block2);

  for (int j = 0; j < num_of_hash_blocks; j++) {
    int hash_block = *(int*)(data + sizeof(int) + attrLen + 20 + 2*sizeof(int) + j*sizeof(int));
    BF_GetBlock(openfiles[*indexDesc]->block_file_indexDesc, hash_block, block2);

    char* hash_array = BF_Block_GetData(block2);
  
    for (int k = 0; k < size_of_array/num_of_hash_blocks; k++) {                    //where k is the counter for each individual block, while hash_counter holds the current index which we will fill with the contents of the array at position k
      (*shash_table)[hash_counter] = malloc(sizeof(BLOCK_ID));
      (*shash_table)[hash_counter]->block_num = *(int*)(hash_array + sizeof(int)*k);//this will be done num_of_hash_blocks*(size_of_array/num_of_hash_blocks) so in the end we will have a hash table the size of size_of_array
      hash_counter++;
    }

    BF_UnpinBlock(block2);
  }

  if (num_of_hash_blocks == 0) {
    for (int i = 0; i < size_of_array; i++) {
      (*shash_table)[i] = malloc(sizeof(BLOCK_ID));
      (*shash_table)[i]->local_depth = 1;
      memcpy((*shash_table)[i]->filename, sfileName, 20); 
      (*shash_table)[i]->num_of_records = 0;
  
      if (i < (size_of_array/2)) {
        (*shash_table)[i]->block_num = 1;
      }
      else {
        (*shash_table)[i]->block_num = 2;
      } 
    }
  }

  BF_UnpinBlock(block);

  for (int i = 0; i < size_of_array; i++) {                          //so that we can access them easier
    char* data;
    BF_GetBlock(openfiles[*indexDesc]->block_file_indexDesc, (*shash_table)[i]->block_num, block);
    data = BF_Block_GetData(block);
    
    memcpy((*shash_table)[i]->filename, sfileName, 20);
    (*shash_table)[i]->local_depth = *(int*)(data);
    (*shash_table)[i]->num_of_records = *(int*)(data + sizeof(int));
    
    BF_UnpinBlock(block);
  }

  openfiles[*indexDesc]->hash_table_pointer = shash_table;

  openfiles[*indexDesc]->global_depth = sglobal_depth;
  
  return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
  if (openfiles[indexDesc] == NULL) {
    return HT_ERROR;
  }

  if (hash_in_memory != indexDesc) {

    hash_table = openfiles[indexDesc]->hash_table_pointer;
    global_depth = openfiles[indexDesc]->global_depth;
    hash_in_memory = indexDesc;
  }

  int var;
  char* data;
  
  BF_Block* block;
  BF_Block_Init(&block);
  BF_GetBlock(openfiles[indexDesc]->block_file_indexDesc, 0, block);                    //get 1st block so that we not only change its contents, but know how many blocks of hash indexes exist on Hard Drive, and thus how many more we should allocate in case the amount of blocks needed exceeds the one that the current number of blocks on the HD 
  
  data = BF_Block_GetData(block);
  int attrLen = *(int*)data;
  memcpy(data + sizeof(int) + attrLen + 20, (char*)&*global_depth, sizeof(int));         //renew global depth
  int num_hash_blocks_HD = *(int*)(data + sizeof(int) + attrLen + 20 + sizeof(int));    //number of hash blocks on HD 
  int size_of_array = (int)pow(2, (double)(*global_depth));

  int num_hash_blocks_MainMemory = (size_of_array*sizeof(int))/BF_BLOCK_SIZE;           //since each position of the array will hold one integere when placed in memory, if we were to divide the array into sets of 512 bytes,we would get (size_of_array*(sizeof(int) bytes))/(512 bytes) as the number of blocks
  if (num_hash_blocks_MainMemory == 0) {                                                //in case size of array is not enough to fill one block, the division would return 0, while 1 is the nuber we want
    num_hash_blocks_MainMemory = 1;
  }    

  int hash_counter = 0;
  BF_Block* block2;
  BF_Block_Init(&block2);
  
  char* hash_array;
  for (int j = 0; j < num_hash_blocks_HD; j++) {                                        //first of all we need to replace the entries of the hash blocks that already exist on the Hard Drive.

    int hash_block = *(int*)(data + sizeof(int) + attrLen + 20 + 2*sizeof(int) + j*sizeof(int));
    BF_GetBlock(openfiles[indexDesc]->block_file_indexDesc, hash_block, block2);
    hash_array = BF_Block_GetData(block2);
    
    for (int k = 0; k < size_of_array/num_hash_blocks_MainMemory; k++) {      
      memcpy(hash_array + k*sizeof(int), (char*)&(*hash_table)[hash_counter]->block_num, sizeof(int));
      hash_counter++;
    }

    BF_Block_SetDirty(block2);
    BF_UnpinBlock(block2);
  }
  
  for (int j = num_hash_blocks_HD; j < num_hash_blocks_MainMemory; j++) {                               //in case we have a bigger array than the one the current number of blocks can store, we create new blocks
    BF_AllocateBlock(openfiles[indexDesc]->block_file_indexDesc, block2);
    int block_num;
    BF_GetBlockCounter(openfiles[indexDesc]->block_file_indexDesc, &block_num);                         //newest created block is at getblock_counter()-1 position
    var = block_num - 1;

    memcpy(data + sizeof(int) + attrLen + 20 + 2*sizeof(int) + j*sizeof(int), (char*)&var, sizeof(int));//remember to inform about a new hash block in the array of hash blocks in the 1st block

    hash_array = BF_Block_GetData(block2);

    for (int k = 0; k < size_of_array/num_hash_blocks_MainMemory; k++) {
      memcpy(hash_array + k*sizeof(int), (char*)&(*hash_table)[hash_counter]->block_num, sizeof(int));
      hash_counter++;
    }

    BF_Block_SetDirty(block2);
    BF_UnpinBlock(block2);
  }

  memcpy(data + sizeof(int) + attrLen + 20 + sizeof(int), (char*)&num_hash_blocks_MainMemory, sizeof(int));//renew the number of blocks which are now stored on HD
  BF_Block_SetDirty(block);
  BF_UnpinBlock(block);

  if (BF_CloseFile(openfiles[indexDesc]->block_file_indexDesc) != BF_OK) {
    return HT_ERROR;
  }
  openfiles[indexDesc] = NULL;
  open_files_counter--;

  return HT_OK;
}


HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc, SecondaryRecord record) {

  if (openfiles[indexDesc] == NULL) {
    return HT_ERROR;
  } 

  if (hash_in_memory != indexDesc) {                                   //if file hash table is not in current memory                                    
    
    hash_table = openfiles[indexDesc]->hash_table_pointer;
    global_depth = openfiles[indexDesc]->global_depth;
    hash_in_memory = indexDesc;
  }

  BF_Block *block;
  BF_Block_Init(&block);
  BF_GetBlock(openfiles[indexDesc]->block_file_indexDesc, 0, block);
  char* dt = BF_Block_GetData(block);
  
  int attrLen = *(int*)dt;
  BF_UnpinBlock(block);

  int hash = hash_func(record.index_key, 20, *global_depth);

  if (BF_GetBlock(openfiles[indexDesc]->block_file_indexDesc, (*hash_table)[hash]->block_num, block) != BF_OK) {
    return HT_ERROR;
  }

  if ((*hash_table)[hash]->num_of_records < 21) {
    //insert entry to block
    int rec = (*hash_table)[hash]->num_of_records;
    char* data;
    data = BF_Block_GetData(block);
    memcpy(data + 2*sizeof(int) + (rec)*sizeof(SecondaryRecord), (char*)&record, sizeof(SecondaryRecord));
    
    int var = (*hash_table)[hash]->num_of_records + 1;
    memcpy(data + sizeof(int), (char*)&var, sizeof(int));
    
    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
    
    for (int i = 0; i < (int)pow(2, *global_depth); i++) {
      if ((*hash_table)[i]->block_num == (*hash_table)[hash]->block_num) {
        (*hash_table)[i]->num_of_records++;
      }
    }
  }

  else {                                                                                      //no space, overflow
    if (*global_depth == (*hash_table)[hash]->local_depth) {

      //double the size of the hash table
      int old_global = *global_depth;
      *global_depth=(*global_depth)+1;
      BLOCK_ID ** temp = malloc((int)pow(2, *global_depth) *sizeof(BLOCK_ID*));
      
      for (int i = 0; i < (int)pow(2, *global_depth); i++) {
        temp[i] = malloc(sizeof(BLOCK_ID));
      }

      printf("OVERFLOW from %d to %d.\n", old_global, *global_depth);
      
      int i = 0;
      while (i < (int)pow(2, old_global)) {

        for (int j = 2*i; j < (2*i + (int) pow(2, *global_depth - (*hash_table)[i]->local_depth)); j++) {
          temp[j]->block_num = (*hash_table)[i]->block_num;
          strcpy(temp[j]->filename, (*hash_table)[i]->filename);
          temp[j]->local_depth = (*hash_table)[i]->local_depth;
          temp[j]->num_of_records = (*hash_table)[i]->num_of_records;
        }
        
        i += (int) pow(2, old_global - (*hash_table)[i]->local_depth);
      }
      
      //free old hash table
      for (int i = 0; i < (int)pow(2, old_global); i++) {
        free((*hash_table)[i]);
      }

      free((*hash_table));
      *hash_table = temp;
      SHT_SecondaryInsertEntry(indexDesc, record);
    
    }
    else {
      char* data;
      data = BF_Block_GetData(block);
      int size = (*hash_table)[hash]->num_of_records;
      SecondaryRecord* array[size];
      for (int i = 0; i < size; i++) {
        array[i] = (SecondaryRecord*)(data + 2*sizeof(int) + i * sizeof(SecondaryRecord));
      }

      if (BF_UnpinBlock(block) != BF_OK) {
        return HT_ERROR;
      }
      
      BF_Block *block2;
      BF_Block_Init(&block2);
      
      if (BF_AllocateBlock(openfiles[indexDesc]->block_file_indexDesc, block2) != BF_OK) {
        return HT_ERROR;
      } 

      if (BF_UnpinBlock(block2) != BF_OK){
        return HT_ERROR;
      }
      
      int start = 0;
      int loops;
      int success = 0;
      int locald = (*hash_table)[hash]->local_depth;
      
      for (int i = 0; i < (int)pow(2, *global_depth); i++) {
        if ((*hash_table)[i]->block_num == (*hash_table)[hash]->block_num) {
          
          success = 1;
          loops = (int)pow(2, (*global_depth - (*hash_table)[i]->local_depth));
          start = i;

          for (int j = i; j < i + loops; j++) {
            if (j >= (int)(i + (loops/2))) {

              int blocks_num;
              if (BF_GetBlockCounter(openfiles[indexDesc]->block_file_indexDesc, &blocks_num) != BF_OK) {
                return HT_ERROR;
              }
              
              (*hash_table)[j]->block_num = blocks_num - 1;
              (*hash_table)[j]->num_of_records = 0;
              (*hash_table)[j]->local_depth = locald + 1;
              
              if (j == (int)(i + (loops/2))) {
                BF_GetBlock(openfiles[indexDesc]->block_file_indexDesc, (*hash_table)[j]->block_num, block);
                data = BF_Block_GetData(block);
                
                int var = 0;
                memcpy(data + sizeof(int), (char*)&var, sizeof(int));
                var = (*hash_table)[j]->local_depth;
                memcpy(data, (char*)&var, sizeof(int));
                
                BF_Block_SetDirty(block);
                BF_UnpinBlock(block);
              }
            }

            else {
              (*hash_table)[j]->local_depth = locald + 1;
              (*hash_table)[j]->num_of_records = 0;
              
              if (j == i) {
                BF_GetBlock(openfiles[indexDesc]->block_file_indexDesc, (*hash_table)[j]->block_num, block);
                data = BF_Block_GetData(block);
                
                int var = 0;
                memcpy(data + sizeof(int), (char*)&var, sizeof(int));
                var = (*hash_table)[j]->local_depth;
                memcpy(data, (char*)&var, sizeof(int));
                
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
        SHT_SecondaryInsertEntry(indexDesc, *array[i]);
      }
      SHT_SecondaryInsertEntry(indexDesc, record);
    }

  }
  
  return HT_OK;
}



HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray ) {
  
  if (openfiles[indexDesc] == NULL) {
    return HT_ERROR;
  } 
  if (updateArray[0].size==-1)  {
    return HT_OK;
  }

  if (hash_in_memory != indexDesc) {                                                 //if file hash table is not in current memory
    hash_table = openfiles[indexDesc]->hash_table_pointer;
    global_depth = openfiles[indexDesc]->global_depth;
    hash_in_memory = indexDesc;
  }
  char* data;

  BF_Block* block;
  BF_Block_Init(&block);
  BF_GetBlock(openfiles[indexDesc]->block_file_indexDesc, 0, block);
  
  data = BF_Block_GetData(block);
  int attrLen = *(int*)data;
  char attrKey[attrLen];
  int hash;
  
  memcpy(attrKey, data + sizeof(int), attrLen);
  BF_UnpinBlock(block);

  for (int i = 0; i < updateArray[0].size; i++) {
    if (updateArray[i].oldTupleId == updateArray[i].newTupleId) {
      continue;
    }

    if (strcmp(attrKey, "city") == 0) {
      hash = hash_func(updateArray[i].city, 20, *global_depth);
    }
    else { 
      hash = hash_func(updateArray[i].surname, 20, *global_depth);
    }

    BF_GetBlock(openfiles[indexDesc]->block_file_indexDesc, (*hash_table)[hash]->block_num, block);
    data = BF_Block_GetData(block);
    
    for (int k = 0; k < (*hash_table)[hash]->num_of_records; k++) {

      SecondaryRecord* rec = (SecondaryRecord*)(data + 2*sizeof(int) + k*sizeof(SecondaryRecord));

      if (updateArray[i].oldTupleId == rec->tupleId && strcmp(updateArray[i].city, rec->index_key)==0 && strcmp(attrKey, "city") == 0) {
        rec->tupleId = updateArray[i].newTupleId;
        memcpy(data + 2*sizeof(int) + k*sizeof(SecondaryRecord), (char*)rec, sizeof(SecondaryRecord));
        break;
      }

      else if (updateArray[i].oldTupleId == rec->tupleId && strcmp(updateArray[i].surname, rec->index_key)==0 && strcmp(attrKey, "surname") == 0) {
        rec->tupleId = updateArray[i].newTupleId;
        memcpy(data + 2*sizeof(int) + k*sizeof(SecondaryRecord), (char*)rec, sizeof(SecondaryRecord));
        BF_Block_SetDirty(block);
        break;
      }
    } 

    BF_UnpinBlock(block);
  }

  return HT_OK;
}




HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index_key ) {
  
  if (openfiles[sindexDesc] == NULL) {
    return HT_ERROR;
  } 

  if (hash_in_memory != sindexDesc) {                                                        //if file hash table is not in current memory
    hash_table = openfiles[sindexDesc]->hash_table_pointer;
    global_depth = openfiles[sindexDesc]->global_depth;
    hash_in_memory = sindexDesc;
  }

  BF_Block* block;
  BF_Block_Init(&block);
  BF_GetBlock(openfiles[sindexDesc]->block_file_indexDesc, 0, block);                       //0 block stores primary hash table filename
  
  char* data = BF_Block_GetData(block);
  int attrLen = *(int*)data;
  
  char filename[20];
  int primary_Index;
  memcpy(filename, data + sizeof(int) + attrLen, 20);

  for (int i = 0; i < MAX_OPEN_FILES; i++) {
    
    if (openfiles[i] != NULL) {
      
      if (strcmp(openfiles[i]->filename, filename) == 0) {
        primary_Index = i;
        break;
      }
    }
  }

  BF_UnpinBlock(block);
  
  char key[20];
  memcpy(key, index_key, 20);
  int hash = hash_func(key, 20, *global_depth);
  BF_GetBlock(openfiles[sindexDesc]->block_file_indexDesc, (*hash_table)[hash]->block_num, block);
  data = BF_Block_GetData(block);

  BF_Block* block2;
  BF_Block_Init(&block2);

  for (int i = 0; i < (*hash_table)[hash]->num_of_records; i++) {
    
    SecondaryRecord* rec = (SecondaryRecord*)(data + 2*sizeof(int) + i*sizeof(SecondaryRecord));
    
    if (strcmp(index_key, rec->index_key) != 0) {                                                
      continue;
    }
     
    int block_id = rec->tupleId/8 - 1;
    int pos_index = rec->tupleId%8;

    BF_GetBlock(openfiles[primary_Index]->block_file_indexDesc, block_id, block2);             //get primary hash table block with record
    char* data2 = BF_Block_GetData(block2);
    Record* p_record = (Record*)(data2 + 2*sizeof(int) + pos_index*sizeof(Record));
    
    printf("Id: %d | Name: %s | Surname: %s | City: %s \n", p_record->id, p_record->name, p_record->surname, p_record->city);
    
    BF_UnpinBlock(block2);
  }

  BF_UnpinBlock(block);
  
  return HT_OK;
}


HT_ErrorCode SHT_HashStatistics(char *filename ) {
  
  int fd;
  int opened_file = 0;
  
  int indexDesc = -1;
  
  for (int i = 0; i < MAX_OPEN_FILES; i++) {
    if (openfiles[i] != NULL && strcmp(openfiles[i]->filename, filename) == 0) {
      indexDesc = openfiles[i]->block_file_indexDesc;
    }
  }

  if (indexDesc == -1) {
    opened_file = 1;
    SHT_OpenSecondaryIndex(filename, &indexDesc);
  }

  if (hash_in_memory != indexDesc)  {
    hash_table = openfiles[indexDesc]->hash_table_pointer;
    global_depth = openfiles[indexDesc]->global_depth;
    hash_in_memory = indexDesc;
  }

  int blocks_num;
  BF_GetBlockCounter(indexDesc, &blocks_num);
  printf("Number of blocks in file %d:\n", blocks_num);
  
  int min = 999;
  int max = -999;
  float average = 0.0;
  
  BF_Block* block;
  BF_Block_Init(&block);
  BF_GetBlock(indexDesc, 0, block);
  
  char* data = BF_Block_GetData(block);
  int attrLen = *(int*)data;
  int num_exclude_blocks = *(int*)(data + attrLen + 20 + sizeof(int));
  int exclude_array[num_exclude_blocks];
  
  for (int i = 0; i < num_exclude_blocks; i++) {
    exclude_array[i] = *(int*)(data + attrLen + 20 + 2*sizeof(int) + i*sizeof(int));
  }

  for (int i = 1; i < blocks_num; i++) {
    char stop = 0;
    
    for (int j = 0; j < num_exclude_blocks; j++) {
      if (exclude_array[j] == i)  {
        stop = 1;
      }
    }

    if (stop != 1) {
      if (BF_GetBlock(indexDesc, i, block) != BF_OK) {
        return HT_ERROR;
      }

      char* data;
      data = BF_Block_GetData(block);
      
      int num_of_records = *(int*)(data + sizeof(int));
      if (num_of_records < min) {
        min = num_of_records;
      }

      if (num_of_records > max) {
        max = num_of_records;
      }

      average += (float)num_of_records;
      
      BF_UnpinBlock(block);
    } 
  }

  printf("Minimun number of records in bucket: %d\nMaximum number of records in bucket: %d\nAverage number of records in buckets: %f\n", min, max, average/blocks_num);
  
  if (opened_file) {
    SHT_CloseSecondaryIndex(fd);
  }
  
  return HT_OK;
}



//A helper function for inner join. Returns a record array with all records that has key = index_key 
void rec_arr(Record** recarr, int sindexDesc, char *index_key, int *count) {

  if (hash_in_memory != sindexDesc) {                                           //if file hash table is not in current memory
    hash_table = openfiles[sindexDesc]->hash_table_pointer;
    global_depth = openfiles[sindexDesc]->global_depth;
    hash_in_memory = sindexDesc;
  }

  BF_Block* block;
  BF_Block_Init(&block);
  BF_GetBlock(openfiles[sindexDesc]->block_file_indexDesc, 0, block);

  char* data = BF_Block_GetData(block);
  int attrLen = *(int*)data;
  
  char filename[20];
  int primary_Index;
  memcpy(filename, data + sizeof(int) + attrLen, 20);                           //get primary hash table filename

  for (int i = 0; i < MAX_OPEN_FILES; i++) {
    
    if (openfiles[i] != NULL) {                                                 //add primary hash table filename to openfile array
      
      if (strcmp(openfiles[i]->filename, filename) == 0) {
        primary_Index = i;
        break;
      }
    }
  }

  BF_UnpinBlock(block);
  
  char key[20];
  memcpy(key, index_key, 20);
  int hash = hash_func(key, 20, *global_depth);
  BF_GetBlock(openfiles[sindexDesc]->block_file_indexDesc, (*hash_table)[hash]->block_num, block);//get the block which has all records with key = index_key
  data = BF_Block_GetData(block);

  BF_Block* block2;
  BF_Block_Init(&block2);

  for (int i = 0; i < 21; i++) {                                                               //max records with same key in a block is 21
    recarr[i] = malloc(sizeof(Record));
  }

  for (int i = 0; i < (*hash_table)[hash]->num_of_records; i++) {

    SecondaryRecord* rec = (SecondaryRecord*)(data + 2*sizeof(int) + i*sizeof(SecondaryRecord));

    if (strcmp(index_key, rec->index_key) != 0) { 
      continue;
    }

    int block_id = rec->tupleId/8 - 1;
    int pos_index = rec->tupleId%8;
    BF_GetBlock(openfiles[primary_Index]->block_file_indexDesc, block_id, block2);              //get block from primary hash table
    char* data2 = BF_Block_GetData(block2);
    Record* p_record = (Record*)(data2 + 2*sizeof(int) + pos_index*sizeof(Record));

    recarr[*count] = p_record;  
    *count = *count + 1;

    BF_UnpinBlock(block2);
  }

  BF_UnpinBlock(block);
}


HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2, char *index_key) {
  // insert code here
  if (openfiles[sindexDesc1] == NULL || openfiles[sindexDesc2] == NULL) {
    return HT_ERROR;
  }

  if (index_key != NULL){
    int count1 = 0;                                                                           //counter for records in first file
    Record* arr1[21];
    rec_arr(arr1, sindexDesc1, index_key, &count1);                                           //get all records with index_key = rec.key from first file

    int count2 = 0;                                                                           //counter for records in second file
    Record *arr2[21];
    rec_arr(arr2, sindexDesc2, index_key, &count2);                                           //get all records with index_key = rec.key from second file
    
    if (count1 !=0 && count2 != 0){                                                           //if both files have at least one record with index_key = key

      for (int i = 0; i < count1; i++) {
        for (int j = 0; j < count2; j++) {
          printf("%s", index_key);
          assert(strcmp(index_key,arr1[i]->surname)==0);
          assert(strcmp(index_key,arr2[j]->surname)==0);
          printf("| %d|%s|%s", arr1[i]->id, arr1[i]->name, arr1[i]->city);
          printf("| %d|%s|%s\n", arr2[j]->id, arr2[j]->name, arr2[j]->city);
        }
      }
    }

    if (hash_in_memory!= sindexDesc1) {                                                       //at the end of each join reset the hash table in memory to be the one from the first file
      hash_table = openfiles[sindexDesc1]->hash_table_pointer;
      global_depth = openfiles[sindexDesc1]->global_depth;
      hash_in_memory = sindexDesc1;
    }
  }

  else {
    
    if (hash_in_memory != sindexDesc1) {
      hash_table = openfiles[sindexDesc1]->hash_table_pointer;
      global_depth = openfiles[sindexDesc1]->global_depth;
      hash_in_memory = sindexDesc1;
    }

    BF_Block *block;
    BF_Block_Init(&block);

    for (int i = 0; i < pow(2, *global_depth); i++) {                                             //itterate hash table and for each record call inner_join
      
      if (i > 0) {
        if ((*hash_table)[i]->block_num == (*hash_table)[i-1]->block_num) {                           //if two cells points to the same block 
          continue;
        }
      
      }
      BF_GetBlock(sindexDesc1, (*hash_table)[i]->block_num, block);
      char *data = BF_Block_GetData(block);
      
      for (int j = 0; j < (*hash_table)[i]->num_of_records; j++) {
        SecondaryRecord* rec = (SecondaryRecord*)(data + 2*sizeof(int) + j*sizeof(SecondaryRecord));

        SHT_InnerJoin(sindexDesc1, sindexDesc2, rec->index_key);
      }

      BF_UnpinBlock(block);
    }
  }
}
