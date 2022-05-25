#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "sht_file.h"

#define RECORDS_NUM 1000 // you can change it if you want
#define GLOBAL_DEPT 2 // you can change it if you want
#define FILE_NAME "data.db"

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {  
  BF_Init(LRU);
  char filename[20];
  strcpy(filename,"Primary_Database");
  char sfilename1[20];
  strcpy(sfilename1,"Secondary_Database1");
  char sfilename2[20];
  strcpy(sfilename2,"Secondary_Database2");
  remove(filename);
  remove(sfilename1);
  remove(sfilename2);

  int index;
  int sindex1;
  int sindex2;
  HT_Init();

  CALL_OR_DIE(HT_CreateIndex(filename,3));

  CALL_OR_DIE(HT_OpenIndex(filename,&index));

  SHT_Init();

  char* attrKey1="city";

  char* attrKey2="surname";

  SHT_CreateSecondaryIndex(sfilename1,attrKey1,strlen(attrKey1)+1,4,filename);

  SHT_CreateSecondaryIndex(sfilename2,attrKey2,strlen(attrKey2)+1,4,filename);

  SHT_OpenSecondaryIndex(sfilename1,&sindex1);
  SHT_OpenSecondaryIndex(sfilename2,&sindex2);
  Record rec;
  SecondaryRecord srec;
  int tupleId;
  for (int i=0;i<10;i++)  {
    for (int k=0;k<2;k++)  {
      
      rec.id=i*10+k;
      int r=rand()%10;
      memcpy(rec.city, cities[i], strlen(cities[i]) + 1);
      memcpy(rec.name, names[r], strlen(names[r]) + 1);
      memcpy(rec.surname, surnames[i], strlen(surnames[i]) + 1);
      
      UpdateRecordArray array[8];
      array[0].size=-1;

      
      HT_InsertEntry(index,rec,&tupleId,array);
      //HT_PrintAllEntries(index,NULL);
      strcpy(srec.index_key,rec.city);
      srec.tupleId=tupleId;
      printf("\nInserting %s to SHT\n",srec.index_key);
      

      SHT_SecondaryUpdateEntry(sindex1,array);
      SHT_SecondaryInsertEntry(sindex1,srec);
      SHT_PrintAllEntries(sindex1,rec.city);

      
      SHT_SecondaryUpdateEntry(sindex2,array);
      strcpy(srec.index_key,rec.surname);
      printf("\nInserting %s to SHT\n",srec.index_key);
      SHT_SecondaryInsertEntry(sindex2,srec);
      SHT_PrintAllEntries(sindex2,rec.surname);
    }
    //break;
  }
  



  SHT_CloseSecondaryIndex(sindex1);
  SHT_CloseSecondaryIndex(sindex2);
  HT_CloseFile(index);
  HT_OpenIndex(filename,&index);
  SHT_OpenSecondaryIndex(sfilename1,&sindex1);
  SHT_OpenSecondaryIndex(sfilename2,&sindex2);

  for (int i=0;i<10;i++) {
    char index_key[20];
    
    memcpy(index_key,cities[i],strlen(cities[i])+1);
    printf("\nAll entries with city:%s\n",cities[i]);
    SHT_PrintAllEntries(sindex1,index_key);
  }

  for (int i=0;i<10;i++) {
    char index_key[20];
    memcpy(index_key,surnames[i],strlen(surnames[i])+1);
    printf("\nAll entries with surname:%s\n",surnames[i]);
    SHT_PrintAllEntries(sindex2,index_key);
  }

  SHT_CloseSecondaryIndex(sindex1);
  SHT_CloseSecondaryIndex(sindex2);
  HT_CloseFile(index);
  BF_Close();


  
}

