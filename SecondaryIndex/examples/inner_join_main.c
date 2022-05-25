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


int main(){

    BF_Init(LRU);
    char filename[20];
    strcpy(filename, "Primary_Database");
    char sfilename1[20];
    strcpy(sfilename1,"Secondary_Database1");
    remove(filename);
    remove(sfilename1);

    HT_Init();
    CALL_OR_DIE(HT_CreateIndex(filename,3));

    int index;
    CALL_OR_DIE(HT_OpenIndex(filename, &index));

    SHT_Init();

    char* attrKey="surname";
    SHT_CreateSecondaryIndex(sfilename1, attrKey, strlen(attrKey) + 1, 4, filename);

    int sindex1;
    SHT_OpenSecondaryIndex(sfilename1, &sindex1);

    Record record;
    srand(time(NULL));
    int r;
    printf("Insert Entries\n");

    int tupleId;
    SecondaryRecord srec;
    
    for (int i = 0; i < 8; ++i) {                                 
        // create a record
        record.id = i;
        r = rand() % 12;
        memcpy(record.name, names[r], strlen(names[r]) + 1);
        r = rand() % 12;
        memcpy(record.surname, surnames[i], strlen(surnames[i]) + 1);
        r = rand() % 10;
        memcpy(record.city, cities[r], strlen(cities[r]) + 1);

        UpdateRecordArray array[8];
        array[0].size=-1;
        HT_InsertEntry(index, record, &tupleId, array);

        strcpy(srec.index_key, record.surname);
        srec.tupleId=tupleId;

        SHT_SecondaryUpdateEntry(sindex1, array);
        SHT_SecondaryInsertEntry(sindex1, srec);
    }

    char filename2[20];
    strcpy(filename2,"Primary_Database2");
    char sfilename2[20];
    strcpy(sfilename2, "Secondary_Database2");
    remove(filename2);
    remove(sfilename2);

    CALL_OR_DIE(HT_CreateIndex(filename2, 3));

    int index2;
    CALL_OR_DIE(HT_OpenIndex(filename2, &index2));

    char* attrKey2 = "surname";
    SHT_CreateSecondaryIndex(sfilename2, attrKey2, strlen(attrKey2) + 1, 4, filename2);

    int sindex2;
    SHT_OpenSecondaryIndex(sfilename2, &sindex2);

    
    Record record2;
    int r2;
    int tupleId2;
    SecondaryRecord srec2;
    for (int i = 0; i < 8; ++i) {
        // create a record
        if (i > 1) {
            record2.id = i;
            r2 = rand() % 12;
            memcpy(record2.name, names[r2], strlen(names[r2]) + 1);
            r2 = rand() % 12;
            memcpy(record2.surname, surnames[i], strlen(surnames[i]) + 1);
            r2 = rand() % 10;
            memcpy(record2.city, cities[r2], strlen(cities[r2]) + 1);
        }
        else {
            record2.id = i;
            r2 = rand() % 12;
            memcpy(record2.name, names[r2], strlen(names[r2]) + 1);
            r2 = rand() % 12;
            memcpy(record2.surname, surnames[0], strlen(surnames[0]) + 1);
            r2 = rand() % 10;
            memcpy(record2.city, cities[r2], strlen(cities[r2]) + 1);
        }

        UpdateRecordArray array[8];
        array[0].size = -1;
        HT_InsertEntry(index2, record2, &tupleId2, array);
        strcpy(srec2.index_key, record2.surname);
        srec2.tupleId = tupleId2;

        SHT_SecondaryUpdateEntry(sindex2, array);
        SHT_SecondaryInsertEntry(sindex2, srec2);

    }

    printf("Inner Join: \n");
    SHT_InnerJoin(sindex1, sindex2, NULL);


  HT_CloseFile(index);
  HT_CloseFile(index2);
  SHT_CloseSecondaryIndex(sindex1);
  SHT_CloseSecondaryIndex(sindex2);
  BF_Close();
}