#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../include/bf.h"
#include "../include/hash_file.h"
#include "math.h"
#define RECORDS_NUM 512// you can change it if you want
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

void CALL_OR_DIE(BF_ErrorCode call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      BF_PrintError(code);
      exit(code);             \
    }                         \
  }

int main() {
  BF_Init(LRU);
  
  CALL_OR_DIE(HT_Init());

  int indexDesc;
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc));
  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int i = 0; i < RECORDS_NUM; ++i) {
    // create a record
    record.id = i+1+RECORDS_NUM;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

   CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
  }

  printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;

  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));

  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));

  CALL_OR_DIE(HT_HashStatistics(FILE_NAME));

  CALL_OR_DIE(HT_CloseFile(indexDesc));
  BF_Close();
}
