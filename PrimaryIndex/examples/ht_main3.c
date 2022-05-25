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
  printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;

  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));

  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));

  CALL_OR_DIE(HT_CloseFile(indexDesc));

  CALL_OR_DIE(HT_HashStatistics(FILE_NAME));//Hash Statistics works even with the file already closed
  BF_Close();
}
