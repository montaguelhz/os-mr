#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <errno.h>
#include <pthread.h>
#include "mapreduce.h"

double getTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)1.0e-6 * tv.tv_usec;
}

void Map(char *file_name, int id)
{

    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);
    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1)
    {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL)
        {
            MR_Emit(token, "1", id);
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int id)
{
    int count = 0;
    char *value;
    while ((value = get_next(key, id)) != NULL)
        count++;
    printf("%s %d\n", key, count);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

int main(int argc, char *argv[])
{
    
    MR_Run(argc, argv, Map, 5, Reduce, 20, MR_DefaultHashPartition);
}
