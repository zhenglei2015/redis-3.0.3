#include "redis.h"
#include "rio.h"
#include <sys/types.h>
#include <sys/wait.h>

long long categoryObjectSize(robj *o);
void doCalculateCategory();
void ccCommand(redisClient *c);
void killccCommand(redisClient *c);
void addCategoryStats(robj *key, long long size, dict* d);
void ccRemoveTempFile(pid_t p);

