#include "redis.h"
#include "rio.h"

long long categoryObjectSize(robj *o);
void doCalculateCategory();
void ccCommand(redisClient *c);
void killccCommand(redisClient *c);
void addCateforyStats(robj *key, long long size, dict* d);

