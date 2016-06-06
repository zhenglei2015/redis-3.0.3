#include "category.h"

static char *filename = "category.txt";

ssize_t sizeOfStringObject(robj *obj) {
    if (obj->encoding == REDIS_ENCODING_INT) {
        return sizeof(long long);
    } else {
        return sdsAllocSize(obj->ptr);
    }
}

long long categoryObjectSize(robj *o) {
    long long total_size = 0;
    if (o->type == REDIS_STRING) {
        /* calculate a string value */
        total_size = sizeOfStringObject(o);
    } else if (o->type == REDIS_LIST) {
        /* calculate a list value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);
            total_size += l;
        } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
            list *list = o->ptr;
            listIter li;
            listNode *ln;

            listRewind(list,&li);
            while((ln = listNext(&li))) {
                robj *eleobj = listNodeValue(ln);
                total_size += sizeOfStringObject(eleobj);
            }
        } else {
            redisPanic("Unknown list encoding");
        }
    } else if (o->type == REDIS_SET) {
        /* calculate a set value */
        if (o->encoding == REDIS_ENCODING_HT) {
            dict *set = o->ptr;
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;

            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetKey(de);
                total_size += sizeOfStringObject(eleobj);
            }
            dictReleaseIterator(di);
        } else if (o->encoding == REDIS_ENCODING_INTSET) {
            size_t l = intsetBlobLen((intset*)o->ptr);

            total_size += l;
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (o->type == REDIS_ZSET) {
        /* calculate a sorted set value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);
            total_size += l;
        } else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;

            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetKey(de);
                total_size += sizeOfStringObject(eleobj);
            }
            dictReleaseIterator(di);
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else if (o->type == REDIS_HASH) {
        /* calculate a hash value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);
            total_size += l;
        } else if (o->encoding == REDIS_ENCODING_HT) {
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            while((de = dictNext(di)) != NULL) {
                robj *key = dictGetKey(de);
                robj *val = dictGetVal(de);
                total_size += sizeOfStringObject(key);
                total_size += sizeOfStringObject(val);
            }
            dictReleaseIterator(di);

        } else {
            redisPanic("Unknown hash encoding");
        }

    } else {
        redisPanic("Unknown object type");
    }
    return total_size;
}

void ccRemoveTempFile(pid_t p) {
    char tmpfile[300];
    snprintf(tmpfile,256,"category-%d.txt", (int) p);
    unlink(tmpfile);
}
void saveResult(dict* tempDict) {
    rio r;
    FILE *fp;
    char tmpfile[300];
    snprintf(tmpfile,256,"category-%d.txt", (int) getpid());
    fp = fopen(tmpfile, "w");
    rioInitWithFile(&r,fp);

    dictIterator *di = dictGetIterator(tempDict);
    dictEntry *de;
    char *line;
    line = (char *)zmalloc(10000);
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        sds val = dictGetVal(de);
        int totalLen = sdslen(key) + sdslen(val);
        if(totalLen > 10000) {
            zfree(line);
            line = zmalloc(totalLen + 200);
        }
        snprintf(line, totalLen + 20, "%s$$%s\n", key, val);
        rioWrite(&r, line, totalLen + 3);
    }
    zfree(line);
    dictReleaseIterator(di);
    // flush 掉
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;
    if (fclose(fp) == EOF) goto werr;
    if(rename(tmpfile,filename) == -1) {
        unlink(tmpfile);
        goto werr;
    }
    return;

    werr:
//    serverLog(LL_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return ;
}

void doCalculateCategory() {
    dict *tempDict;
    tempDict = dictCreate(&categoryStatsDictType, NULL);
    dictIterator *di = NULL;
    dictEntry *de;

    dict *d = server.db[0].dict;
    if (dictSize(d) == 0) return;
    di = dictGetSafeIterator(d);
    if (!di) return ;

    /* Iterate this DB writing every entry */
    while((de = dictNext(di)) != NULL) {
        sds keystr = dictGetKey(de);
        robj key, *o = dictGetVal(de);

        initStaticStringObject(key,keystr);
        int keysize = sdslen(keystr);
        long long objsize = categoryObjectSize(o);
        addCategoryStats(&key, keysize + objsize, tempDict);
    }
    dictReleaseIterator(di);
    saveResult(tempDict);
}

void categoryInfoInsert(void *p) {
    p = NULL; // 防止 warning
    FILE *file = fopen(filename, "r");
    rio r;
    rioInitWithFile(&r, file);
    char line[10000];
    int len = 10000;
    dictEmpty(server.categoryStatsDict, NULL);
    while(fgets(line,len,file)!=NULL) {
        int pos = strstr(line, "$$") - line;
        if(pos > 0) {
            sds key = sdsnewlen(line, pos);
            sds val = sdsnewlen(line + pos + 2, strlen(line) - pos - 3);
            if(dictAdd(server.categoryStatsDict, key, val) != DICT_OK) {

            } else {

            }
        }
    }
    fclose(file);
}

void *waitToUpdate(void *p) {
    pid_t calp = *(pid_t*)(p);
    long long ts = time(NULL);
    waitpid(calp,NULL,0);
//    printf("category occupy memory space over pid %d\n", calp);
    dictEmpty(server.categoryStatsDict, NULL);
    categoryInfoInsert(0);
    long long te = time(NULL);
    printf("time used %lld\n", te - ts);
    server.calculateCategoryChild = -1;
    return ((void *)0);
}


void addCategoryStats(robj *key, long long valsize, dict* tempDict) {
    int len = strlen(key->ptr);
    char *categoryKey = (char *)sdsnewlen(key->ptr, len);
    for(int i = 0; i < len; i++) {
        if(categoryKey[i] == '.') {
            categoryKey[i] = '\0';
            break;
        }
    }
    sds k = sdsnewlen(categoryKey, strlen(categoryKey));// 这一步没必要
    long long change = valsize;
    char changeStr[50];
    dictEntry *di;
    if((di = dictFind(tempDict, k)) == NULL) {
        sprintf(changeStr, "%lld" , change);
        sds v = sdsnew(changeStr);
        dictAdd(tempDict, k, v);
    } else {
        sds* oldv = dictGetVal(di);
        long long old = atol((char *)oldv);
        sprintf(changeStr, "%lld" , change + old);
        sds v = sdsnew(changeStr);
        dictDelete(tempDict,k);
        dictAdd(tempDict, k, v);
    }
}

void killccCommand(redisClient *c) {
    if(server.calculateCategoryChild != -1){
        kill(server.calculateCategoryChild, SIGINT);
        char line[300];
        snprintf(line, 256, "%d be killed", server.calculateCategoryChild);
        server.calculateCategoryChild = -1;
        addReplyStatus(c, line);
    } else {
        char line[300] = "calculate process is not running";
        addReplyStatus(c, line);
    }
}

void ccCommand(redisClient *c) {
    pid_t p;
    if(server.calculateCategoryChild != -1) {
        addReplyError(c, "ctegory calculating thread is running\n");
    } else if((p = fork()) == 0) { /* child */
        doCalculateCategory();
        exit(0);
    } else {
        setpgid(getpid(), getpid());
        server.calculateCategoryChild = p;
        pthread_t tid;
        if (pthread_create(&tid,NULL,waitToUpdate,&p) == 0) {
            pthread_detach(tid);
            char line[300];
            snprintf(line, 256, "%d is runing to do calculate", (int) getpid());
            addReplyStatus(c, line);
        } else {
            addReplyStatus(c, "create wait thread failed");
        }
    }
}
