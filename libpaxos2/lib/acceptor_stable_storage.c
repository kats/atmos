#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <assert.h>
/*
Getting started:
 http://www.oracle.com/technology/documentation/berkeley-db/db/gsg/C/index.html
API:
 http://www.oracle.com/technology/documentation/berkeley-db/db/api_c/frame.html
Reference Guide:
 http://www.oracle.com/technology/documentation/berkeley-db/db/ref/toc.html
BDB Forums @ Oracle
 http://forums.oracle.com/forums/forum.jspa?forumID=271
*/
#include <db.h>

#include "libpaxos_priv.h"
#include "acceptor_stable_storage.h"

//Size of cache <GB, B, ncaches
#define MEM_CACHE_SIZE (0), (4*1024*1024)
//DB env handle, DB handle, Transaction handle 
//FIXME: should be static, but abmagic can't link then
DB_ENV *dbenv;
DB *dbp;
DB_TXN *txn;

//Buffer to read/write current record
static char record_buf[MAX_UDP_MSG_SIZE];
static acceptor_record * record_buffer = (acceptor_record*)record_buf;

//Set to 1 if init should do a recovery
static int do_recovery = 0;

//Invoked before stablestorage_init, sets recovery mode on
// the acceptor will try to recover a DB rather than creating a new one
void stablestorage_do_recovery() {
    printf("Acceptor in recovery mode\n");
    do_recovery = 1;
}

static char db_env_path[512];
static char db_filename[512];
static char db_file_path[512];

int bdb_init_tx_handle(int tx_mode) {
    int result;

    //Create environment handle
    result = db_env_create(&dbenv, 0);
    if (result != 0) {
        printf("DB_ENV creation failed: %s\n", db_strerror(result));
        return -1;
    }

    //Durability mode, see paxos_config.h
    result = dbenv->set_flags(dbenv, tx_mode, 1);
    if (result != 0) {
        printf("DB_ENV set_flags failed: %s\n", db_strerror(result));
        return -1;
    }
    	
    //Redirect errors to sdout
    dbenv->set_errfile(dbenv, stdout);
    
    //Set the size of the memory cache
    result = dbenv->set_cachesize(dbenv, MEM_CACHE_SIZE, 1);
    if (result != 0) {
        printf("DB_ENV set_cachesize failed: %s\n", db_strerror(result));
        return -1;
    }
    
    //TODO see page size impact
    //Set page size for this db
    // result = dbp->set_pagesize(dbp, pagesize);
    // assert(result  == 0);

    //FIXME set log size

    
    // Environment open flags
    int flags;
    flags =
        DB_CREATE       |  /* Create if not existing */ 
        DB_RECOVER      |  /* Run normal recovery. */
        // DB_INIT_LOCK    |  /* Initialize the locking subsystem */
        DB_INIT_LOG     |  /* Initialize the logging subsystem */
        DB_INIT_TXN     |  /* Initialize the transactional subsystem. */
        DB_PRIVATE      |  /* DB is for this process only */
        // DB_THREAD       |  /* Cause the environment to be free-threaded */  
        DB_INIT_MPOOL;     /* Initialize the memory pool (in-memory cache) */
    
    //Open the DB environment
    result = dbenv->open(dbenv, 
        db_env_path,            /* Environment directory */
        flags,                  /* Open flags */
        0);                     /* Default file permissions */

    if (result != 0) {
        printf("DB_ENV open failed: %s\n", db_strerror(result));
        return -1;
    }
    
    return 0;
}

int bdb_init_db(char * db_path) {
    int result;
    //Create the DB file
    result = db_create(&dbp, dbenv, 0);
    if (result != 0) {
        printf("db_create failed: %s\n", db_strerror(result));
        return -1;
    }
    
    if(DURABILITY_MODE == 0 || DURABILITY_MODE == 20) {
        //Set the size of the memory cache
        result = dbp->set_cachesize(dbp, MEM_CACHE_SIZE, 1);
        if (result != 0) {
            printf("DBP set_cachesize failed: %s\n", db_strerror(result));
            return -1;
        }
    }
    
    // DB flags
    int flags = 
        DB_CREATE;          /*Create if not existing*/

    stablestorage_tx_begin();

    //Open the DB file
    result = dbp->open(dbp,
        txn,                    /* Transaction pointer */
        db_path,                /* On-disk file that holds the database. */
        NULL,                   /* Optional logical database name */
        ACCEPTOR_ACCESS_METHOD, /* Database access method */
        flags,                  /* Open flags */
        0);                     /* Default file permissions */

    stablestorage_tx_end();

    if(result != 0) {
        printf("DB open failed: %s\n", db_strerror(result));
        return -1;
    }
    
    return 0;
}

//Initializes the underlying stable storage
int stablestorage_init(int acceptor_id) {
    
    //Create path to db file in db dir
    sprintf(db_env_path, ACCEPTOR_DB_PATH);    
    sprintf(db_filename, ACCEPTOR_DB_FNAME);
    sprintf(db_file_path, "%s/%s", db_env_path, db_filename);
    LOG(VRB, ("Opening db file %s/%s\n", db_env_path, db_filename));    

    struct stat sb;
    //Check if the environment dir and db file exists
    int dir_exists = (stat(db_env_path, &sb) == 0);
    int db_exists = (stat(db_file_path, &sb) == 0);

    //Check for old db file if running recovery
    if(do_recovery && (!dir_exists || !db_exists)) {
        printf("Error: Acceptor recovery failed!\n");
        printf("The file:%s does not exist\n", db_file_path);
        return -1;
    }
    
    //Create the directory if it does not exist
    if(!dir_exists && (mkdir(db_env_path, S_IRWXU) != 0)) {
        printf("Failed to create env dir %s: %s\n", db_env_path, strerror(errno));
        return -1;
    } 
    
    //Delete and recreate an empty dir if not recovering
    if(!do_recovery && dir_exists) {
        char rm_command[600];
        sprintf(rm_command, "rm -r %s", db_env_path);
        
        if((system(rm_command) != 0) || 
            (mkdir(db_env_path, S_IRWXU) != 0)) {
            printf("Failed to recreate empty env dir %s: %s\n", db_env_path, strerror(errno));
        }
    }
    
    int ret = 0;
    char * db_file = db_filename;
    printf("Durability mode is: ");
    switch(DURABILITY_MODE) {
        //In memory cache
        case 0: {
            //Give full path if opening without handle
            printf("no durability!\n");
            db_file = db_file_path;
        }
        break;
        
        //Transactional storage
        case 10: {
            printf("transactional, no durability!\n");
            ret = bdb_init_tx_handle(DB_LOG_IN_MEMORY);
        }
        break;

        case 11: {
            printf("transactional, DB_TXN_NOSYNC\n");
            ret = bdb_init_tx_handle(DB_TXN_NOSYNC);
        }
        break;

        case 12: {
            printf("transactional, DB_TXN_WRITE_NOSYNC\n");
            ret = bdb_init_tx_handle(DB_TXN_WRITE_NOSYNC);
        }
        break;

        case 13: {
            printf("transactional, durable\n");
            ret = bdb_init_tx_handle(0);
        }
        break;
        
        case 20: {
            //Give full path if opening without handle
            printf("manual db flush\n");
            db_file = db_file_path;
        }
        break;

        default: {
            printf("Unknow durability mode %d!\n", DURABILITY_MODE);
            return -1;
        }
    }
    
    if(ret != 0) {
        printf("Failed to open DB handle\n");
    }
    
    if(bdb_init_db(db_file) != 0) {
        printf("Failed to open DB file\n");
        return -1;
    }
    
    return 0;
}

//Safely closes the underlying stable storage
int stablestorage_shutdown() {
    int result = 0;
    
    //Close db file
    if(dbp->close(dbp, 0) != 0) {
        printf("DB_ENV close failed\n");
        result = -1;
    }

    switch(DURABILITY_MODE) {
        case 0:
        case 20:
        break;
        
        //Transactional storage
        case 10:
        case 11:
        case 12:
        case 13: {
            //Close handle
            if(dbenv->close(dbenv, 0) != 0) {
                printf("DB close failed\n");
                result = -1;
            }
        }
        break;
        
        default: {
            printf("Unknow durability mode %d!\n", DURABILITY_MODE);
            return -1;
        }
    }    
 
    LOG(VRB, ("DB close completed\n"));  
    return result;
}

//Begins a new transaction in the stable storage
void 
stablestorage_tx_begin() {

    if(DURABILITY_MODE == 0 || DURABILITY_MODE == 20) {
        return;
    }

    int result;
    result = dbenv->txn_begin(dbenv, NULL, &txn, 0);
    assert(result == 0);
}

//Commits the transaction to stable storage
void 
stablestorage_tx_end() {
    int result;

    if(DURABILITY_MODE == 0) {
        return;
    }
    if (DURABILITY_MODE == 20) {
        result = dbp->sync(dbp, 0);
        assert(result == 0);
        return;
    }

    //Since it's either read only or write only
    // and there is no concurrency, should always commit!
    result = txn->commit(txn, 0);
    assert(result == 0);
}

//Retrieves an instance record from stable storage
// returns null if the instance does not exist yet
acceptor_record * 
stablestorage_get_record(iid_t iid) {
    int flags, result;
    DBT dbkey, dbdata;
    
    memset(&dbkey, 0, sizeof(DBT));
    memset(&dbdata, 0, sizeof(DBT));

    //Key is iid
    dbkey.data = &iid;
    dbkey.size = sizeof(iid_t);
    
    //Data is our buffer
    dbdata.data = record_buffer;
    dbdata.ulen = MAX_UDP_MSG_SIZE;
    //Force copy to the specified buffer
    dbdata.flags = DB_DBT_USERMEM;

    //Read the record
    flags = 0;
    result = dbp->get(dbp, 
        txn, 
        &dbkey, 
        &dbdata, 
        flags);
    
    if(result == DB_NOTFOUND || result == DB_KEYEMPTY) {
        //Record does not exist
        LOG(DBG, ("The record for iid:%u does not exist\n", iid));
        return NULL;
    } else if (result != 0) {
        //Read error!
        printf("Error while reading record for iid:%u : %s\n",
            iid, db_strerror(result));
        return NULL;
    }
    
    //Record found
    assert(iid == record_buffer->iid);
    return record_buffer;
}

//Save a valid accept request, the instance may be new (no record)
// or old with a smaller ballot, in both cases it creates a new record
acceptor_record * 
stablestorage_save_accept(accept_req * ar) {
    int flags, result;
    DBT dbkey, dbdata;
    
    //Store as acceptor_record (== accept_ack)
    record_buffer->iid = ar->iid;
    record_buffer->ballot = ar->ballot;
    record_buffer->value_ballot = ar->ballot;
    record_buffer->is_final = 0;
    record_buffer->value_size = ar->value_size;
    memcpy(record_buffer->value, ar->value, ar->value_size);
    
    memset(&dbkey, 0, sizeof(DBT));
    memset(&dbdata, 0, sizeof(DBT));

    //Key is iid
    dbkey.data = &ar->iid;
    dbkey.size = sizeof(iid_t);
        
    //Data is our buffer
    dbdata.data = record_buffer;
    dbdata.size = ACCEPT_ACK_SIZE(record_buffer);
    
    //Store permanently
    flags = 0;
    result = dbp->put(dbp, 
        txn, 
        &dbkey, 
        &dbdata, 
        0);

    assert(result == 0);    
    return record_buffer;
}

//Save a valid prepare request, the instance may be new (no record)
// or old with a smaller ballot
acceptor_record * 
stablestorage_save_prepare(prepare_req * pr, acceptor_record * rec) {
    int flags, result;
    DBT dbkey, dbdata;
    
    //No previous record, create a new one
    if (rec == NULL) {
        //Record does not exist yet
        rec = record_buffer;
        rec->iid = pr->iid;
        rec->ballot = pr->ballot;
        rec->value_ballot = 0;
        rec->is_final = 0;
        rec->value_size = 0;
    } else {
    //Record exists, just update the ballot
        rec->ballot = pr->ballot;
    }
    
    memset(&dbkey, 0, sizeof(DBT));
    memset(&dbdata, 0, sizeof(DBT));

    //Key is iid
    dbkey.data = &pr->iid;
    dbkey.size = sizeof(iid_t);
        
    //Data is our buffer
    dbdata.data = record_buffer;
    dbdata.size = ACCEPT_ACK_SIZE(record_buffer);
    
    //Store permanently
    flags = 0;
    result = dbp->put(dbp, 
        txn, 
        &dbkey, 
        &dbdata, 
        0);
        
    assert(result == 0);
    return record_buffer;
    
}

//Save the final value delivered by the underlying learner. 
// The instance may be new or previously seen, in both cases 
// this creates a new record
acceptor_record * 
stablestorage_save_final_value(char * value, size_t size, iid_t iid, ballot_t ballot) {

    int flags, result;
    DBT dbkey, dbdata;
    
    //Store as acceptor_record (== accept_ack)
    record_buffer->iid = iid;
    record_buffer->ballot = ballot;
    record_buffer->value_ballot = ballot;
    record_buffer->is_final = 1;
    record_buffer->value_size = size;
    memcpy(record_buffer->value, value, size);
    
    memset(&dbkey, 0, sizeof(DBT));
    memset(&dbdata, 0, sizeof(DBT));

    //Key is iid
    dbkey.data = &iid;
    dbkey.size = sizeof(iid_t);
        
    //Data is our buffer
    dbdata.data = record_buffer;
    dbdata.size = ACCEPT_ACK_SIZE(record_buffer);
    
    //Store permanently
    flags = 0;
    result = dbp->put(dbp, 
        txn, 
        &dbkey, 
        &dbdata, 
        0);

    assert(result == 0);    
    return record_buffer;

}
