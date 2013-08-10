//
//  TD_ChainedDatabase.h
//  TouchDB
//
//  Created by Matt on 7/14/13.
//
//

#import <TouchDB/TouchDB.h>

/* A chained database is one that is 'backed' by a remote database.  Instead of computing it's own views, it relies on
 the backing database to do view computation.  View results are downloaded and cached.
 
 Similarly, documents are backed by the remote database.  If a document is not found locally, then the backing database
 supplies it.  The chained database caches it for use offline.
 
 Chained databases are strictly forbidden from computing their own views, since they do not necessarily have all the
 documents to compute against.
 */

@class TD_BackedView;

@interface TD_BackedDatabase : TD_Database {
    @private
    NSMutableDictionary* _backedViews;
    TDReplicator* pushRepl;
    TDReplicator* pullRepl;
}

@property NSString* backingDatabase;  /* This should be the full path to the backing database, including scheme host and port and database name */


- (id) initWithBackingDatabase: (NSString*)backingDatabase withPath:(NSString*) path;

+ (TD_BackedDatabase*) createEmptyDBAtPath: (NSString*)path withBackingDatabase:(NSString*) backingDatabase;

-(void)startReplication;

@end


