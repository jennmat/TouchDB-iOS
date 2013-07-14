//
//  TD_LazyView.m
//  TouchDB
//
//  Created by Matt on 6/3/13.
//
//

#import "TD_RemoteView.h"
#import "TDRemoteRequest.h"
#import "TD_Body.h"
#import "TD_Database+Insertion.h"
#import "FMDatabase.h"
#import "FMDatabaseAdditions.h"
#import "FMResultSet.h"


@implementation TD_RemoteView


@synthesize remoteDB = _remoteDB;
@synthesize remoteHost = _remoteHost;
@synthesize remoteView = _remoteView;
@synthesize remoteDDoc = _remoteDDoc;

-(id)initWithDatabase:(TD_Database *)db name:(NSString *)name withRemoteHost:(NSString *)host withRemoteDatabase:(NSString *)remoteDB withRemoteDDoc:(NSString *)remoteDDoc withRemoteView:(NSString *)remoteView
{
    self = [self initWithDatabase:db name:name];
    
    _remoteHost = host;
    _remoteDB = remoteDB;
    _remoteView = remoteView;
    _remoteDDoc = remoteDDoc;
    
    [self updateViewToVersion:@"1"];
    
    return self;
}



static NSString* toJSONString( id object ) {
    if (!object)
        return nil;
    return [TDJSON stringWithJSONObject: object
                                options: TDJSONWritingAllowFragments
                                  error: NULL];
}



/*
 * A Lazy View doesn't compute it's own results.  Instead it relies on a backing couchdb server to compute the view
 * results.
 * Those results are cached locally in the sqlite db
 */
-(TDStatus) updateIndex
{
    int viewID = self.viewID;
    if (viewID <= 0)
        return kTDStatusNotFound;
    __block typeof(_db) db = _db;
    NSLog(@"%@", [db name]);
    
    //dispatch_semaphore_t sema = dispatch_semaphore_create(0);
    
    
    NSMutableURLRequest * req = [[NSMutableURLRequest alloc] initWithURL:[NSURL URLWithString:[NSString stringWithFormat:@"%@://%@/%@/_design/%@/_view/%@", @"http", self.remoteHost, self.remoteDB, self.remoteDDoc, self.remoteView]]];
    [req setValue: @"application/json" forHTTPHeaderField: @"Accept"];
    
    NSHTTPURLResponse* response = [NSHTTPURLResponse alloc];
    NSError* error;
    
    NSData* data = [NSURLConnection sendSynchronousRequest:req returningResponse:&response error:&error];
    
    if ( error == nil ){

        id result = [TDJSON JSONObjectWithData: data options: 0 error: NULL];
    
        NSDictionary* results = (NSDictionary*)result;
    
        [self.database beginTransaction];
        FMResultSet* r = nil;
        TDStatus status = kTDStatusDBError;
        
        @try {
            // Check whether we need to update at all:
            const SequenceNumber lastSequence = self.lastSequenceIndexed;
            //const SequenceNumber dbMaxSequence = self.database.lastSequence;
            //TODO:
            //if (lastSequence == dbMaxSequence) {
            //    status = kTDStatusNotModified;
            //    return status;
            //}
            
            //__block unsigned inserted = 0;
            FMDatabase* fmdb = db.fmdb;
            
            // First remove obsolete emitted results from the 'maps' table:
            //__block SequenceNumber sequence = lastSequence;
            //if (lastSequence < 0)
                //return kTDStatusDBError;
            BOOL ok;
            if (lastSequence == 0) {
                // If the lastSequence has been reset to 0, make sure to remove all map results:
                ok = [fmdb executeUpdate: @"DELETE FROM maps WHERE view_id=?", @(self.viewID)];
            } else {
                // Delete all obsolete map results (ones from since-replaced revisions):
                ok = [fmdb executeUpdate: @"DELETE FROM maps WHERE view_id=? AND sequence IN ("
                      "SELECT parent FROM revs WHERE sequence>? "
                      "AND parent>0 AND parent<=?)",
                      @(self.viewID), @(lastSequence), @(lastSequence)];
            }
            //if (!ok)
                //return kTDStatusDBError;
#ifndef MY_DISABLE_LOGGING
            //unsigned deleted = fmdb.changes;
#endif
            int totalResults = (int)[results objectForKey:@"total_rows"];
            
            FMResultSet* maxrs = [fmdb executeQuery:@"SELECT MAX(doc_id) FROM docs"];
            SInt64 nextdocid = 1;
            if ( [maxrs next] ){
                nextdocid = [maxrs longLongIntForColumnIndex:0];
            }
            
            if ( nextdocid == 0 ){
                /* Can't be 0, let's start at 1 */
                nextdocid = 1;
            }
            
            if ( totalResults > 0 ){
                NSArray * rows = (NSArray*)[results objectForKey:@"rows"];
                for(NSDictionary* row in rows ){
                    NSString* _id = [row objectForKey:@"id"];
                    NSString* _key = [row objectForKey:@"key"];
                    NSString* _value = [row objectForKey:@"value"];
                    
                    
                    NSString* keyJSON = toJSONString(_key);
                    NSString* valueJSON = toJSONString(_value);
                    
                    
                    LogTo(View, @"    emit(%@, %@)", keyJSON, valueJSON);
                    
                    BOOL rc;
                    
                    FMResultSet* rs = [fmdb executeQuery:@"SELECT doc_id FROM docs WHERE docid=?", _id];
                    sqlite_int64 sequence;
                    
                    if ( ![rs next] ){
                    
                        rc = [fmdb executeUpdate:@"INSERT INTO docs (doc_id, docid) values (?, ?)", @(nextdocid), _id];
                        
                        if ( rc == NO ){
                            NSLog(@"%@", [fmdb lastErrorMessage]);
                        }
                        rc = [fmdb executeUpdate:@"INSERT INTO revs (doc_id, revid, current) values (?, ?, 1)", @(nextdocid), @"STUB"];
                        if ( rc == NO ){
                            NSLog(@"%@", [fmdb lastErrorMessage]);
                        }
                        sequence = [fmdb lastInsertRowId];
                        
                        nextdocid++;
                    } else {
                        //[db get]
                       // sequence = [db getSequenceOfDocument:self<#(SInt64)#> revision:<#(NSString *)#> onlyCurrent:<#(BOOL)#>]
                    }
                    
                    rc = [fmdb executeUpdate: @"INSERT INTO maps (view_id, sequence, key, value) VALUES "
                               "(?, ?, ?, ?)",
                               @(viewID), @(sequence), keyJSON, valueJSON];
                    
                    if ( rc == NO ){
                        if ( rc == NO ){
                            NSLog(@"%@", [fmdb lastErrorMessage]);
                        }
                        NSLog(@"Update failed!");
                    } else {
                        NSLog(@"Update succeeded!");
                    }
                }
            }
            
            
            // Finally, record the last revision sequence number that was indexed:
            //TODO:
            //if (![fmdb executeUpdate: @"UPDATE views SET lastSequence=? WHERE view_id=?",
            //      @(dbMaxSequence), @(viewID)])
            //    return kTDStatusDBError;
            
            status = kTDStatusOK;
        
        } @finally {
            [r close];
            if (status >= kTDStatusBadRequest)
                Warn(@"TouchDB: Failed to rebuild view '%@': %d", self.name, status);
            [self.database endTransaction: (status < kTDStatusBadRequest)];
        }
      
    }

    return kTDStatusOK;
}

@class TD_Body;

TestCase(TD_RemoteView_Create){
    
    TD_Database* db = [TD_Database createEmptyDBAtPath: [NSTemporaryDirectory() stringByAppendingPathComponent: @"TouchDB_RemoteViewTest.touchdb"]];
    
    TD_RemoteView* rv = [db remoteViewNamed:@"properties-by-address" withRemoteHost:@"localhost:5984" withRemoteDB:@"properties" withRemoteDDoc:@"properties" withRemoteView:@"by-address"];
    
    [rv updateIndex];

    TDStatus s;
    
    const TDQueryOptions options = {
        .limit = 25
        // everything else will default to nil/0/NO
    };

    
    NSArray* arr = [rv queryWithOptions:&options status:&s];
    
    for(NSDictionary* dict in arr ){
        NSLog(@"Key:%@", [dict objectForKey:@"key"]);
    
        NSString * docId = [dict objectForKey:@"id"];
        
        TD_Revision* readRev = [db getDocumentWithID:docId revisionID:nil];
        TDStatus status;
        
        NSMutableDictionary* props = [readRev.properties mutableCopy];
        props[@"status"] = @"updated!";
        TD_Body* doc = [TD_Body bodyWithProperties: props];
        TD_Revision* rev2 = [[TD_Revision alloc] initWithBody: doc];
        //TD_Revision* rev2Input = rev2;
        
        rev2 = [db putRevision: rev2 prevRevisionID: readRev.revID allowConflict: NO status: &status];
    
        
    }
    
    
    NSURL* remote = [NSURL URLWithString: @"http://localhost:5984/properties"];
    TDReplicator* repl = [[TDReplicator alloc] initWithDB: db remote: remote
                                                     push: YES continuous: NO];
    [repl start];
    
    CAssert(repl.running);
    Log(@"Waiting for replicator to finish...");
    while (repl.running || repl.savingCheckpoint) {
        if (![[NSRunLoop currentRunLoop] runMode: NSDefaultRunLoopMode
                                      beforeDate: [NSDate dateWithTimeIntervalSinceNow: 0.5]])
            break;
    }
    
    
    
    
}



@end
