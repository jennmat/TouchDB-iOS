//
//  TD_LazyView.m
//  TouchDB
//
//  Created by Matt on 6/3/13.
//
//

#import "TD_BackedView.h"
#import "TDRemoteRequest.h"
#import "TD_Body.h"
#import "TD_Database+Insertion.h"
#import "TD_BackedDatabase.h"
#import "FMDatabase.h"
#import "FMDatabaseAdditions.h"
#import "FMResultSet.h"


@implementation TD_BackedView


@synthesize remoteDB = _remoteDB;
@synthesize remoteView = _remoteView;
@synthesize remoteDDoc = _remoteDDoc;

-(id)initWithDatabase:(TD_Database *)db name:(NSString *)name withRemoteDatabase:(NSString *)remoteDB withRemoteDDoc:(NSString *)remoteDDoc withRemoteView:(NSString *)remoteView
{
    self = [self initWithDatabase:db name:name];
    
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
    // Do nothing
    return kTDStatusOK;
}

- (BOOL) compileFromProperties: (NSDictionary*)viewProps {
    // DO Nothing
    return YES;
}

- (NSArray*) queryWithOptions: (const TDQueryOptions*)options
                       status: (TDStatus*)outStatus
{
    NSLog(@"Backed view is being queried");
    [self updateLocalCacheForQuery:options];

    return [super queryWithOptions:options status:outStatus];
}

-(NSString*) buildQueryStringForQueryOptions:(const TDQueryOptions*) options {
    NSMutableString* str = [[NSMutableString alloc] init];
    [str appendString:@"?"];
    if ( options->limit != UINT_MAX ){
        [str appendFormat:@"limit=%d&", options->limit];
    }
    if ( options->startKey != nil ){
        [str appendFormat:@"startkey=%@&", toJSONString(options->startKey)];
    }
    if ( options->endKey != nil ){
        [str appendFormat:@"endkey=%@&", toJSONString(options->endKey)];
    }
    
    return [str substringToIndex:[str length]-1];
    
}

-(TDStatus) updateLocalCacheForQuery: (const TDQueryOptions*) options {

    NSLog(@"Remote db is %@", self.remoteDB);
    int viewID = self.viewID;
    if (viewID <= 0)
        return kTDStatusNotFound;
    
    FMDatabase* fmdb = _db.fmdb;
    
    //dispatch_semaphore_t sema = dispatch_semaphore_create(0);

    //CREATE TABLE backed_view_etags ( \
      //view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, \
      //startkey TEXT COLLATE JSON\
      //endkey TEXT COLLATE JSON\
      //limit INTEGER\
      //etag TEXT NOT NULL\
      //);
    


    NSString* startkeyForQuery = toJSONString(options->startKey);
    if ( startkeyForQuery == nil ) startkeyForQuery = @"";
    
    NSString* endkeyForQuery = toJSONString(options->endKey);
    if ( endkeyForQuery == nil ) endkeyForQuery = @"";
    
    /* See if this particular view with query options was queried before,  if so I can include an etag */
    NSString* ifNoneMatch = [fmdb stringForQuery:@"SELECT etag FROM backed_view_etags WHERE view_id=? and startkey=? and endkey=? and query_limit=?", @(viewID), startkeyForQuery, endkeyForQuery, @(options->limit)];
    
    NSLog(@"Found ifNoneMatch: %@", ifNoneMatch);
    NSString* queryOptions = [self buildQueryStringForQueryOptions:options];
    
    NSString* url = [NSString stringWithFormat:@"%@/_design/%@/_view/%@%@", self.remoteDB, self.remoteDDoc, self.remoteView, queryOptions];
    NSMutableURLRequest * req = [[NSMutableURLRequest alloc] initWithURL:[NSURL URLWithString:url]];
    [req setCachePolicy:NSURLRequestReloadIgnoringLocalCacheData];
    [req setValue: @"application/json" forHTTPHeaderField: @"Accept"];
    if ( ifNoneMatch != nil ){
        [req setValue:ifNoneMatch forHTTPHeaderField:@"If-None-Match"];
    }
    
    NSLog(@"URL is %@", url);
    
    NSHTTPURLResponse* response = [NSHTTPURLResponse alloc];
    NSError* error;
    
    NSData* data = [NSURLConnection sendSynchronousRequest:req returningResponse:&response error:&error];
    
    if ( error != nil ){
        NSLog(@"Error querying %@", [error description]);
    } else {
        NSLog(@"No Error querying");
    }
    
    NSLog(@"Status code is %d", response.statusCode);
    
    if ( error == nil && response.statusCode == 200 ){
       
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
            NSLog(@"Found results: %d", totalResults);
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
                        SInt64 docid = [rs intForColumnIndex:0];
                        NSLog(@"Found doc id: %@", @(docid));
                        TD_Revision* rev = [_db getDocumentWithID:_id revisionID:nil];
                        if( rev ){
                            sequence = [rev sequence];
                            NSLog(@"Found sequence %@", @(sequence));
                        }
                    }
                    
                    if ( sequence > 0 ){
                    
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
            }
            
            
            // Finally, record the last revision sequence number that was indexed:
            //TODO:
            //if (![fmdb executeUpdate: @"UPDATE views SET lastSequence=? WHERE view_id=?",
            //      @(dbMaxSequence), @(viewID)])
            //    return kTDStatusDBError;
            
            status = kTDStatusOK;
            
            
            /* Record the etag received */
            NSString* etag = [[response allHeaderFields] valueForKey:@"Etag"];
        
            if ( ifNoneMatch != nil ){
                NSLog(@"Updating etag to %@", etag);
                /* Update the etag in place */
                [fmdb executeUpdate:@"UPDATE backed_view_etags SET etag=? WHERE view_id=? and startkey=? and endkey=? and query_limit=?", etag, @(viewID), toJSONString(options->startKey), toJSONString(options->endKey), @(options->limit)];
            } else {
                /* First time, insert a row instead */
                NSLog(@"Inserting a row for if none match: %@", etag);
                
                [fmdb executeUpdate:@"INSERT INTO backed_view_etags VALUES (?, ?, ?, ?, ?)", @(viewID), endkeyForQuery, startkeyForQuery, @(options->limit), etag];
                
            }
            
            
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
@class TD_BackedDatabase;

TestCase(TD_BackedView_Create){
    
    
    TD_BackedDatabase* db = [TD_BackedDatabase createEmptyDBAtPath:[NSTemporaryDirectory() stringByAppendingPathComponent: @"TouchDB_BackedViewCreate.touchdb"] withBackingDatabase:@"http://localhost:5984/properties"];
    
    
    TD_View* rv = [db viewNamed:@"properties/by-address"];
    
    TDStatus s;
    
    const TDQueryOptions options = {
        // everything else will default to nil/0/NO
    };

    
    NSArray* arr = [rv queryWithOptions:&options status:&s];
    
    arr = [rv queryWithOptions:&options status:&s];
    
    for(NSDictionary* dict in arr ){
        NSLog(@"Key:%@", [dict objectForKey:@"key"]);
    
        /*NSString * docId = [dict objectForKey:@"id"];
        
        TD_Revision* readRev = [db getDocumentWithID:docId revisionID:nil];
        TDStatus status;
        NSMutableDictionary* props = [readRev.properties mutableCopy];
        props[@"status2"] = @"updated!";
        TD_Body* doc = [TD_Body bodyWithProperties: props];
        TD_Revision* rev2 = [[TD_Revision alloc] initWithBody: doc];
        //TD_Revision* rev2Input = rev2;
        
        rev2 = [db putRevision: rev2 prevRevisionID: readRev.revID allowConflict: NO status: &status];
            */
    }
    
    
}

TestCase(TD_BackedView_BuildQueryStringFromQueryOptions){
    //TD_BackedDatabase* db = [TD_BackedDatabase createEmptyDBAtPath:[NSTemporaryDirectory() stringByAppendingPathComponent: @"TouchDB_RemoteViewTest.touchdb"] withBackingDatabase:@"http://localhost:5984/properties"];
    
    
    //TD_View* rv = [db viewNamed:@"properties/properties-by-address"];

    
    TDQueryOptions options = kDefaultTDQueryOptions;
  
    options.limit = 25;
    
    //NSString* str = [rv buildQueryStringForQueryOptions:&options];
    
    //CAssertEqual(str, @"?limit=25");

    
    options.startKey = @"2";
    
    //str = [rv buildQueryStringForQueryOptions:&options];
    
    //CAssertEqual(str, @"?limit=25&startkey=\"2\"");

    
    options.endKey = @"44444";

    
    //str = [rv buildQueryStringForQueryOptions:&options];
    
    //CAssertEqual(str, @"?limit=25&startkey=\"2\"&endkey=\"44444\"");

}



TestCase(TD_BackedView) {
    RequireTestCase(TD_BackedView_Create);
    RequireTestCase(TD_BackedView_BuildQueryStringFromQueryOptions);
}




@end
