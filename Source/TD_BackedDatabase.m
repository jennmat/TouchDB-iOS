//
//  TD_ChainedDatabase.m
//  TouchDB
//
//  Created by Matt on 7/14/13.
//
//

#import "TD_BackedDatabase.h"
#import "TD_BackedView.h"
#import "TD_Database+Insertion.h"

@implementation TD_BackedDatabase

@synthesize backingDatabase;

-(id) initWithBackingDatabase:(NSString *)database withPath:(NSString *)path
{
    self  = [self initWithPath:path];
    
    self.backingDatabase = database;
    
    return self;
}

+ (TD_Database*) createEmptyDBAtPath: (NSString*)path withBackingDatabase:(NSString *)backingDatabase
{
    TD_BackedDatabase* db = (TD_BackedDatabase *)[self createEmptyDBAtPath:path];
    db.backingDatabase = backingDatabase;
    
    return db;
}


- (TD_View*) viewNamed: (NSString*)name {
    CAssert(false, @"Only backed views are supported by backed databases");
}


- (TD_View*) existingViewNamed: (NSString*)name {
    CAssert(false, @"Only backed views are supported by backed databases");
}


- (TD_BackedView*) registerView: (TD_BackedView*)view {
    if (!view)
        return nil;
    if (!_remoteViews)
        _remoteViews = [[NSMutableDictionary alloc] init];
    _remoteViews[view.name] = view;
    return view;
}


- (TD_BackedView*) backedViewNamed: (NSString*) name withRemoteDDoc:(NSString*)ddoc withRemoteView:(NSString*)remoteView
{
    TD_BackedView* view = (TD_BackedView*)_remoteViews[name];
    if( view ){
        return view;
    }
    view = [[TD_BackedView alloc] initWithDatabase:self name:name withRemoteDatabase:self.backingDatabase withRemoteDDoc:ddoc withRemoteView:remoteView];
    
    return [self registerView:view];
}




- (TD_Revision *) getDocumentWithID: (NSString*)docID
                         revisionID: (NSString*)revID
                            options: (TDContentOptions)options
                             status: (TDStatus*)outStatus
{
    TD_Revision* rev = [super getDocumentWithID:docID revisionID:revID options:options status:outStatus];

    NSString* ifNoneMatch = @"";
    
    if ( *outStatus != kTDStatusNotFound ) {
        ifNoneMatch = rev.revID;
    }
    
    /* Attempt to refresh it from the backing server */
    NSMutableURLRequest * req = [[NSMutableURLRequest alloc] initWithURL:[NSURL URLWithString:[NSString stringWithFormat:@"%@/%@", self.backingDatabase, docID]]];

    [req setCachePolicy:NSURLRequestReloadIgnoringLocalCacheData];
    [req setValue: @"application/json" forHTTPHeaderField: @"Accept"];
    [req setValue: [NSString stringWithFormat:@"\"%@\"", ifNoneMatch] forHTTPHeaderField:@"If-None-Match"];
    NSHTTPURLResponse* response = [NSHTTPURLResponse alloc];
    NSError* error;
    

    NSData* data = [NSURLConnection sendSynchronousRequest:req returningResponse:&response error:&error];
    //NSString* revId = [[response allHeaderFields] objectForKey:@"ETag"];
    
    if ( response.statusCode == 304 ){
        *outStatus = kTDStatusNotModified;
        return rev;
    } else if (response.statusCode == 200) {
        TDJSON * result = [TDJSON JSONObjectWithData: data options: 0 error: NULL];
        NSDictionary* doc = $castIf(NSDictionary, result);
        
        TD_Revision* rev = [[TD_Revision alloc] initWithProperties:doc];
        
        [self forceInsert:rev revisionHistory:nil source:[NSURL URLWithString:self.backingDatabase]];
        *outStatus = kTDStatusOK;
        return rev;
    }
    
    return rev;
}


@end
