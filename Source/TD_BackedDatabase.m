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
    [self startReplication];
    return self;
}



+ (TD_Database*) createEmptyDBAtPath: (NSString*)path withBackingDatabase:(NSString *)backingDatabase
{
    TD_BackedDatabase* db = (TD_BackedDatabase *)[self createEmptyDBAtPath:path];
    db.backingDatabase = backingDatabase;
    [db startReplication];
    return db;
}

-(void) startReplication {
    /* Start a continuous replication to the backed databae */
    NSURL* remote = [NSURL URLWithString: self.backingDatabase];
    pushRepl = [[TDReplicator alloc] initWithDB: self remote: remote
                                           push: YES continuous: NO];
    
    pullRepl = [[TDReplicator alloc] initWithDB:self remote:remote push:NO continuous:YES];
    
    [pushRepl start];
    [pullRepl start];
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




@end
