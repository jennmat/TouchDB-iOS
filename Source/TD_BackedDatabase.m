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


- (TD_View*) registerBackedView: (TD_BackedView*)view {
    if (!view)
        return nil;
    if (!_backedViews)
        _backedViews = [[NSMutableDictionary alloc] init];
    _backedViews[view.name] = view;
    return view;
}


-(TD_View*) existingViewNamed:(NSString *)name {
    return [self viewNamed:name];
}

- (TD_View*) viewNamed: (NSString*)name {
    NSArray* parts = [name componentsSeparatedByString:@"/"];
    if ( parts.count != 2 ){
        return nil;
    }
    
    NSString* ddoc = parts[0];
    NSString* remoteView = parts[1];
    
    NSLog(@"Looking for backed view named %@ %@", ddoc, remoteView );
    
    
    TD_BackedView* view = (TD_BackedView*)_backedViews[name];
    if( view ){
        return view;
    }
    view = [[TD_BackedView alloc] initWithDatabase:self name:name withRemoteDatabase:self.backingDatabase withRemoteDDoc:ddoc withRemoteView:remoteView];
    NSLog(@"Registering a backed view for %@ %@", ddoc, remoteView);
    return [self registerBackedView:view];
}




@end
