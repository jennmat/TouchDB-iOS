//
//  TD_LazyView.h
//  TouchDB
//
//  Created by Matt on 6/3/13.
//
//

#import <TouchDB/TouchDB.h>
#import "TDInternal.h"

@interface TD_BackedView : TD_View {
@private
    NSString* _remoteDB;
    NSString* _remoteDDoc;
    NSString* _remoteView;
}

@property (readonly) NSString* remoteDB;
@property (readonly) NSString* remoteDDoc;
@property (readonly) NSString* remoteView;


- (id) initWithDatabase: (TD_Database*)db name: (NSString*)name withRemoteDatabase:(NSString*)remoteDB withRemoteDDoc: (NSString*) remoteDDoc withRemoteView: (NSString*) remoteView;

- (NSArray*) queryWithOptions: (const TDQueryOptions*)options
                       status: (TDStatus*)outStatus;

-(NSString*) buildQueryStringForQueryOptions:(const TDQueryOptions*) options;


@end
