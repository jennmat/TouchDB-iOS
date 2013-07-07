//
//  TD_LazyView.m
//  TouchDB
//
//  Created by Matt on 6/3/13.
//
//

#import "TD_RemoteView.h"
#import "TDRemoteRequest.h"

@implementation TD_LazyView

/*
 * A Lazy View doesn't compute it's own results.  Instead it relies on a backing couchdb server to compute the view
 * results.
 * Those results are cached locally in the sqlite db
 */
-(TDStatus) updateIndex
{
   
    TDRemoteJSONRequest * req = [[TDRemoteJSONRequest alloc]
        initWithMethod:@"GET" URL:[NSURL URLWithString:@"http://192.168.1.106/addresses/_design/friends/_view/by-first-name"] body:nil requestHeaders:nil
        onCompletion:^(id result, NSError *error){
            NSLog(@"Complete");
        }
    ];

    [req start];
    return kTDStatusOK;
}



@end
