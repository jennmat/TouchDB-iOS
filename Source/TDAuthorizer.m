//
//  TDAuthorizer.m
//  TouchDB
//
//  Created by Jens Alfke on 5/21/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

#import "TDAuthorizer.h"
#import "TDMisc.h"
#import "TDBase64.h"
#import "MYURLUtils.h"


@implementation TDBasicAuthorizer

- (id) initWithCredential: (NSURLCredential*)credential {
    Assert(credential);
    self = [super init];
    if (self) {
        _credential = [credential retain];
    }
    return self;
}

- (void)dealloc
{
    [_credential release];
    [super dealloc];
}

- (NSString*) authorizeURLRequest: (NSMutableURLRequest*)request
                         forRealm: (NSString*)realm
{
    NSString* username = _credential.user;
    NSString* password = _credential.password;
    if (username && password) {
        NSString* seekrit = $sprintf(@"%@:%@", username, password);
        seekrit = [TDBase64 encode: [seekrit dataUsingEncoding: NSUTF8StringEncoding]];
        return [@"Basic " stringByAppendingString: seekrit];
    }
    return nil;
}

- (NSString*) description {
    return $sprintf(@"%@[%@/****]", self.class, _credential.user);
}

@end


@implementation TDMACAuthorizer

- (id) initWithKey: (NSString*)key
        identifier: (NSString*)identifier
         algorithm: (NSString*)algorithm
         issueTime: (NSDate*)issueTime
{
    self = [super init];
    if (self) {
        _key = [key copy];
        _identifier = [identifier copy];
        _issueTime = [issueTime copy];
        if ([algorithm isEqualToString: @"hmac-sha-1"])
            _hmacFunction = &TDHMACSHA1;
        else if ([algorithm isEqualToString: @"hmac-sha-256"])
            _hmacFunction = &TDHMACSHA256;
        else {
            [self release];
            return nil;
        }
    }
    return self;
}


- (void)dealloc
{
    [_key release];
    [_identifier release];
    [_issueTime release];
    [super dealloc];
}


- (NSString*) authorizeURLRequest: (NSMutableURLRequest*)request
                         forRealm: (NSString*)realm
{
    // <http://tools.ietf.org/html/draft-ietf-oauth-v2-http-mac-00>
    if (!request)
        return nil;
    NSString* nonce = $sprintf(@"%.0f:%@", -[_issueTime timeIntervalSinceNow], TDCreateUUID());
    NSURL* url = request.URL;
    NSString* ext = @"";  // not implemented yet

    NSString* bodyHash = @"";
    NSData* body = request.HTTPBody;
    if (body.length > 0) {
        NSData* digest = (_hmacFunction == &TDHMACSHA1) ? TDSHA1Digest(body) : TDSHA256Digest(body);
        bodyHash = [TDBase64 encode: digest];
    }

    NSString* normalized = $sprintf(@"%@\n%@%@\n%@\n%d\n%@\n%@\n",
                                    nonce,
                                    request.HTTPMethod,
                                    url.my_pathAndQuery,
                                    [url.host lowercaseString],
                                    url.my_effectivePort,
                                    bodyHash,
                                    ext);
    NSString* mac;
    mac = [TDBase64 encode: _hmacFunction([_key dataUsingEncoding: NSUTF8StringEncoding],
                                          [normalized dataUsingEncoding: NSUTF8StringEncoding])];
    return $sprintf(@"MAC id=\"%@\", nonce=\"%@\", bodyhash=\"%@\", mac=\"%@\"",
                    _identifier, nonce, bodyHash, mac);
}


@end
