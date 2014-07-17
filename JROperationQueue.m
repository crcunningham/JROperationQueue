// The MIT License (MIT)
//
// Copyright (c) 2014 Chris Cunningham
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.


#import "JROperationQueue.h"

@interface JROperationQueue ()

@property (nonatomic) NSUInteger maxOperationsCount;
@property (nonatomic) NSUInteger currentOperationCount;
@property (nonatomic) MCOperationQueueType type;
@property (nonatomic) NSMutableArray* standardPriorityBlocks;
@property (nonatomic) NSMutableArray* highPriorityBlocks;
@property (nonatomic) NSMapTable* requestIdToBlockMap;
@property (nonatomic) NSUInteger requestIdCounter;
@property (nonatomic) dispatch_queue_t queue;

@end

@implementation JROperationQueue

- (id)initWithMaxOperationCount:(NSUInteger)count type:(MCOperationQueueType)type
{
	self = [super init];
	
	if (self)
	{
		if (count <= 0) {
			count = NSUIntegerMax;
        }
		
		self.queue = dispatch_queue_create("jroperationqueue", DISPATCH_QUEUE_CONCURRENT);
		self.maxOperationsCount = count;
		self.type = type;
		self.standardPriorityBlocks = [[NSMutableArray alloc] init];
		self.highPriorityBlocks = [[NSMutableArray alloc] init];
		self.requestIdToBlockMap = [NSMapTable strongToWeakObjectsMapTable];
	}
	
	return self;
}

- (NSUInteger)addOperationWithBlock:(void (^)(void))block isHighPriority:(BOOL)isHighPriority
{
	NSUInteger requestId;
	
	@synchronized(self)
	{
		self.requestIdCounter += 1;
		requestId = self.requestIdCounter;
		void (^copy)(void) = [block copy];

		if(isHighPriority) {
			[self.highPriorityBlocks addObject:copy];
        }
		else {
			[self.standardPriorityBlocks addObject:copy];
        }

		[self.requestIdToBlockMap setObject:copy forKey:@(self.requestIdCounter)];
		
		// Give the caller time to store the requestId before we start the operation
        double delayInSeconds = 0.0;
        dispatch_time_t popTime = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(delayInSeconds * NSEC_PER_SEC));
        dispatch_after(popTime, self.queue, ^(void){
            [self processOperations];
        });
	}
	
	return requestId;
}

- (NSUInteger)addHighPriorityOperationWithBlock:(void (^)(void))block
{
	return [self addOperationWithBlock:block isHighPriority:YES];
}

- (NSUInteger)addOperationWithBlock:(void (^)(void))block
{
	return [self addOperationWithBlock:block isHighPriority:NO];
}

- (void)processOperations
{
	[self processOperationsInBlockArray:self.highPriorityBlocks];
	[self processOperationsInBlockArray:self.standardPriorityBlocks];
}

- (void)processOperationsInBlockArray:(NSMutableArray*)blocks
{
	@synchronized(self)
	{
		while (self.currentOperationCount < self.maxOperationsCount && [blocks count] > 0)
		{
			NSUInteger index;
			if (JROperationQueueTypeLIFO == self.type)
				index = [blocks count] - 1;
			else // JROperationQueueTypeFIFO
				index = 0;
			
			void (^block)(void) = blocks[index];
			[self.requestIdToBlockMap removeObjectForKey:@(index)];
			[blocks removeObjectAtIndex:index];
			
			self.currentOperationCount += 1;
			dispatch_async(self.queue, ^{
				@autoreleasepool
				{
					block();
				}
				
				@synchronized(self)
				{
					self.currentOperationCount -= 1;
					[self processOperations];
				}
			});
		}
	}
}

- (void)cancelOperation:(NSUInteger)requestId
{
	@synchronized(self)
	{
		void (^block)(void) = [self.requestIdToBlockMap objectForKey:@(requestId)];
		
		if (block)
		{
			//NSLog(@"cancelled request with Id: %lu", (unsigned long)requestId);
			[self.highPriorityBlocks removeObject:block];
			[self.standardPriorityBlocks removeObject:block];
			[self.requestIdToBlockMap removeObjectForKey:@(requestId)];
		}
	}
}

- (void)cancelAllOperations
{
	@synchronized(self)
	{
		[self.standardPriorityBlocks removeAllObjects];
		[self.highPriorityBlocks removeAllObjects];
		[self.requestIdToBlockMap removeAllObjects];
	}
}

@end