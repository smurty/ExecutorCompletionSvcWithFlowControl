# Executor Completion Service With Flow Control

## Introduction
`ExecutorCompletionService` provides an excellent way to run batch jobs on several million work items with configurable concurrency. It provides:
* Unbounded work queue to accept inbound requests
* worker thread pool to perform work
* Unbounded completion queue to collect processed work

## What problem does it solve?
While this may suffice for many use cases, this model lacks flow control, for both inbound and outbound queues. 
The work queue, or completion queue depth can become very large depending on which is faster, the enqueuing side, or dequeuing side respectively. This could result in memory overruns further compounded if request or response payloads are large. Stack exchange has programmers who have been bitten by this.

## Flow Control How To

Here is a way to provide flow control with minimal components, that uses constant memory, and can still process an unbounded number of requests. 
Flow Control is inserted in between the service requester and the ExecutorCompletionService. The flow control is not a rate limiter.


## Design
![schematic](https://github.com/smurty/ExecutorCompletionSvcWithFlowControl/blob/master/ExecutorCompletionSvcWithFlowControl.png?raw=true)

### Flow control uses the following elements:
* one `Executor` thread to enqueue work on the work queue (`enqueue(..)`)
* One `Executor` thread to dequeue work from the completion queue (`dequeue(..)`)
* An `Exchanger` (concurrency construct) to manage flow control between the two threads. The queue depth stays between `Q_HI_WATER`, and `Q_LO_WATER`.
* `CompletableFuture`s to run the threads asynchronously and wait for completion.


## Implementation
It is a generic implementation with your choice of:
* input and output payload classes (type parameters), 
* A callback Function that processes a request item into a processed item.
* A consumer to receive processed items

It is a single class with only one entry point `process(..)` as described below:

```
T = request type, U = result type
Stream<T> requestStream is a stream of requests to be processed
Function<T,U> processor is the callback function to process work items
Consumer<U> resultConsumer is a callback with the output (result)

class ExecutorCompletionSvcWithFlowControl<T, U> {

    public void process(Stream<T> requestStream, Function<T,U> processor, Consumer<U> resultConsumer) {

    }
}
```

* The `process(..)` method pulls items from the `requestStream`, processes it using `processor`, and is consumed by the `resultCconsumer`.
`process(..)` returns when all items have been processed (or error'd out). 
This mechanism also ensures that flow control is provided not just at the inbound side but at the outbound side as well.

* The code has been tested for work items of different sizes, different Q depths, different inbound, processing, and outbound rates, either executor thread enqueuer or dequeuer started first.

## Running
* Just download and run as a Java program. Requires Java 8 or higher.
* Running in a custom setup is no different. Just supply the `requestStream`, `processor`, and `resultConsumer`, and that’s it.
* Making it available as a simple java program is deliberate, but it is easy to add your own flare.
