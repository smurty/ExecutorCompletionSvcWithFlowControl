/*
 * Please feel free to use/modify at will.
 * Sudarshan Murty
 * LinkedIn: https://www.linkedin.com/in/sudarshan-murty-b6571b3/
 */
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * ExecutorCompletionService with Flow Control
 * 
 * @author smurty
 *
 * @param <T>
 * @param <U>
 */
class ExecutorCompletionSvcWithFlowControl<T/*input*/, U/*output*/> {

	private int poolSize;
	private int qHiWater;
	private int qLoWater;
	
	AtomicInteger nItemsInQueue = new AtomicInteger(0);
	AtomicInteger nItemsFailed	= new AtomicInteger(0);
	
	/**
	 * Driver program to exercise the impl
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		final int NTHREADS = 10;		// size of thread pool
		final int NWORKITEMS = 1000;	// number of work items to process
		final int WORK_SIM_MS = 20; 	// simulate time taken to process a work item in millis
		
		AtomicInteger nItemsDone = new AtomicInteger(0);
		
		Random rand = new Random(); // to simulate random failures when processing workItems
		
		ExecutorCompletionSvcWithFlowControl<String, String> execCompSvcFlowControl = 
				new ExecutorCompletionSvcWithFlowControl<String, String>(NTHREADS)
						.qLoWaterMark(10)
						.qHiWaterMark(100);		
		/*
		 * input stream to process
		 */
		Stream<String> requestStream = 
				IntStream.rangeClosed(1, NWORKITEMS)
					.mapToObj(String::valueOf);	
		
		/*
		 * Processor (callback), with simulated failures
		 */
		Function<String, String> processor = (workItem) -> {
			try {
				Thread.sleep(WORK_SIM_MS); // simulate work
				if(rand.nextFloat() <= 0.1) // simulate 10% failure
					throw new RuntimeException("workItem ex: arrrgh...");
				
			} catch (InterruptedException e) {
				System.err.println("processor interrupted: " + e.getMessage());
			}
			System.out.println("Processed: " + workItem);
			return workItem;
		};	
		
		/*
		 * Processed output (callback)
		 */
		Consumer<String> resultConsumer = (outboundItem) -> {
			nItemsDone.incrementAndGet();
			System.out.println("DONE: " + outboundItem);			
		};
		
		/*
		 * Process the requestStream, using processor, and consume results using resultConsumer
		 */
		execCompSvcFlowControl.process(requestStream, processor, resultConsumer);		
		
		System.out.printf("nItemsDone[%d]\n", nItemsDone.get());
	}
			
	/**
	 * inputs to service: poolSize, loWater, hiWater
	 * TODO: use a builder pattern + validate request
	 */
	public ExecutorCompletionSvcWithFlowControl(int poolSize) {
		this.poolSize = poolSize;
	}		
	public ExecutorCompletionSvcWithFlowControl<T, U> qLoWaterMark(int loWater) {
		qLoWater = loWater;
		return this;
	}		
	public ExecutorCompletionSvcWithFlowControl<T, U> qHiWaterMark(int hiWater) {
		qHiWater = hiWater;
		return this;
	}
	
	/**
	 * @param poolSize
	 * @return
	 */
	private ExecutorService initExecutorSvc(int poolSize) {
		final ThreadGroup parent = Thread.currentThread().getThreadGroup();

		ExecutorService execSvc = Executors.newFixedThreadPool( poolSize, new ThreadFactory() {	
			int threadNum = 1;				
			@Override
			public Thread newThread(Runnable runnable) {

				return new Thread(
								parent,								// thread group
								runnable, 							// Runnable
								"executorPool-" + String.format("%02d", threadNum++));	// thread name
			}			
		});		
		return execSvc;
	}
	
	/**
	 * @param requestStream
	 * @param processor
	 * @param resultConsumer
	 */
	public void process(Stream<T> requestStream, Function<T,U> processor, Consumer<U> resultConsumer) {
		ExecutorService execSvc = initExecutorSvc(poolSize);			
		ExecutorCompletionService<U> execCompletionSvc = new ExecutorCompletionService<>(execSvc);			
		Exchanger<Integer> exchanger = new Exchanger<>();
		
		/*
		 * Enqueuer
		 */
		ExecutorService enqueuer = Executors.newSingleThreadExecutor();			
		CompletableFuture<Void> cfEnqueuer = CompletableFuture.runAsync( () -> enqueue(execCompletionSvc, requestStream, processor, exchanger), enqueuer );			
		System.out.println("-------------------- cfEnqueuer");
	
		/*
		 * Dequeuer
		 */
		ExecutorService dequeuer = Executors.newSingleThreadExecutor();			
		CompletableFuture<Void> cfDequeuer = CompletableFuture.runAsync( () -> dequeue(execCompletionSvc, resultConsumer, exchanger), dequeuer );			
		System.out.println("-------------------- cfDequeuer");

		/*
		 * Group hug
		 */
		CompletableFuture.allOf(cfEnqueuer, cfDequeuer).join();			
		System.out.println("-------------------- wooHoo");
		
		System.out.printf("nItemsInQueue[%d], nItemsFailed[%d]\n", nItemsInQueue.get(), nItemsFailed.get());
		
		/*
		 * cleanup
		 */
		enqueuer.shutdown();
		dequeuer.shutdown();
		execSvc.shutdown();
	}
	
	/**
	 * @param execCompletionSvc
	 * @param requestStream
	 * @param processor
	 * @param exchanger
	 */
	private void enqueue(ExecutorCompletionService<U> execCompletionSvc, Stream<T> requestStream, Function<T,U> processor, Exchanger<Integer> exchanger) {
		AtomicInteger nItemsThisIter = new AtomicInteger(0);
		
		requestStream.forEach( (workItem) -> {

			try {
				execCompletionSvc.submit( () -> processor.apply(workItem) );
				nItemsThisIter.incrementAndGet();
				int qdepth = nItemsInQueue.incrementAndGet();
				
				if(qdepth >= qHiWater) {
					try {
						System.out.printf("enqueue: sending nItemsThisIter[%d], qdepth[%d]\n", nItemsThisIter.get(), qdepth);
						
						int fromDequeuer = exchanger.exchange(nItemsThisIter.get()); // <------------------
						
						System.out.printf("enqueuer: receiving qdepth[%d]\n", fromDequeuer);
						nItemsThisIter.set(0);
					} catch (InterruptedException e) {
						System.err.println("enqueue: interrupted, " + e.getMessage());
					}
				}
			} catch(Exception e) {
				nItemsFailed.incrementAndGet();
				System.err.printf("failed enqueuing workitem[%s]: %s\n", workItem, e.getMessage());
			}
		});
		
		try {
			nItemsThisIter.getAndUpdate( (i) -> i * -1); // sentinel (last batch is nItemsThisIter as a negative number)
			exchanger.exchange(nItemsThisIter.get());
		} catch (InterruptedException e) {
			System.err.printf("enqueue: interrupted sending sentinel, last batch[%d], %s", nItemsThisIter.get(), e.getMessage());
		}
		System.out.println("------------------------------ enqueue exiting");
	}
	
	/**
	 * @param execCompletionSvc
	 * @param resultConsumer
	 * @param exchanger
	 */
	private void dequeue(ExecutorCompletionService<U> execCompletionSvc, Consumer<U> resultConsumer, Exchanger<Integer> exchanger) {
		boolean allDone = false;
		
		while(true) {
			try {
				U result = execCompletionSvc.take().get();
				resultConsumer.accept(result);
			} catch(ExecutionException | InterruptedException e) {
				nItemsFailed.incrementAndGet();
				System.err.printf("dequeue exception[%s]\n", e.getMessage());
			}
									
			int qdepth = nItemsInQueue.decrementAndGet();
			
			if(qdepth <= 0)
				break; // <------------------
			
			if(!allDone && qdepth <= qLoWater) {
				try {
					System.out.printf("dequeue: sending qdepth[%d]\n", qdepth);
					
					int fromEnqueuer = exchanger.exchange(qdepth);
					allDone = fromEnqueuer <= 0;
					
					System.out.printf("dequeue: receiving nItemsThisIter[%d]\n", fromEnqueuer);
				} catch (InterruptedException e) {
					System.err.printf("dequeue exception[%s]\n", e.getMessage());		
				} 
			}
		} // while{}
		System.out.println("------------------------------ dequeue exiting");
	}		
}

