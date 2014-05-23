package webreduce.iterator;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/* Abstract base class for that enabled parallel iteration over a set of files */
public abstract class ParallelIterator {

	protected ExecutorService executorService;
	protected long startTime;
	protected AtomicInteger i;
	protected AtomicInteger corruptFiles;
	protected int maxThreads;

	public ParallelIterator(int maxThreads) {
		this.maxThreads = maxThreads;
		this.executorService = new ThreadPoolExecutor(maxThreads, // core
				maxThreads, // maximum thread pool size
				1, // time to wait before resizing pool
				TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(maxThreads,
						true), new ThreadPoolExecutor.CallerRunsPolicy());

		this.i = new AtomicInteger(0);
		this.corruptFiles = new AtomicInteger(0);
	}

	public ParallelIterator() {
		this(4);

	}

	public abstract void iterate(String inPath) throws IOException,
			InterruptedException;

	protected void start() {
		startTime = System.currentTimeMillis();
	}

	protected void reportCorrputFile() {
		this.corruptFiles.incrementAndGet();
	}

	protected void finishedItem() {
		int iVal = i.addAndGet(1);
		if (iVal % 10000 == 0) {
			double speed = iVal
					/ ((System.currentTimeMillis() - startTime) / 1000.0);
			System.out.println("processed " + i + " lines, speed: " + speed
					+ " l per second");
		}
	}

	protected void close() throws IOException, InterruptedException {
		System.out.println("Waiting for tasks to finish... will close.");
		executorService.shutdown();
		executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		System.out.println("Tasks finished");
		System.out.println("There were " + corruptFiles.get()
				+ " corrupt files.");
	}

}
