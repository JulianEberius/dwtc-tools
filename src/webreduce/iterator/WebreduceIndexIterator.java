package webreduce.iterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.NIOFSDirectory;

/*
 * Specialized version of the ParallelIterator that iterates a Lucene index instead
 * of a raw DWTC dataset 
 */
public abstract class WebreduceIndexIterator extends ParallelIterator {

	protected class Job {
		public int start;
		public int end;

		private Job(int start, int end) {
			this.start = start;
			this.end = end;
		}
	}

	@Override
	public void iterate(String inPath) throws IOException, InterruptedException {
		File inDir = new File(inPath);
		IndexSearcher searcher = new IndexSearcher(
				DirectoryReader.open(new NIOFSDirectory(inDir)));
		final IndexReader reader = searcher.getIndexReader();

		System.out.println("Iterating " + inDir);

		// create jobs
		int jobSize = (int) Math.ceil((double) reader.maxDoc() / maxThreads);
		int x = 0;

		List<Job> jobs = new ArrayList<Job>(maxThreads);
		for (int i = 0; i < maxThreads - 1; i++) {
			jobs.add(new Job(x, x + jobSize));
			x += jobSize;
		}
		// final job
		jobs.add(new Job(x, reader.maxDoc()));

		start();
		for (final Job job : jobs) {
			executorService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						for (int i = job.start; i < job.end; i++) {
							// do the actual processing
							Document doc = reader.document(i);
							process(i, doc);
							finishedItem();
						}
						finishProcessFile(job);

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
		}
		close();
	}

	protected abstract void process(int docId, Document value)
			throws IOException;

	protected abstract void finishProcessFile(Job job) throws IOException;

}
