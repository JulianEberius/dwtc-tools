package webreduce.iterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/* simple implementation of the ParallelIterator for iterating over lines of uncompressed files */
public abstract class FileLinesIterator extends ParallelIterator {

	private class LineProcessor implements Runnable {
		private final String line;

		public LineProcessor(String line) {
			this.line = line;
		}

		@Override
		public void run() {
			try {
				FileLinesIterator.this.process(line);
			} catch (IOException e) {
				// pass
			}
			finishedItem();
		}
	}

	@Override
	public void iterate(String inPath) throws IOException, InterruptedException {
		File inFile = new File(inPath);
		FileReader fis = new FileReader(inFile);
		BufferedReader input = new BufferedReader(fis);

		String line;
		start();
		while ((line = input.readLine()) != null) {
			executorService.submit(new LineProcessor(line));
		}
		input.close();
		close();
	}

	protected abstract void process(String line) throws IOException;

	@Override
	protected void close() throws IOException {
	}

}
