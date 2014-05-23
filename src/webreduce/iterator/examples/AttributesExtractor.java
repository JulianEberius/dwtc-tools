package webreduce.iterator.examples;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.Document;

import webreduce.data.Dataset;
import webreduce.iterator.WebreduceIterator;

/* extracts the "schema" of all tables in the corpus, assuming the "schema" is in the first line */
public class AttributesExtractor extends WebreduceIterator {

	public static List<String> result = Collections
			.synchronizedList(new ArrayList<String>());

	private static final ThreadLocal<List<String>> localResult = new ThreadLocal<List<String>>() {
		@Override
		protected List<String> initialValue() {
			return new ArrayList<String>();
		}
	};

	public static void main(String[] args) throws IOException,
			InterruptedException {
		if (2 != args.length) {
			System.out
					.println("Usage: java webreduce.tools.AttributesExtractor <path-to-corpus> <output-path>");
			System.exit(1);
		}
		AttributesExtractor cc = new AttributesExtractor();
		cc.iterate(args[0]);

		// transform map to list and sort
		PrintWriter writer = new PrintWriter(args[1], "UTF-8");

		System.out.println("Finished iteration, printing...");
		for (String s : result) {
			writer.println(s);
		}

		writer.close();
	}

	@Override
	protected void process(String key, String value) throws IOException {
		Dataset er = Dataset.fromJson(value);
		String[][] cols = er.getRelation();

		StringBuilder builder = new StringBuilder();
		for (String[] col : cols) {
			builder.append(col[0].replaceAll(",", "").toLowerCase())
					.append(",");
		}
		localResult.get().add(builder.toString());
		
	}

	@Override
	protected void finishProcessFile(File f) throws IOException {
		result.addAll(localResult.get());
	}
}
