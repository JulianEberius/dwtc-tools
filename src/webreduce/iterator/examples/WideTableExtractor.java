package webreduce.iterator.examples;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import webreduce.data.Dataset;
import webreduce.iterator.WebreduceIterator;

/* Another example iterator: this one selects only wide tables (more than
 * 8 attributes) from the corpus and writes a new corpus containing only them. 
 */
public class WideTableExtractor extends WebreduceIterator {

	public List<Dataset> list = Collections
			.synchronizedList(new ArrayList<Dataset>());

	@Override
	protected void process(String key, String value) throws IOException {
		Dataset er = Dataset.fromJson(value);
		String[][] cols = er.getRelation();
		Integer numCols = cols.length;

		if (numCols > 8)
			list.add(er);
	}

	@Override
	protected void finishProcessFile(File f) throws IOException {
		// pass
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		if (2 != args.length) {
			System.out
					.println("Usage: java webreduce.tools.WideTableExtractor <path-to-corpus> <output-path>");
			System.exit(1);
		}
		WideTableExtractor cc = new WideTableExtractor();
		String outputPath = args[1];
		cc.iterate(args[0]);
		

		// transform map to list and sort
		List<Dataset> result = cc.list;
		PrintWriter writer = new PrintWriter(outputPath, "UTF-8");

		for (Dataset er : result) {
			writer.println(er.toJson());
		}

		writer.close();
	}

}
