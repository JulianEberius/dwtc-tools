package webreduce.indexing;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

import webreduce.data.Dataset;
import webreduce.iterator.WebreduceIterator;
import webreduce.typing.Types;

import com.google.common.base.Joiner;
import com.google.common.net.InternetDomainName;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

public class Indexer extends WebreduceIterator {

	/* command line flags: */
	protected static final String CORPUS_PATH = "corpusPath";
	protected static final String OUTPUT_PATH = "outputPath";
	
	// index only tables that contained <th> tags in the original HTML
	protected static final String HEADERED_TABLES_ONLY = "headeredTablesOnly";
	
	// store the original data as a stored field in the index
	protected static final String STORE_FULL_RESULT = "storeFullResult";
	// activate preprocessing (analysis of title, terms and, column typing, domain from url extraction)
	protected static final String PREPROCESSING = "preprocessing";

	protected final JSAPResult config;
	protected final Analyzer analyzer = new CustomAnalyzer();
	protected final Joiner joiner;
	protected final IndexWriter writer;
	private Pattern urlSplitPattern = Pattern.compile("[/_-]|%20");

	public Indexer(JSAPResult config) throws IOException {
		SimpleFSDirectory idx_dir = new SimpleFSDirectory(new File(
				config.getString(OUTPUT_PATH)));

		IndexWriterConfig indexConfig = new IndexWriterConfig(
				Version.LUCENE_45, analyzer);
		this.writer = new IndexWriter(idx_dir, indexConfig);
		this.joiner = Joiner.on(" ").skipNulls();
		this.config = config;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, JSAPException {
		JSAP jsap = new JSAP();
		jsap.registerParameter(new Switch(HEADERED_TABLES_ONLY).setLongFlag(
				HEADERED_TABLES_ONLY).setShortFlag('h'));
		jsap.registerParameter(new Switch(PREPROCESSING).setLongFlag(
				PREPROCESSING).setShortFlag('r'));
		jsap.registerParameter(new Switch(STORE_FULL_RESULT).setLongFlag(
				STORE_FULL_RESULT).setShortFlag('s'));
		jsap.registerParameter(new UnflaggedOption(CORPUS_PATH)
				.setRequired(true));
		jsap.registerParameter(new UnflaggedOption(OUTPUT_PATH)
				.setRequired(true));
		JSAPResult config = jsap.parse(args);

		if (!config.success()) {
			System.err.println();
			System.err.println("Usage: java " + Indexer.class.getName());
			System.err.println("                " + jsap.getUsage());
			System.err.println();
			System.exit(1);
		}

		Indexer fi = new Indexer(config);
		fi.iterate(config.getString(CORPUS_PATH));
	}

	@Override
	protected void process(String key, String value) throws IOException {
		// deserialize the json formatted data
		Dataset er = Dataset.fromJson(value);

		String url = er.getUrl();
		if (!(url.contains(".com") || url.contains(".net")
				|| url.contains(".org") || url.contains(".uk"))) {
			return;
		}

		if (config.getBoolean(HEADERED_TABLES_ONLY) && !er.getHasHeader()) {
			return;
		}

		String[][] cols = er.getRelation();
		int numCols = cols.length;
		String[] attributes = new String[numCols];

		for (int j = 0; j < numCols; j++) {
			attributes[j] = cols[j][0];
		}

		int numRows = cols[0].length;

		StringBuilder sb = new StringBuilder();
		for (String[] col : cols) {
			for (int p = 1; p < numRows; p++) {
				sb.append(col[p]);
				sb.append(" ");
			}
		}

		StringBuilder sk = new StringBuilder();
		for (int p = 0; p < numRows; p++) {
			sk.append(cols[0][p]);
			sk.append(" ");
		}

		StringBuilder sa = new StringBuilder();
		for (String s : attributes) {
			sa.append(s);
			sa.append(" ");
		}

		String terms = er.getTerms();
		if (terms == null)
			terms = "";

		String title = er.getTitle();
		if (title == null || title.length() == 0) {
			title = joiner.join(splitURL(er.getUrl()));
			er.setTitle(title);
		}

		String attributesStr = sa.toString();
		String entitiesStr = sb.toString();
		String keysStr = sk.toString();

		Document doc = new Document();
		doc.add(new TextField("url", er.getUrl(), Field.Store.YES));
		doc.add(new TextField("title", title, Field.Store.YES));
		doc.add(new TextField("attributes", attributesStr, Field.Store.NO));
		doc.add(new TextField("entities", entitiesStr, Field.Store.NO));
		doc.add(new TextField("terms", terms, Field.Store.NO));
		doc.add(new TextField("keys", keysStr, Field.Store.NO));
		if (config.getBoolean(STORE_FULL_RESULT)) {
			doc.add(new StoredField("full_result", preprocess(er)));
		}
		writer.addDocument(doc);
	}

	@Override
	protected void finishProcessFile(File f) throws IOException {
	}

	@Override
	protected void close() throws IOException, InterruptedException {
		super.close();
		writer.close();
	}

	protected String preprocess(webreduce.data.Dataset ds) {
		/* example of some useful preprocessing done while indexing */
		try {
			ds.domain = InternetDomainName.from(new URI(ds.url).getHost())
					.topPrivateDomain().toString();
		} catch (Exception e) {
			ds.domain = null;
		}
		ds.termSet = analyze(ds.terms);
		ds.titleTermSet = analyze(ds.title);
		ds.urlTermSet = analyze(joiner.join(splitURL(ds.url)));
		ds.columnTypes = new String[ds.relation.length];
		for (int i = 0; i < ds.relation.length; i++)
			ds.columnTypes[i] = Types.columnType(Arrays.copyOfRange(
					ds.relation[i], 1, ds.relation[i].length)).Name;
		return ds.toJson();
	}

	protected String[] analyze(String s) {
		List<String> result = new ArrayList<String>();
		TokenStream stream;
		try {
			stream = analyzer.tokenStream(null, new StringReader(s));
			stream.reset();
			while (stream.incrementToken())
				result.add(stream.getAttribute(CharTermAttribute.class)
						.toString());
			stream.end();
			stream.close();
		} catch (IOException e) {
			// what to do?
		}
		return result.toArray(new String[] {});
	}

	protected String[] splitURL(String url) {
		URL u;
		String s;
		try {
			u = new URL(url);
			s = u.getPath();
			if (s.endsWith(".htm") || s.endsWith(".html")) {
				s = s.substring(0, s.lastIndexOf("."));
			}
		} catch (MalformedURLException e) {
			s = url;
		}
		return urlSplitPattern.split(s);
	}

}
