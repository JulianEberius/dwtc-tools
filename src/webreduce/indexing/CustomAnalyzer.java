package webreduce.indexing;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.util.Version;

/* Just an example custom Lucene analyzer */ 
public class CustomAnalyzer extends Analyzer {

	@Override
	protected TokenStreamComponents createComponents(final String fieldName,
			final Reader reader) {
		final ClassicTokenizer src = new ClassicTokenizer(Version.LUCENE_45,
				reader);
		src.setMaxTokenLength(255);
		TokenStream filter = new ClassicFilter(src);
		filter = new LowerCaseFilter(Version.LUCENE_45, filter);
		filter = new StopFilter(Version.LUCENE_45, filter,
				StopAnalyzer.ENGLISH_STOP_WORDS_SET);
		filter = new ASCIIFoldingFilter(filter);
		return new TokenStreamComponents(src, filter) {
			@Override
			protected void setReader(final Reader reader) throws IOException {
				src.setMaxTokenLength(255);
				super.setReader(reader);
			}
		};
	}
}