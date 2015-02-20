package webreduce.cleaning;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.google.common.base.Joiner;

public class Analysis {

	private static Joiner joiner = Joiner.on(" ");;

	public static List<String> tokenize(Analyzer analyzer, String keywords) {

		List<String> result = new ArrayList<String>();
		if (keywords.length() == 0)
			return result;
		

		try {
			TokenStream stream = analyzer.tokenStream(null, new StringReader(
					keywords));
			stream.reset();
			while (stream.incrementToken()) {
				result.add(stream.getAttribute(CharTermAttribute.class)
						.toString());
			}
			stream.end();
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static String analyze(Analyzer analyzer, String keywords) {
		return joiner.join(tokenize(analyzer, keywords));
	}
}