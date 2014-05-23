package webreduce.typing;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

/* Heuristics for typing table columns (e.g. recognizing numeric columns) */
public class Types {

	static double TYPE_MAJORITY_THRESHOLD = 0.40;
	static double NUMERIC_PERCENTAGE = 0.6;

	static Pattern isCurrency = Pattern
			.compile("-?\\s*([$£₤]\\s*[\\d,]+(\\.\\d+)?)|([\\d,]+(\\.\\d+)?\\s*[€])");
	static Pattern isInt = Pattern
			.compile("(^|[^\\d])-?[\\d,]{1,9}(\\s)?(%)?($|[^\\d])");
	static Pattern isLong = Pattern.compile("-?[\\d,]+");
	static Pattern isDouble = Pattern.compile("-?[\\d,]+\\.\\d+(\\s)?(%)?");

	static DataType typeOf(String s) {
		Matcher m;
		double size = s.length();
		if (s.equals(""))
			return DataType.NONE;
		m = isCurrency.matcher(s);
		if (m.find() && ((m.end() - m.start()) / size > NUMERIC_PERCENTAGE))
			return DataType.CURRENCY;
		m = isDouble.matcher(s);
		if (m.find() && ((m.end() - m.start()) / size > NUMERIC_PERCENTAGE))
			return DataType.DOUBLE;
		m = isInt.matcher(s);
		if (m.find() && ((m.end() - m.start()) / size > NUMERIC_PERCENTAGE))
			return DataType.INTEGER;
		m = isLong.matcher(s);
		if (m.find() && ((m.end() - m.start()) / size > NUMERIC_PERCENTAGE))
			return DataType.LONG;
		return DataType.STRING;
	}

	static Map<DataType, Integer> typeCounts(String[] column) {
		Map<DataType, Integer> result = new HashMap<DataType, Integer>();
		for (String s : column) {
			DataType t = typeOf(s);
			if (result.containsKey(t))
				result.put(t, result.get(t) + 1);
			else
				result.put(t, 1);
		}
		return result;
	}

	static public DataType columnType(String[] column) {
		Map<DataType, Integer> typeCounts = typeCounts(column);
		typeCounts.remove(DataType.NONE);
		if (typeCounts.size() == 1)
			return typeCounts.keySet().iterator().next();
		else if (typeCounts.size() == 0)
			return DataType.NONE;

		List<Entry<DataType, Integer>> counts = Lists.newArrayList(typeCounts
				.entrySet());
		Collections.sort(counts, new Comparator<Entry<DataType, Integer>>() {
			@Override
			public int compare(Entry<DataType, Integer> o1,
					Entry<DataType, Integer> o2) {
				return Integer.valueOf(o1.getValue()).compareTo(
						Integer.valueOf(o2.getValue()));
			}
		});

		Entry<DataType, Integer> mostFrequent = counts.get(counts.size() - 1);
		Entry<DataType, Integer> secondMostFrequent = counts
				.get(counts.size() - 2);
		float mostFrequentPercentage = mostFrequent.getValue()
				/ (float) column.length;
		float secondMostFrequentPercentage = secondMostFrequent.getValue()
				/ (float) column.length;
		if (mostFrequentPercentage - secondMostFrequentPercentage > TYPE_MAJORITY_THRESHOLD)
			return mostFrequent.getKey();

		int countNumeric = 0;
		for (Entry<DataType, Integer> e : counts) {
			if (e.getKey().Specificity >= DataType.DOUBLE.Specificity)
				countNumeric += e.getValue();
		}
		if ((countNumeric / (float) column.length) > 0.9) {
			// sort by specificity
			Collections.sort(counts,
					new Comparator<Entry<DataType, Integer>>() {
						@Override
						public int compare(Entry<DataType, Integer> o1,
								Entry<DataType, Integer> o2) {
							return Integer
									.valueOf(o1.getKey().Specificity)
									.compareTo(
											Integer.valueOf(o2.getKey().Specificity));
						}
					});
			for (Entry<DataType, Integer> e : counts) {
				if (e.getKey().Specificity >= DataType.DOUBLE.Specificity)
					return e.getKey();
			}
		}
		return DataType.STRING;
	}

	public static void main(String[] args) {
		System.out.println(columnType(new String[] { "0.043", "123124124",
				"21343124124124€", "2131233213123123" }));
		System.out.println(columnType(new String[] { "324", "123124124",
				"21343", "213123434313123123" }));
		System.out.println(typeCounts(new String[] { "324", "123124124",
				"21343", "213123434313123123", "34eddd" }));
	}
}
