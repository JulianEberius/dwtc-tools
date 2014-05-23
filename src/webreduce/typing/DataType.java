package webreduce.typing;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public enum DataType {
	NONE(-1, "None"), STRING(0, "String"), EMAIL(1, "Email"), URL(2, "URL"), DATETIME(
			3, "Datetime"), DOUBLE(4, "Double"), LONG(6, "Long"), INTEGER(7,
			"Integer"), CURRENCY(8, "Currency");

	public final int Specificity;
	public final String Name;

	private DataType(int value, String name) {
		Specificity = value;
		Name = name;
	}

	private static Map<String, DataType> typeNameMap = ImmutableMap
			.<String, DataType> builder().put("None", DataType.NONE)
			.put("String", DataType.STRING).put("Double", DataType.DOUBLE)
			.put("Long", DataType.LONG).put("Integer", DataType.INTEGER)
			.put("Currency", DataType.CURRENCY).build();

	public static DataType byString(String name) {
		return typeNameMap.get(name);
	}
}
