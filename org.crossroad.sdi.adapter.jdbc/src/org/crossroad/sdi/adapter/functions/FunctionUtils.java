package org.crossroad.sdi.adapter.functions;

public final class FunctionUtils {

	private FunctionUtils() {
	}
	
	public static boolean isAggregateFunction(String x) {

		for (AGGREGATE currentType : AGGREGATE.values()) {
			if (x.equalsIgnoreCase(currentType.toString())) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isConversionFunction(String x) {

		for (CONVERSION currentType : CONVERSION.values()) {
			if (x.equalsIgnoreCase(currentType.toString())) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isMiscFunction(String x) {

		for (MISC currentType : MISC.values()) {
			if (x.equalsIgnoreCase(currentType.toString())) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isNumericFunction(String x) {

		for (NUMERIC currentType : NUMERIC.values()) {
			if (x.equalsIgnoreCase(currentType.toString())) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isStringFunction(String x) {

		for (STRING currentType : STRING.values()) {
			if (x.equalsIgnoreCase(currentType.toString())) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isTimeFunction(String x) {

		for (TIME currentType : TIME.values()) {
			if (x.equalsIgnoreCase(currentType.toString())) {
				return true;
			}
		}
		return false;
	}
	
}
