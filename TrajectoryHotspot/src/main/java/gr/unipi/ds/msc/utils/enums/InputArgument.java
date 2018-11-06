package gr.unipi.ds.msc.utils.enums;

/**
 * Helper Enumeration for input arguments. This helps users input data.
 * 
 * @author Aris Paraskevopoulos
 *
 */
public enum InputArgument {
	PATH_TO_INPUT("--input", "-i"),
	PATH_TO_OUTPUT("--output", "-o"),
	CELL_SIZE_IN_DEGREES("--cell-size", "-cs"),
	CELL_HEIGHT_IN_FEET("--cell-height", "-ch"),
	CELL_TIME_SIZE_IN_DAYS("--cell-time", "-ct"),
	NEIGHBOR_DISTANCE("--neighbor-distance", "-nd"),
	TOP_K_PRINTED("--top-k", "-tk"),
	INPUT_DATA_TYPE("--input-data", "-id"),
	ANALYSIS_MODE("--mode", "-m"),
	FLAG("--flag", "-f");
	
	private final String value;
	private final String shortValue;
	
	InputArgument(String value, String shortValue) {
		this.value = value;
		this.shortValue = shortValue;
	}
	
	public String getValue() {
		return value;
	}
	
	public String getShortValue() {
		return shortValue;
	}
	
	/**
	 * Returns an enumeration from a {@link String} value
	 * if that exists in the {@link InputArgument} values or
	 * null. This is used to parse argument's full name.
	 * 
	 * @param value a {@link Sting} value
	 * @return an {@link InputArgument} enumeration or null
	 */
	public static InputArgument fromValue(String value) {
		for (InputArgument inputArgument : InputArgument.values()) {
			if (inputArgument.getValue().equals(value)) {
				return inputArgument;
			}
		}
		return null;
	}
	/**
	 * Returns an enumeration from a {@link String} value
	 * if that exists in the {@link InputArgument} values or
	 * null. This is used to parse argument's short name.
	 * 
	 * @param shortValue a {@link String} value
	 * @return an {@link InputArgument} enumeration or null
	 */
	public static InputArgument fromSortValue(String shortValue) {
		for (InputArgument inputArgument : InputArgument.values()) {
			if (inputArgument.getShortValue().equals(shortValue)) {
				return inputArgument;
			}
		}
		return null;
	}
}
