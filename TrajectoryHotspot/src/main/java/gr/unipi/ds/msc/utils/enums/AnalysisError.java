package gr.unipi.ds.msc.utils.enums;

/**
 *  Helper Enumeration for Analysis Errors
 * 
 * @author Aris Paraskevopoulos
 *
 */
public enum AnalysisError {
	ARGUMENT_MISSPELLED_ERROR(0, "Argument Misspelled"),
	INPUT_NOT_SUPPORTED_ERROR(1, "Input Not Supported"),
	PARSE_LINE_ERROR(2, "Parse Line Error"),
	WRONG_NUMBER_OF_ARGUMENTS_ERROR(3, "Wrong Number of Arguments");
	
	private final int code;
	private final String message;
	
	AnalysisError(int code, String message) {
		this.code = code;
		this.message = message;
	}
		
	public int getCode() {
		return code;
	}

	public String getMessage() {
		return message;
	}
}
