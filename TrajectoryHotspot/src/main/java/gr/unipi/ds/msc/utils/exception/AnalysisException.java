package gr.unipi.ds.msc.utils.exception;

import gr.unipi.ds.msc.utils.enums.AnalysisError;

/**
 * Custom Exception class
 * 
 * @author Aris Paraskevopoulos
 *
 */
public class AnalysisException extends Exception {
	private static final long serialVersionUID = 6861461985551631396L;
	
	public AnalysisException(AnalysisError analysisError) {
		super(analysisError.getMessage());
	}
	
	public AnalysisException(String message) {
		super(message);
	}
}
