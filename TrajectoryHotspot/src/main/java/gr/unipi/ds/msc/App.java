package gr.unipi.ds.msc;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import com.typesafe.config.Config;
import gr.unipi.ds.msc.analysis.PDatasetAnalysis;
import gr.unipi.ds.msc.common.AppConfig;
import gr.unipi.ds.msc.utils.broadcast.Params;
import gr.unipi.ds.msc.utils.enums.AnalysisError;
import gr.unipi.ds.msc.utils.enums.InputArgument;
import gr.unipi.ds.msc.utils.exception.AnalysisException;
import org.apache.spark.sql.SparkSession;

/**
 * The class that contains the entry point of the job
 */
public class App {

	public static void main(String[] args) throws Exception {
		long executionStart = System.currentTimeMillis();
		if ((args != null) && (args.length == 1)) {
			AppConfig.initiate(args[0]);
		}
		else {
			AppConfig.initiate(null);
		}
		Config conf = AppConfig.getConfig();
		//Initialize the job params with default values
		/*String pathToInput = "";
		String pathToOutput = "";
		for (int i = 0; i < args.length; i+=2) {
			InputArgument inputArgument = InputArgument.fromValue(args[i]);
			InputArgument shortInputArgument = InputArgument.fromSortValue(args[i]);
			if (inputArgument == null && shortInputArgument == null) {
				throw new AnalysisException(AnalysisError.ARGUMENT_MISSPELLED_ERROR);
			}
			if (inputArgument == InputArgument.PATH_TO_INPUT || shortInputArgument == InputArgument.PATH_TO_INPUT) {
				pathToInput = args[i + 1];
			System.out.println("path to input: " + pathToInput);
			} else if (inputArgument == InputArgument.PATH_TO_OUTPUT || shortInputArgument == InputArgument.PATH_TO_OUTPUT) {
				pathToOutput = args[i + 1];
				System.out.println("path to output: " + pathToOutput);
			}
		}
		int outputNumber = 500;
		long neighborDistance = 1;
		double cellSizeInDegrees = 0.01d;
		double timeStepSize = 1d;
		if (args.length % 2 != 0) {
			throw new AnalysisException(AnalysisError.WRONG_NUMBER_OF_ARGUMENTS_ERROR);
		} else {
			for (int i = 0; i < args.length; i+=2) {
				
				InputArgument inputArgument = InputArgument.fromValue(args[i]);
				InputArgument shortInputArgument = InputArgument.fromSortValue(args[i]);
				if (inputArgument == null && shortInputArgument == null) {
					throw new AnalysisException(AnalysisError.ARGUMENT_MISSPELLED_ERROR);
				}
				if (inputArgument == InputArgument.PATH_TO_INPUT || shortInputArgument == InputArgument.PATH_TO_INPUT) {
					pathToInput = args[i + 1];
					System.out.println("path to input: " + pathToInput);
				} else if (inputArgument == InputArgument.PATH_TO_OUTPUT || shortInputArgument == InputArgument.PATH_TO_OUTPUT) {
					pathToOutput = args[i + 1];
					System.out.println("path to output: " + pathToOutput);
				} else if (inputArgument == InputArgument.CELL_SIZE_IN_DEGREES || shortInputArgument == InputArgument.CELL_SIZE_IN_DEGREES) {
					try {
						cellSizeInDegrees = Double.parseDouble(args[i + 1]);
					} catch (Exception e) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
				} else if (inputArgument == InputArgument.CELL_TIME_SIZE_IN_DAYS || shortInputArgument == InputArgument.CELL_TIME_SIZE_IN_DAYS) {
					try {
						timeStepSize = Double.parseDouble(args[i + 1]);
					} catch (Exception e) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
				} else if (inputArgument == InputArgument.TOP_K_PRINTED || shortInputArgument == InputArgument.TOP_K_PRINTED) {
					try {
						outputNumber = Integer.parseInt(args[i + 1]);
					} catch (Exception e) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
					if (outputNumber < 10) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
				} else if (inputArgument == InputArgument.NEIGHBOR_DISTANCE || shortInputArgument == InputArgument.NEIGHBOR_DISTANCE) {
					try {
						neighborDistance = Long.parseLong(args[i + 1]);
						if (neighborDistance < 1L) {
							throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
						}
					} catch (Exception e) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
				}
			}*/
		double cellSizeInDegrees = conf.getDouble("process.spatialCellSizeInDegrees");
		double timeStepSize = conf.getDouble("process.spatialCellSizeInDegrees");
		int outputNumber = conf.getInt("process.k");
		int neighborDistance = conf.getInt("process.h");
		SparkSession spark = SparkSession.builder().appName("Hotspot analysis").master(conf.getString("spark.master")).getOrCreate();
		String tmpFolder = new Date().getTime() + "";
		Params params = Preprocess.doPreprocess(spark, tmpFolder);
		params.executionStart = executionStart;
		params.requestId = conf.getLong("process.requestId");
		PDatasetAnalysis.analyze(tmpFolder, tmpFolder + ".out", params, spark);
	}
}
