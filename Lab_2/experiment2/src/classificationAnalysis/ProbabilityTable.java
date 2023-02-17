package classificationAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class ProbabilityTable {

	private static double[][][] conditionalProbabilityTable = new double[20][2][2];
	private static double[][]   typeProbabilityTable = new double[20][2];
	private static double[]     classProbability = new double[2];
	
	public ProbabilityTable(String pathstr) throws IOException {
		int totalNum;
		int[][][] statisticsTable = new int[20][2][2];
		int[] classTable = new int[2];
		Path centerpath = new Path(pathstr);
		Configuration conf = new Configuration();
		FileSystem fileSystem = centerpath.getFileSystem(conf);
		
		FSDataInputStream fsis = fileSystem.open(centerpath);
		LineReader lineReader = new LineReader(fsis, conf);
		Text line = new Text();
		while(lineReader.readLine(line) > 0) {
			String[] items = line.toString().split("\t")[1].split(",");
			if(items.length == 4) {
				int attribute = Integer.parseInt(items[0]);
				int attrValue = Integer.parseInt(items[1]);
				int type = Integer.parseInt(items[2]);
				statisticsTable[attribute][attrValue][type] = Integer.parseInt(items[3]);
			} else {
				if(Integer.parseInt(items[1]) == 0) classTable[0] = Integer.parseInt(items[2]);
				else classTable[1] = Integer.parseInt(items[2]);
			}
		}
		lineReader.close();
		
		totalNum = classTable[0] + classTable[1];
		
		classProbability[0] = (double)classTable[0] / (double)totalNum;
		classProbability[1] = (double)classTable[1] / (double)totalNum;
		
		for(int i = 0; i < 20; i++) {
			for(int j = 0; j < 2; j++) {
				int count = 0;
				for(int k = 0; k < 2; k++) {
					int temp = statisticsTable[i][j][k];
					count += temp;
					conditionalProbabilityTable[i][j][k] = (double)temp / (double)classTable[k];
				}
				typeProbabilityTable[i][j] = (double)count / (double)totalNum;
			}
		}
		
	}
	
	public double getConditionalProbability(int i, int j, int k) {
		return conditionalProbabilityTable[i][j][k];
	}
	
	public double getTypeProbability(int i, int j) {
		return typeProbabilityTable[i][j];
	}
	
	public double getClassProbability(int k) {
		return classProbability[k];
	}
	
	
}
