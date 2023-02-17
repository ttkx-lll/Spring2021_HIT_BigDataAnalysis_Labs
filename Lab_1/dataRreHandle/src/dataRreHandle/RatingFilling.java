package dataRreHandle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingFilling {
	
	static String INPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_incomeFilled/part-r-00000"; 
	static String OUTPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_Done";
	
	public static final class RatingFillingMapper extends Mapper<Object, Text, Text, Text> {

		private final List<Double[]> xList = new ArrayList<Double[]>();
		private final List<Double> yList = new ArrayList<Double>();
		public static double[] linear;
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
			String line = value.toString();
	        String[] values = line.split("\\|");
	        
	        String rating = values[6];
	        
	        if(!rating.contains("?")) {
	        	Double longitude = Double.valueOf(values[1]);
		        Double latitude = Double.valueOf(values[2]);
		        Double altitude = Double.valueOf(values[3]);
		        Double income = Double.valueOf(values[11]);
		        Double[] xValues = new Double[4];
		        xValues[0] = longitude;
		        xValues[1] = latitude;
		        xValues[2] = altitude;
		        xValues[3] = income;
		        xList.add(xValues);
		        yList.add(Double.valueOf(rating));
	        }
	        
	        context.write(value, new Text(""));
		}
		
		@Override
		protected void cleanup(Context context)	throws IOException, InterruptedException {
			double[] y = new double[yList.size()];
			int count = 0;
			for(double value : yList) {
				y[count++] = value;
			}
			double[][] x = new double[xList.size()][4];
			count = 0;
			for(Double[] values : xList) {
				for(int i = 0; i < 4; i++) {
					x[count][i] = values[i];
				}
				count++;
			}
			OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
			regression.newSampleData(y, x);
			linear = regression.estimateRegressionParameters();
		}
		
	}
	
	public static final class RatingFillingReducer extends Reducer<Text, Text, Text, Text> {
	    
		public static double[] linear = RatingFillingMapper.linear;
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        for(Text value : values) {
	        	String line = key.toString();
		        String[] temp = line.split("\\|");
		        String rating = temp[6];
		        if(rating.contains("?")) {
		        	double longitude = Double.valueOf(temp[1]);
		        	double latitude = Double.valueOf(temp[2]);
		        	double altitude = Double.valueOf(temp[3]);
		        	double income = Double.valueOf(temp[11]);
		        	double ratingToFill = longitude*linear[1] + latitude*linear[2] + altitude*linear[3] + income*linear[4] + linear[0];
		        	line = line.replace(rating, String.valueOf(ratingToFill));
		        	context.write(new Text(line), value);
		        } else {
		        	context.write(key, value);
		        }
		        
	        }
	    }
	    
	}
	
	public static void main(String[] arg) throws Exception{
		
		Path outputpath=new Path(OUTPUT_PATH);
		Path inputpath=new Path(INPUT_PATH);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "rating_filling");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(RatingFilling.class);
		job.setMapperClass(RatingFillingMapper.class);
		job.setReducerClass(RatingFillingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}
	
}