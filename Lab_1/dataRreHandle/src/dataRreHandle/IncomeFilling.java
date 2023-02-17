package dataRreHandle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IncomeFilling {
	
	static String INPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_Filter3/part-r-00000"; 
	static String OUTPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_incomeFilled";
	
	public static final class IncomeFillingMapper extends Mapper<Object, Text, Text, Text> {
		
		public static Map<String, List<Double>> map = new HashMap<String, List<Double>>();
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
			String line = value.toString();
	        String[] values = line.split("\\|");
	        String nationality = values[9];
	        String career = values[10];
	        String income = values[11];
	        String nationality_career = nationality + career;
	        
	        if(!income.contains("?")) {
	        	if(map.containsKey(nationality_career)) {
	        		List<Double> list = map.get(nationality_career);
	        		double sum = (double)list.get(0) + Double.valueOf(income);
	        		double num = (double)list.get(1) + 1.0;
	        		list.set(0, sum);
	        		list.set(1, num);
	        	} else {
	        		List<Double> list = new ArrayList<Double>();
	        		list.add(Double.valueOf(income));
	        		list.add(1.0);
	        		map.put(nationality_career, list);
	        	}
	        }
	        
	        context.write(value, new Text(""));
		}
		
	}
	
	public static final class IncomeFillingReducer extends Reducer<Text, Text, Text, Text> {
		public static Map<String, List<Double>> map = IncomeFillingMapper.map;
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        for(Text value : values) {
	        	String line = key.toString();
		        String[] temp = line.split("\\|");
		        String income = temp[11];
		        if(income.contains("?")) {
		        	String nationality = temp[9];
		        	String career = temp[10];
		        	String nationality_career = nationality + career;
		        	if(map.containsKey(nationality_career)) {
		        		List<Double> list = map.get(nationality_career);
		        		double incomeToFill = list.get(0)/list.get(1);
		        		line = line.replace(income, String.valueOf(incomeToFill));
		        	} else {
		        		line = line.replace(income, String.valueOf(3000));
		        	}
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
		Job job=Job.getInstance(conf, "income_filling");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(IncomeFilling.class);
		job.setMapperClass(IncomeFillingMapper.class);
		job.setReducerClass(IncomeFillingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}
	
}