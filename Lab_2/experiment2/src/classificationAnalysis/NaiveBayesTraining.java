package classificationAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaiveBayesTraining {

	static String DATA_PATH = "hdfs://localhost:9000/input/experiment2/训练数据.txt";
	static String Classified_Statistics = "hdfs://localhost:9000/output/experiment2/Classified_Statistics";
	
	public static int dim = 20;			// 数据维度为20
	public static int typeIndex = 20;	// 数据类别下标为20
	
	
	public static final class NaiveBayesTrainingMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		// 生成<属性,属性类别,分类结果>, <1>这样的组合，其中1表示该属性大于0, 0表示该属性小于等于0
		// 类别统计则为<type,分类结果>, <1>这样的形式进行统计
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
			String[] items = value.toString().split(",");
			String type = items[typeIndex];
			for(int i = 0; i < dim; i++) {
				double itemNum = Double.valueOf(items[i]);
				if(itemNum > 0) {
					context.write(new Text(i + "," + "1" + "," + type), new IntWritable(1));
				} else {
					context.write(new Text(i + "," + "0" + "," + type), new IntWritable(1));
				}
			}
			context.write(new Text("type" + "," + type), new IntWritable(1));
	        
		}
		
	}
	
	public static final class NaiveBayesTrainingReducer extends Reducer<Text, IntWritable, Text, Text> {
		
	    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        
	    	int total = 0;
	    	
	    	for(IntWritable value : values) {
	    		total += value.get();
	        }
	    	
	    	String result = key.toString() + "," + total;
	    	
	    	context.write(new Text(""), new Text(result));
	    }
	    
	}
	
	
	public static void main(String[] arg) throws Exception{

		Path outputpath=new Path(Classified_Statistics);
		Path inputpath=new Path(DATA_PATH);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "NaiveBayesTraining");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(NaiveBayesTraining.class);
		job.setMapperClass(NaiveBayesTrainingMapper.class);
		job.setReducerClass(NaiveBayesTrainingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
