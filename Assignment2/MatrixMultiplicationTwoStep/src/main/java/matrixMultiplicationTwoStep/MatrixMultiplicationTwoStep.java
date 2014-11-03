package matrixMultiplicationTwoStep;

import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiplicationTwoStep {
	public final static String TOKEN = ",";
	public final static String MATRIX_FILE = "matrixFileTwoPass.txt";
	public final static String TMP_OUTPUT_FILE = "tmpOutputTwoPass.txt";	
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] elements = record.split(TOKEN);
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (elements[0].equals("M")) {
                outputKey.set(elements[2]);
                outputValue.set("M," + elements[1] + TOKEN + elements[3]);
                context.write(outputKey, outputValue);
            } else {
                outputKey.set(elements[1]);
                outputValue.set("N," + elements[2] + TOKEN + elements[3]);
                context.write(outputKey, outputValue);
            }
        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] elements;
            ArrayList<Entry<Integer, Float>> MIndexValueMap = new ArrayList<Entry<Integer, Float>>();
            ArrayList<Entry<Integer, Float>> NIndexValueMap = new ArrayList<Entry<Integer, Float>>();
            for (Text val : values) {
            	elements = val.toString().split(TOKEN);
                if (elements[0].equals("M")) {
                	MIndexValueMap.add(new SimpleEntry<Integer, Float>(Integer.parseInt(elements[1]), Float.parseFloat(elements[2])));
                } else {
                	NIndexValueMap.add(new SimpleEntry<Integer, Float>(Integer.parseInt(elements[1]), Float.parseFloat(elements[2])));
                }
            }
            String i;
            float m_ij;
            String k;
            float n_jk;
            Text outputValue = new Text();
            for (Entry<Integer, Float> m : MIndexValueMap) {
                i = Integer.toString(m.getKey());
                m_ij = m.getValue();                
                for (Entry<Integer, Float> n : NIndexValueMap) {
                    k = Integer.toString(n.getKey());
                    n_jk = n.getValue();
                    outputValue.set(i + TOKEN + k + TOKEN + Float.toString(m_ij*n_jk));
                    context.write(null, outputValue);
                }
            }
        }
    }
	
    /*
     * args[0]: matrix file; args[1]: result file
     */
	public static void main(String[] args)  throws Exception{			
		Configuration conf = new Configuration();
		 
        Job job = new Job(conf, "MatrixMultiplicationTwoPass");
        job.setJarByClass(MatrixMultiplicationTwoStep.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
