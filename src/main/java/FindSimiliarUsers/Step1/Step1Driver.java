package FindSimiliarUsers.Step1;

import FindSimiliarUsers.MovieWithScore;
import FindSimiliarUsers.MyPair;
import FindSimiliarUsers.PairCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Step1Driver extends Configured implements Tool {

    public int run(String [] args)
            throws IOException,InterruptedException, ClassNotFoundException {
        if(args.length<4){
            System.err.println("wrong with the input and output file paths");
            return 1;
        }
        String input = args[0];
        String pairOutput = args[1];
        String countOutput = args[2];
        String sortCountOutput = args[3];


        Configuration conf = getConf();
        conf.setBoolean("mapred.output.compress", true);
        //return runFirstStep(conf, input, pairOutput);
        if(runSecondStep(conf, pairOutput, countOutput)==0){
            conf.setBoolean("mapred.output.compress", false);
            return runThirdStep(conf, countOutput, sortCountOutput);
        }else{
            return 1;
        }
    }

    private int runFirstStep(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job pairJob = Job.getInstance(conf, "pairing job");
        pairJob.setJarByClass(getClass());
        //mapper&reducer
        pairJob.setMapperClass(PairMapper.class);
        pairJob.setReducerClass(PairReducer.class);
        pairJob.setNumReduceTasks(100);
        //mapoutput key and value
        pairJob.setMapOutputKeyClass(MovieWithScore.class);
        pairJob.setMapOutputValueClass(IntWritable.class);
        //file paths
        FileInputFormat.addInputPath(pairJob, new Path(input));
        SequenceFileOutputFormat.setOutputPath(pairJob, new Path(output));
        //file format
        pairJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        pairJob.setOutputKeyClass(MyPair.class);
        pairJob.setOutputValueClass(IntWritable.class);

        return pairJob.waitForCompletion(true)?0:1;
    }

    private int runSecondStep(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job countPairJob = Job.getInstance(conf, "count pairs job");
        //set driver
        countPairJob.setJarByClass(Step1Driver.class);
        //mapper&reducer
        countPairJob.setMapperClass(CountPairMapper.class);
        countPairJob.setReducerClass(CountPairReducer.class);
        countPairJob.setNumReduceTasks(100);
        //mapoutput key and value
        countPairJob.setMapOutputKeyClass(MyPair.class);
        countPairJob.setMapOutputValueClass(IntWritable.class);
        //file paths
        SequenceFileInputFormat.addInputPath(countPairJob, new Path(input));
        countPairJob.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileOutputFormat.setOutputPath(countPairJob, new Path(output));
        //fileformat
        countPairJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        countPairJob.setOutputKeyClass(MyPair.class);
        countPairJob.setOutputValueClass(IntWritable.class);

        return countPairJob.waitForCompletion(true)?0:1;
    }

    private int runThirdStep(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job sortJob = Job.getInstance(conf, "sort counted pairs");

        sortJob.setJarByClass(Step1Driver.class);
        //mapper&reducer
        sortJob.setMapperClass(SortCountMapper.class);
        sortJob.setReducerClass(SortCountReducer.class);
        //mapoutput Key and value
        sortJob.setMapOutputKeyClass(PairCount.class);
        sortJob.setMapOutputValueClass(NullWritable.class);
        sortJob.setNumReduceTasks(100);
        //file paths
        SequenceFileInputFormat.addInputPath(sortJob, new Path(input));
        sortJob.setInputFormatClass(SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(sortJob, new Path(output));
        //fileformat
        sortJob.setOutputKeyClass(PairCount.class);
        sortJob.setOutputValueClass(NullWritable.class);
        return sortJob.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Step1Driver(), args);
        System.exit(exitCode);
    }
}
