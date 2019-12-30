package FindSimiliarUsers.Step3;

import FindSimiliarUsers.MyPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Step3Driver extends Configured implements Tool {

    @Override
    public int run(String [] args)
            throws IOException,InterruptedException, ClassNotFoundException {
        if(args.length<4){
            System.err.println("wrong with the input and output file paths");
            return 1;
        }
        String upIn = args[0];
        String downIn = args[1];
        String simiOut = args[2];
        String res = args[3];


        Configuration conf = new Configuration();

        if(runSimi(conf, upIn, downIn, simiOut)==0)
            return findSimi(conf, simiOut, res);
        return 1;

    }

    private int runSimi(Configuration conf, String input1, String input2, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        /**
         * input1 repersents union Step3Data
         * input2 repersents interaction Step3Data
         * */
        Job simiJob = Job.getInstance(conf, "merge two kinds of Step3Data");
        //set driver
        simiJob.setJarByClass(Step3Driver.class);
        //multiple inputs
        MultipleInputs.addInputPath(simiJob, new Path(input1), TextInputFormat.class, SimiUpMapper.class);
        MultipleInputs.addInputPath(simiJob, new Path(input2), TextInputFormat.class, SimiDownMapper.class);
        simiJob.setReducerClass(SimiReducer.class);
        //mapoutput key and value
        simiJob.setMapOutputKeyClass(MyPair.class);
        simiJob.setMapOutputValueClass(Text.class);

        SequenceFileOutputFormat.setOutputPath(simiJob, new Path(output));
        simiJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        //FileOutputFormat.setOutputPath(simiJob, new Path(output));

        simiJob.setOutputKeyClass(MyPair.class);
        simiJob.setOutputValueClass(FloatWritable.class);

        return simiJob.waitForCompletion(true)?0:1;
    }

    private int findSimi(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "find similar users");

        job.setJarByClass(Step3Driver.class);

        job.setMapperClass(FindSimiMapper.class);
        job.setReducerClass(FindSimiReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String [] args) throws Exception{

        BasicConfigurator.configure(); // config log4j
        int exitCode = ToolRunner.run(new Step3Driver(), args);
        System.exit(exitCode);
    }
}

