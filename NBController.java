package com.ift.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NBController extends Configured implements Tool {
 
    static enum NB_COUNTERS {
        TOTAL_WORDS_POS,
        TOTAL_WORDS_NEG,
        VOCABULARY_SIZE_POS,
        VOCABULARY_SIZE_NEG,
        VOCABULARY_SIZE_UNION,
        TOTAL_REVIEWS,
        CORRECT_PREDICT,
        ACCURACY
    }
    public static final String TOTAL_WORDS_POS = "total.words.pos";
    public static final String TOTAL_WORDS_NEG = "total.words.neg";
    public static final String  VOCABULARY_SIZE_UNION = "vocabulary.size.union";
 
    /**
     * Deletes the specified path from HDFS 
     * @param conf
     * @param path
     */
    public static void delete(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        } 
    }
    
    @Override
    public int run(String [] args) throws Exception {
        Configuration conf = getConf();
        Configuration classifyConf = new Configuration();
        String name = conf.get("result");
        
        Path traindata = new Path(conf.get("train"));
        Path testdata = new Path(conf.get("test"));
        Path output = new Path(conf.get("output"));

        int numReducers = conf.getInt("reducers", 10);
        Path model = new Path(output.getParent(), "model");

        
        // Job 1: Use training data to build model.
        NBController.delete(conf, model);
        Job trainWordJob = new Job(conf, name + " training");
        trainWordJob.setJarByClass(NBController.class);
        trainWordJob.setNumReduceTasks(numReducers);
        trainWordJob.setMapperClass(NBTrainingMapper.class);
        trainWordJob.setReducerClass(NBTrainingReducer.class);
        
        trainWordJob.setInputFormatClass(TextInputFormat.class);
        trainWordJob.setOutputFormatClass(TextOutputFormat.class);
        
        trainWordJob.setMapOutputKeyClass(Text.class);
        trainWordJob.setMapOutputValueClass(Text.class);
        trainWordJob.setOutputKeyClass(Text.class);
        trainWordJob.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(trainWordJob, traindata);
        FileOutputFormat.setOutputPath(trainWordJob, model);
        
        if (!trainWordJob.waitForCompletion(true)) {
            System.err.println("ERROR: Word training failed!");
            return 1;
        }
        
        // Job 2: Merge the model with test reviews for the classify
        Path testprepare = new Path("testprepare");
        NBController.delete(conf, testprepare);
        Job testPrepareJob = new Job(conf, name + " test prepare");
        testPrepareJob.setJarByClass(NBController.class);
        testPrepareJob.setNumReduceTasks(numReducers);
        testPrepareJob.setMapperClass(NBTestPrepareMapper.class);
        testPrepareJob.setReducerClass(NBTestPrepareReducer.class);
        
        testPrepareJob.setInputFormatClass(TextInputFormat.class);
        testPrepareJob.setOutputFormatClass(TextOutputFormat.class);
        
        testPrepareJob.setMapOutputKeyClass(Text.class);
        testPrepareJob.setMapOutputValueClass(Text.class);
        testPrepareJob.setOutputKeyClass(Text.class);
        testPrepareJob.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(testPrepareJob, testdata);
        FileInputFormat.addInputPath(testPrepareJob, model);
        FileOutputFormat.setOutputPath(testPrepareJob, testprepare);
        
        if (!testPrepareJob.waitForCompletion(true)) {
            System.err.println("ERROR: Test Prepare failed!");
            return 1;
        }
        
        // Job 3: Classify
        NBController.delete(classifyConf, output);
        // Pass parameters from trainwordjob
        conf.setLong(TOTAL_WORDS_POS,
        		trainWordJob.getCounters().findCounter(NBController.NB_COUNTERS.TOTAL_WORDS_POS).getValue());
        conf.setLong(TOTAL_WORDS_NEG,
        		trainWordJob.getCounters().findCounter(NBController.NB_COUNTERS.TOTAL_WORDS_NEG).getValue());
        conf.setLong(VOCABULARY_SIZE_UNION,
        		trainWordJob.getCounters().findCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE_UNION).getValue());
        Job ClassifyJob = new Job(conf, name + " classify");
        ClassifyJob.setJarByClass(NBController.class);
        ClassifyJob.setNumReduceTasks(numReducers);
        ClassifyJob.setMapperClass(NBClassifyMapper.class);
        ClassifyJob.setReducerClass(NBClassifyReducer.class);
        
        ClassifyJob.setInputFormatClass(TextInputFormat.class);
        ClassifyJob.setOutputFormatClass(TextOutputFormat.class);
        
        ClassifyJob.setMapOutputKeyClass(Text.class);
        ClassifyJob.setMapOutputValueClass(Text.class);
        ClassifyJob.setOutputKeyClass(Text.class);
        ClassifyJob.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(ClassifyJob, testprepare);
        FileOutputFormat.setOutputPath(ClassifyJob, output);
        
        if (!ClassifyJob.waitForCompletion(true)) {
            System.err.println("ERROR: Classify failed!");
            return 1;
        }
        
        // write result
        FileSystem fs = FileSystem.get(conf);       
        Path resultdir= new Path("result");
        if (!fs.exists(resultdir)) {
        	fs.mkdirs(resultdir);
        } 
        Path outFile = new Path("result/"+name+".txt"); 
        NBController.delete(conf, outFile);
        FSDataOutputStream out = fs.create(outFile);
        String result="";
        long total_reviews = ClassifyJob.getCounters().findCounter(NBController.NB_COUNTERS.TOTAL_REVIEWS).getValue();
        long correct_predicts = ClassifyJob.getCounters().findCounter(NBController.NB_COUNTERS.CORRECT_PREDICT).getValue();
        double accuracy = (double) correct_predicts/total_reviews;
        result += name + "," + String.valueOf(correct_predicts) + "," 
        		+ String.valueOf(total_reviews) + ","+String.valueOf(accuracy)+"\n";
        out.writeChars(result);
        out.close();

        return 0;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NBController(), args);
        System.exit(exitCode);
    }
}