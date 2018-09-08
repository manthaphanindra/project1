package comp9313.proj1;


import comp9313.proj1.ioformat.MapOutputKey;
import comp9313.proj1.ioformat.MapOutputValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class Project1 extends Configured implements Tool {
    public enum Documents {
        count
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, MapOutputKey, MapOutputValue > {

        public void map(Object key, Text doc, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(doc.toString());
            if (itr.countTokens() > 1 ) {
                Text docId = new Text(itr.nextToken());
                context.getCounter(Documents.count).increment(1);
                Map<Text, Integer> wordFrequency = new HashMap<Text, Integer>();
                while (itr.hasMoreTokens()) {
                    Text word = new Text(itr.nextToken().toLowerCase());
                    if (wordFrequency.containsKey(word)) {
                        wordFrequency.put(word, wordFrequency.get(word) + 1);
                    } else {
                        wordFrequency.put(word, 1);
                    }
                }

                for (Map.Entry<Text, Integer> entry : wordFrequency.entrySet() ){
                    context.write(new MapOutputKey(entry.getKey(), docId), new MapOutputValue(new IntWritable(entry.getValue()), docId));
                }
            }
        }

    }

    public static class IndexPartitoner extends Partitioner< MapOutputKey, MapOutputValue> {
        @Override
        public int getPartition(MapOutputKey key, MapOutputValue value, int numberOfPartitions){
            return Math.abs(key.getWord().hashCode() % numberOfPartitions);
        }
    }

    public static class IntSumReducer
            extends Reducer<MapOutputKey , MapOutputValue, Text, DoubleWritable> {

        public void reduce(MapOutputKey key, Iterable<MapOutputValue> values,
                           Context context
        ) throws IOException, InterruptedException {
            Set<String> docIds = new HashSet<String>();
            String param = context.getConfiguration().get("getDocumentsCount");
            Long documentsCount=Long.parseLong(param);

            List<MapOutputValue> mapOutputValueList = new ArrayList<MapOutputValue>();
            for (MapOutputValue mapOutput : values) {
                docIds.add(mapOutput.getDocId().toString());
                mapOutputValueList.add(mapOutput);
            }

            for (MapOutputValue mapOutput : mapOutputValueList) {
                int termFrequency = mapOutput.getTermFrequency().get();
                int numDocsWithTerm = docIds.size();

                double weight = termFrequency * Math.log10(documentsCount.doubleValue()/ numDocsWithTerm);
                DoubleWritable weightDW = new DoubleWritable(weight);
                context.write(new Text(String.format("%s\t%s", key.getWord(), key.getDocId())), weightDW);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        fs.delete(outputPath, true);

        Job getTotalDocsJob = Job.getInstance(conf, "Get total number of docs");
        getTotalDocsJob.setJarByClass(Project1.class);
        getTotalDocsJob.setMapperClass(TokenizerMapper.class);
        getTotalDocsJob.setMapOutputKeyClass(MapOutputKey.class);
        getTotalDocsJob.setMapOutputValueClass(MapOutputValue.class);
        FileInputFormat.addInputPath(getTotalDocsJob, inputPath);
        FileOutputFormat.setOutputPath(getTotalDocsJob, outputPath);
        getTotalDocsJob.waitForCompletion(true);
        Counter docCount=getTotalDocsJob.getCounters().findCounter(Documents.count);
        fs.delete(outputPath, true);



        conf.set("getDocumentsCount", Long.toString(docCount.getValue()));
        Job job = Job.getInstance(conf, "Weighted inverted index");
        job.setJarByClass(Project1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setPartitionerClass(IndexPartitoner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(MapOutputKey.class);
        job.setMapOutputValueClass(MapOutputValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Project1(), args);
        System.exit(exitCode);
    }
}
