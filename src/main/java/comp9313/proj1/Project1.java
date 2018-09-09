package comp9313.proj1;


import comp9313.proj1.ioformat.MapOutputKey;
import comp9313.proj1.ioformat.MapOutputValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class Project1 extends Configured implements Tool {
    /*
     custom counter to keep track of number of documents.
    */
    public enum Documents {
        count
    }

    /*

    Custom Mapper class, that emits MapOutputKey(Word, DocId), MapOutputKey(DocId, word frequency in the document)
     */
    public static class DoumentTermMapper
            extends Mapper<Object, Text, MapOutputKey, MapOutputValue > {

        public void map(Object key, Text doc, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(doc.toString());
            // Token count should be atleast two - (docId doc)
            if (itr.countTokens() > 1 ) {
                Text docId = new Text(itr.nextToken());
                // Increment counter to get the number of documents, used in the subsequent job.
                context.getCounter(Documents.count).increment(1);

                // Calculate wordFrequency in the doc
                Map<Text, Integer> wordFrequency = new HashMap<Text, Integer>();
                while (itr.hasMoreTokens()) {
                    Text word = new Text(itr.nextToken().toLowerCase());
                    if (wordFrequency.containsKey(word)) {
                        wordFrequency.put(word, wordFrequency.get(word) + 1);
                    } else {
                        wordFrequency.put(word, 1);
                    }
                }

                // emit key(word, docId) -> value(word frequency in document, docId)
                for (Map.Entry<Text, Integer> entry : wordFrequency.entrySet() ){
                    context.write(new MapOutputKey(entry.getKey(), docId), new MapOutputValue(new IntWritable(entry.getValue()), docId));
                }
            }
        }

    }

    /*
    Partition the keys based on the word default hashcode.
     */
    public static class DocumentMapOutputKeyPartitoner extends Partitioner< MapOutputKey, MapOutputValue> {
        @Override
        public int getPartition(MapOutputKey key, MapOutputValue value, int numberOfPartitions){
            return Math.abs(key.getWord().hashCode() % numberOfPartitions);
        }
    }

    /* Group and sort keys based on the word */
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(MapOutputKey.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            return ((MapOutputKey)w1).getWord ().compareTo (((MapOutputKey)w2).getWord ());
        }

    }

    public static class WeightedInvertedIndexReducer
            extends Reducer<MapOutputKey , MapOutputValue, Text, DoubleWritable> {

        public void reduce(MapOutputKey key, Iterable<MapOutputValue> values,
                           Context context
        ) throws IOException, InterruptedException {

            List<MapOutputValue> mapOutputValueList = new ArrayList<MapOutputValue>();
            Set<String> docIds = new HashSet<String>();

            for (MapOutputValue mapOutput : values) {
                docIds.add(mapOutput.getDocId().toString()); // get unique docIds for the word.
                mapOutputValueList.add(WritableUtils.clone(mapOutput, context.getConfiguration()));
            }

            Long documentsCount=Long.parseLong(context.getConfiguration().get("getDocumentsCount"));
            for (MapOutputValue mapOutput : mapOutputValueList) {
                int termFrequency = mapOutput.getWordFrequency().get();
                int numDocsWithTerm = docIds.size();
                double weight = termFrequency * Math.log10(documentsCount.doubleValue()/ numDocsWithTerm);
                context.write(new Text(String.format("%s\t%s", key.getWord(), mapOutput.getDocId())), new DoubleWritable(weight));
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

        // Get number of documents using hadoop mapper job
        Counter docCount = getDocumentCounter(conf, inputPath, outputPath);

        // clean output path as we got the counter
        fs.delete(outputPath, true);

        // update config with document count, and output separator.
        conf.set("getDocumentsCount", Long.toString(docCount.getValue()));
        conf.set("mapred.textoutputformat.separator", ",");

        // run inverted job
        Job weightedInvertedIndexJob = Job.getInstance(conf, "Weighted inverted index");
        weightedInvertedIndexJob.setJarByClass(Project1.class);
        weightedInvertedIndexJob.setMapperClass(DoumentTermMapper.class);
        weightedInvertedIndexJob.setPartitionerClass(DocumentMapOutputKeyPartitoner.class);
        weightedInvertedIndexJob.setReducerClass(WeightedInvertedIndexReducer.class);
        weightedInvertedIndexJob.setMapOutputKeyClass(MapOutputKey.class);
        weightedInvertedIndexJob.setMapOutputValueClass(MapOutputValue.class);
        weightedInvertedIndexJob.setGroupingComparatorClass (GroupComparator.class);
        weightedInvertedIndexJob.setNumReduceTasks(Integer.parseInt(args[2]));
        weightedInvertedIndexJob.setOutputKeyClass(Text.class);
        weightedInvertedIndexJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(weightedInvertedIndexJob, inputPath);
        FileOutputFormat.setOutputPath(weightedInvertedIndexJob, outputPath);

        // wait for inverted job
        return weightedInvertedIndexJob.waitForCompletion(true) ? 0 : 1;
    }

    private Counter getDocumentCounter(Configuration conf, Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job getTotalDocsJob = Job.getInstance(conf, "Get total number of docs");
        getTotalDocsJob.setJarByClass(Project1.class);
        getTotalDocsJob.setMapperClass(DoumentTermMapper.class);
        getTotalDocsJob.setMapOutputKeyClass(MapOutputKey.class);
        getTotalDocsJob.setMapOutputValueClass(MapOutputValue.class);
        FileInputFormat.addInputPath(getTotalDocsJob, inputPath);
        FileOutputFormat.setOutputPath(getTotalDocsJob, outputPath);
        getTotalDocsJob.waitForCompletion(true);
        return getTotalDocsJob.getCounters().findCounter(Documents.count);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Project1(), args);
        System.exit(exitCode);
    }
}
