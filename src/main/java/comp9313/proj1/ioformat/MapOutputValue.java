package comp9313.proj1.ioformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author pmantha.
 */

public class MapOutputValue implements Writable {
    public IntWritable getWordFrequency() {
        return wordFrequency;
    }

    public void setWordFrequency(IntWritable wordFrequency) {
        this.wordFrequency = wordFrequency;
    }

    public Text getDocId() {
        return docId;
    }

    public void setDocId(Text docId) {
        this.docId = docId;
    }

    private IntWritable wordFrequency;
    private Text docId;

    public MapOutputValue() {
        this(new IntWritable(), new Text());
    }

    public MapOutputValue(IntWritable wordFrequency, Text docId) {
        this.wordFrequency = wordFrequency;
        this.docId = docId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        docId.write(dataOutput);
        wordFrequency.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        docId.readFields(dataInput);
        wordFrequency.readFields(dataInput);
    }
}
