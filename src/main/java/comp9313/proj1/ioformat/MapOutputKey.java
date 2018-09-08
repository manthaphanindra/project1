package comp9313.proj1.ioformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author pmantha.
 */

public class MapOutputKey implements Writable, WritableComparable<MapOutputKey> {
    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    public Text getDocId() {
        return docId;
    }

    public void setDocId(Text docId) {
        this.docId = docId;
    }

    private Text word;
    private Text docId;

    public MapOutputKey() {
        this(new Text(), new Text());
    }

    public MapOutputKey(Text word, Text docId) {
        this.word = word;
        this.docId = docId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        docId.write(dataOutput);
        word.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        docId.readFields(dataInput);
        word.readFields(dataInput);
    }


    private int compare(String s1, String s2){
        if (s1 == null && s2 != null) {
            return -1;
        } else if (s1 != null && s2 == null) {
            return 1;
        } else if (s1 == null && s2 == null) {
            return 0;
        } else {
            return s1.compareTo(s2);
        }
    }

    @Override
    public int compareTo(MapOutputKey o) {
        int cmp = compare(getWord().toString(), o.getWord().toString());
        if(cmp != 0){
            return cmp;
        }
        return Integer.parseInt(getDocId().toString()) - Integer.parseInt(o.getDocId().toString());
    }

}
