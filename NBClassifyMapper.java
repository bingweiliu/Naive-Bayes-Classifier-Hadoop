package naivebayes;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

    /**
     * @author Bingwei Liu
     *
     * 
     */
public class NBClassifyMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
    // value = "word #pos,#neg 1:idPOS 2:idNEG ..."
    // output <docid, freq+modelline+word>
        String[] labels = value.toString().split("\\s+");
        String word = labels[0];
        String modelline = labels[1]+" ";
          
        for (int i = 2; i < labels.length; i++) { 
            String[] doclabel=labels[i].trim().split(":");
            if (doclabel.length>0){
                String freq=doclabel[0]+" ";
                String docid = doclabel[1];
                context.write(new Text(docid), new Text(freq + modelline+word));
            }
        }
     }
}
