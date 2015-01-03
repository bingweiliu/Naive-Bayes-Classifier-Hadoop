package naivebayes;

import java.io.IOException;
import java.lang.Math;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NBClassifyReducer  extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
                throws InterruptedException, IOException {
    // each time this reducer is called, we have a new document with stats
    // values are the stats all words in this documents
      // format freq+modelline+word
      double pos_prob=0;
      double neg_prob=0;
      long model_pos_num = context.getConfiguration().getLong(NBController.TOTAL_WORDS_POS,100);
      long model_neg_num = context.getConfiguration().getLong(NBController.TOTAL_WORDS_NEG, 100);
      long model_vacabulary_size = context.getConfiguration().getLong(NBController.VOCABULARY_SIZE_UNION, 100);

      context.getCounter(NBController.NB_COUNTERS.TOTAL_REVIEWS).increment(1);

      for (Text value : values) {
        String[] nums = value.toString().split("\\s+");
        int freq = Integer.valueOf(nums[0]);
        String[] model_freqs= nums[1].split(",");
        int model_pos_freq = Integer.valueOf(model_freqs[0]);
        int model_neg_freq = Integer.valueOf(model_freqs[1]);
        pos_prob += freq*(Math.log(model_pos_freq+1)-Math.log(model_pos_num + model_vacabulary_size));
        neg_prob += freq*(Math.log(model_neg_freq+1)-Math.log(model_neg_num + model_vacabulary_size));          
      }// for 
      String docid = key.toString();
      String polarity ="";
      String predict = "";

      if (docid.contains("POS")) {
        polarity = "POS";
      } else {
        polarity = "NEG";
      }
      if (pos_prob > neg_prob) {
        predict = "POS";
      } else {
        predict = "NEG";
      }
      if (predict.equals(polarity)) {
        context.getCounter(NBController.NB_COUNTERS.CORRECT_PREDICT).increment(1);
      } 
      StringBuilder out = new StringBuilder();
      out.append(model_pos_num + "\t" + model_neg_num + "\t" + model_vacabulary_size);
      out.append("\t" + pos_prob + "\t" + neg_prob + "\t" + predict);
      // output <docid, predict>
      context.write(key, new Text(out.toString()));
    }
}
 
