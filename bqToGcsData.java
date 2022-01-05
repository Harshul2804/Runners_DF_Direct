import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class bqToGcsData {
        public static byte[] getSHA(String input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return md.digest(input.getBytes(StandardCharsets.UTF_8));
    }

    public static String toHexString(byte[] hash)
    {
        BigInteger number = new BigInteger(1, hash);
        StringBuilder hexString = new StringBuilder(number.toString(16));
        while (hexString.length() < 32)
        {
            hexString.insert(0, '0');
        }
        return hexString.toString();
    }

    public static void main(String[] args) {
        Logger LOG = LoggerFactory.getLogger(bqToGcsData.class);
        String project = "dmgcp-del-108";
        String dataset = "training_freshers";
        String table = "DF_gtbqJava_1391";
        Pipeline pipeline = Pipeline.create();
        pipeline.apply("Read from BigQuery query", BigQueryIO.readTableRows().from(String.format("%s:%s:%s", project, dataset, table)));
//                .apply(TextIO.write().to("gs://training_freshers/harshul/dfRes/"));
         }
    }

