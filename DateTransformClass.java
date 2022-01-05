import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringJoiner;

public class DateTransformClass {
    public static class DateFormater extends DoFn<String,String>{
        // Use OutputReceiver.output to emit the output element.
        @ProcessElement
        public void processElement(@Element String str, OutputReceiver<String> out) throws ParseException {
            String[] strColumns = str.split(",");
        String d = strColumns[3];
        SimpleDateFormat dt = new SimpleDateFormat("dd/MM/yy");
            try {
            Date date1=dt.parse(d);
            String nd = new SimpleDateFormat("yyyy-MM-dd").format(date1);
            strColumns[3] = nd;
            StringJoiner sj1 = new StringJoiner(",");
            for(int i = 0; i< strColumns.length;i++)
            {
                sj1.add(strColumns[i]);
            }
            String str2 = sj1.toString();
            out.output(str2);
        } catch (ParseException pe) {
            StringJoiner sj1 = new StringJoiner(",");
            for(int i = 0; i< strColumns.length;i++)
            {
                sj1.add(strColumns[i]);
            }
            String str2 = sj1.toString();
            out.output(str2);
        }
    }

    }
    public static void main(String[] args) throws Throwable {
        DataflowPipelineOptions po = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        po.setJobName("dc1391");
        po.setProject("dmgcp-del-108");
        po.setRegion("us-central1");
        po.setRunner(DataflowRunner.class);
        po.setGcpTempLocation("gs://training_freshers/harshul/temp/");
        po.setTempLocation("gs://training_freshers/harshul/temp/");
        po.setUsePublicIps(Boolean.FALSE);
        po.setServiceAccount("sa-dataflow@dmgcp-del-108.iam.gserviceaccount.com");
        po.setNetwork("dm-primary-network");
        po.setSubnetwork("https://www.googleapis.com/compute/v1/projects/dm-network-host-project/regions/us-central1/subnetworks/subnet-dm-delivery-us-central1");

        Pipeline pipeline = Pipeline.create(po);
        pipeline.apply("reading",TextIO.read().from("gs://training_freshers/harshul/custTransactions.csv"))
                .apply("date conversion",ParDo.of(new DateFormater()))
                .apply("writing",TextIO.write().to("gs://training_freshers/harshul/dfRes/"));
        pipeline.run().waitUntilFinish();
    }
}
