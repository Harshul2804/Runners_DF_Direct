import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.util.Arrays;
import java.util.List;

public class LocalToGCS {
    public static void main(String[] args) {
        //dataflow pipeline to write text file to
        DataflowPipelineOptions pipeLineOptions= PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipeLineOptions.setJobName("csvJob1391");
        pipeLineOptions.setProject("dmgcp-del-108");
        pipeLineOptions.setRegion("us-central1");
        pipeLineOptions.setRunner(DataflowRunner.class);
        pipeLineOptions.setGcpTempLocation("gs://training_freshers/harshul/temp/");
        pipeLineOptions.setServiceAccount("sa-dataflow@dmgcp-del-108.iam.gserviceaccount.com");
        pipeLineOptions.setNetwork("dm-primary-network");
        pipeLineOptions.setSubnetwork("https://www.googleapis.com/compute/v1/projects/dm-network-host-project/regions/us-central1/subnetworks/subnet-dm-delivery-us-central1");
        Pipeline pipeline = Pipeline.create(pipeLineOptions);
//        final List<String> input = Arrays.asList("I","am","in","a","good","SSE","Team");
        //from file
        pipeline.apply(TextIO.read().from("D://parquetToCsv.csv")).apply(TextIO.write().to("gs://training_freshers/harshul/dfRes/"));
        //from list
//        pipeline.apply(Create.of(input)).apply(TextIO.write().to("gs://training_freshers/harshul/dfRes/").withSuffix(".txt"));

        pipeline.run().waitUntilFinish();
    }
}
