import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

public class GSTBQ_DF {
    private static final Logger LOG = LoggerFactory.getLogger(GSTBQ_DF.class);
    private static String HEADERS = "FirstName,SecondName,Number,City,State,Country";
    public static class FormatForBigquery extends DoFn<String, TableRow> {
        private String[] columnNames = HEADERS.split(",");
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = new TableRow();
            String[] parts = c.element().split(",");
            if (!c.element().contains(HEADERS)) {
                for (int i = 0; i < parts.length; i++) {
                    // No typy conversion at the moment.
                    row.set(columnNames[i], parts[i]);
                }
                c.output(row);
            }
        }
        /** Defines the BigQuery schema used for the output. */
        static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();
//             Currently store all values as String
            fields.add(new TableFieldSchema().setName("FirstName").setType("STRING"));
            fields.add(new TableFieldSchema().setName("SecondName").setType("STRING"));
            fields.add(new TableFieldSchema().setName("Number").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("City").setType("STRING"));
            fields.add(new TableFieldSchema().setName("State").setType("STRING"));
            fields.add(new TableFieldSchema().setName("Country").setType("STRING"));
            return new TableSchema().setFields(fields);

        }
    }
    public static void main(String[] args) throws Throwable {
        DataflowPipelineOptions pipeLineOptions= PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipeLineOptions.setJobName("DFGcsBQ1391");
        pipeLineOptions.setProject("dmgcp-del-108");
        pipeLineOptions.setRegion("us-central1");
        pipeLineOptions.setRunner(DataflowRunner.class);
//        pipeLineOptions.setGcpTempLocation("gs://bkt-dmgcp-freshers-08/temp/");
        pipeLineOptions.setTempLocation("gs://training_freshers/harshul/temp/");
//        pipeLineOptions.setStagingLocation("gs://bkt-dmgcp-freshers-08/staging/");
        pipeLineOptions.setServiceAccount("sa-dataflow@dmgcp-del-108.iam.gserviceaccount.com");
        pipeLineOptions.setUsePublicIps(Boolean.FALSE);
        pipeLineOptions.setNetwork("dm-primary-network");
        pipeLineOptions.setSubnetwork("https://www.googleapis.com/compute/v1/projects/dm-network-host-project/regions/us-central1/subnetworks/subnet-dm-delivery-us-central1");
        Pipeline pipeline = Pipeline.create(pipeLineOptions);

        String sourceFilePath = "gs://training_freshers/harshul/csvByJava.csv";
        boolean isStreaming = false;
        TableReference tableRef = new TableReference();
        // Replace this with your own GCP project id
        tableRef.setProjectId("dmgcp-del-108");
        tableRef.setDatasetId("training_freshers");
        tableRef.setTableId("DF_gtbqJava_1391");


        pipeline.apply("Read CSV File", TextIO.read().from(sourceFilePath))
                .apply("Log messages", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("Processing row: " + c.element());
                        c.output(c.element());
                    }
                })).apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))
                .apply("Write into BigQuery",
                        BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatForBigquery.getSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(isStreaming ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
                                        : BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        pipeline.run().waitUntilFinish();
    }
}