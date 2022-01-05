import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

public class GcsToBq {
    private static final Logger LOG = LoggerFactory.getLogger(GcsToBq.class);
    private static String HEADERS = "FirstName,LastName,PhoneNo,City,State,Country";
    public static class FormatForBigquery extends DoFn<String, TableRow> {
        private String[] columnNames = HEADERS.split(",");
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = new TableRow();
            String[] parts = c.element().split(",");
            if (!c.element().contains(HEADERS)) {
                for (int i = 0; i < parts.length; i++) {

                    row.set(columnNames[i], parts[i]);
                }
                c.output(row);
            }
        }
        /** Defines the BigQuery schema used for the output. */
        static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();
            // Currently store all values as String
            fields.add(new TableFieldSchema().setName("FirstName").setType("STRING"));
            fields.add(new TableFieldSchema().setName("LastName").setType("STRING"));
            fields.add(new TableFieldSchema().setName("PhoneNo").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("City").setType("STRING"));
            fields.add(new TableFieldSchema().setName("State").setType("STRING"));
            fields.add(new TableFieldSchema().setName("Country").setType("STRING"));
            return new TableSchema().setFields(fields);
        }
    }
    public static void main(String[] args) throws Throwable {
        DataflowPipelineOptions po = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        po.setJobName("gcsToBq1391");
        po.setProject("dmgcp-training-q4-2021");
        po.setRegion("us-central1");
        po.setRunner(DataflowRunner.class);
        po.setGcpTempLocation("gs://bkt-dmgcp-freshers-08/Harshul/temp/");
        po.setTempLocation("gs://bkt-dmgcp-freshers-08/Harshul/temp/");
        po.setUsePublicIps(Boolean.FALSE);
        po.setServiceAccount("sa-dmgcp-freshers-08-dataflow@dmgcp-training-q4-2021.iam.gserviceaccount.com");
        po.setNetwork("dm-primary-network");
        po.setSubnetwork("https://www.googleapis.com/compute/v1/projects/dm-network-host-project/regions/us-central1/subnetworks/subnet-dm-delivery-us-central1");

        boolean isStreaming = false;

        TableReference tableRef = new TableReference();
        tableRef.setProjectId("dmgcp-training-q4-2021");
        tableRef.setDatasetId("DB_FRESHERS_08");
        tableRef.setTableId("GcsToBq_1391");

        Pipeline p = Pipeline.create(po);

        p.apply("Read CSV File", TextIO.read().from("gs://bkt-dmgcp-freshers-08/Harshul/csvByJava.csv"))
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
        p.run().waitUntilFinish();
    }
}