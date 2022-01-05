import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.util.Arrays;
import java.util.List;

public class LocalToLocal {
    public static void main(String[] args) {
        PipelineOptions pipeLineOptions= PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipeLineOptions);
        final List<String> input = Arrays.asList("","is","Third","Last");
        pipeline.apply(Create.of(input)).apply(TextIO.write().to("D:\\JavaProjects\\DemoProjectDF\\output\\example").withSuffix(".txt"));
        pipeline.run().waitUntilFinish();
    }
}
