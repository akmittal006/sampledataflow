import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindmillPipeline {

    private static Logger LOG = LoggerFactory.getLogger(WindmillPipeline.class);

    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = element.split("[^\\p{L}]+", -1);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    static class ExtractStringFromPubSubMessage extends DoFn<PubsubMessage, String> {

        @ProcessElement
        public void processElement(@Element PubsubMessage message, OutputReceiver<String> receiver) {
            byte[] s = message.getPayload();
            String line = new String(s);
            receiver.output(line);
        }
    }

    public static class CountWords
            extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());
            return wordCounts;
        }
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    static void runWordCount(WindmillPipelineOptions options) {

        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", PubsubIO.readMessages().fromTopic("projects/cloud-dataflow-244206/topics/newpubsub"))
                .apply("FixedWindows",
                        Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowDuration()))))
                .apply("ConvertToString", ParDo.of(new ExtractStringFromPubSubMessage()))
                .apply("CountWords", new CountWords())
                .apply("FormatOutput", MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", new WriteOneFilePerWindow(options.getOutput(),1));

        p.run().waitUntilFinish();
    }


    public static void main(String[] args) {

        WindmillPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(WindmillPipelineOptions.class);

        String jsonPath = jsonPath = "/Users/ankur/Downloads/cloud-dataflow-da1f19a92eb0.json";

        options.setGcpCredential(Utils.getCredentials(jsonPath));
        options.setProject("cloud-dataflow-244206");
//        options.setPubsubTopic("newpubsub");
        runWordCount(options);

    }

}
