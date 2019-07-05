import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.*;

public interface WindmillPipelineOptions extends GcpOptions {

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();

    void setOutput(String value);

    @Description("Pub/Sub topic")
    @Default.InstanceFactory(PubsubTopicFactory.class)
    String getPubsubTopic();

    void setPubsubTopic(String topic);

    @Description("Window duration")
    @Default.Long(60)
    long getWindowDuration();

    void setWindowDuration(long duration);

    /** Returns a default Pub/Sub topic based on the project and the job names. */
    class PubsubTopicFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {
            return "projects/cloud-dataflow-244206/topics/testpubsub";
        }
    }
}
