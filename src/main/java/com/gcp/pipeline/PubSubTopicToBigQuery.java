package com.gcp.pipeline;

import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubTopicToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubTopicToBigQuery.class);

    /**
     * The {@link Options} class provides the custom execution options passed by the
     * executor at the command-line.
     */

    public interface Options extends DataflowPipelineOptions {

        @Description("Bigquery Table Name")
        String getTableName();
        void setTableName(String tableName);

        @Description("Pubsub Subscription")
        String getpubsubTopic();
        void setpubsubTopic(String pubsubTopic);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }


    /**
     * A DoFn acccepting Json and outputing CommonLog with Beam Schema
     */
    
   static class JsonToTableData extends DoFn<String, TableDetails> {

        @ProcessElement
        public void processElement(@Element String json, OutputReceiver r) throws Exception {
            Gson gson = new Gson();
            TableDetails details = gson.fromJson(json, TableDetails.class);
            r.output(details);
        }
    }

    public static final Schema rawSchema = Schema
            .builder()
            .addInt64Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();



    public static PipelineResult run(Options options) {
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        //options.setJobName("usecase1-labid-5" + System.currentTimeMillis());

        PCollection<TableDetails> tableDetails = pipeline
                .apply("ReadMessage", PubsubIO.readStrings()
                        .fromTopic(options.getpubsubTopic()))

                .apply("ParseJson", ParDo.of(new JsonToTableData()));


        tableDetails
                .apply("Convert ToR ow", ParDo.of(new DoFn<TableDetails, String>() {
                    @ProcessElement
                    public void processElement(OutputReceiver outputReceiver) {
                        Gson g = new Gson();
                        String gsonString = g.toJson(outputReceiver.toString());
                        outputReceiver.output(gsonString);
                        
                    }
                })).apply("Convert Json To row", JsonToRow.withSchema(rawSchema))
                // Streaming insert of aggregate data
                .apply("Write PubSub Topic to BigQuery",
                        BigQueryIO.<Row>write().to(options.getTableName()).useBeamSchema()
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        LOG.info("Building pipeline...");

        return pipeline.run();
    }
}
