import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;



import java.util.List;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;




import java.util.Arrays;

public class DataPipeline {

  public static void main(String[] args){
    DataflowPipelineOptions pipelineOptions =  PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    pipelineOptions.setJobName("runnerJob");
    pipelineOptions.setProject("dataanalytics-363215");
    pipelineOptions.setRegion("us-central1");
    pipelineOptions.setRunner(DataflowRunner.class);
    pipelineOptions.setGcpTempLocation("gs://batch_processing/tmp");


    //    SchemaReader schemaReader = new SchemaReader();
    //    Schema reservationSchema = SchemaReader.readAvroSchema(SchemaReader.SALES_SCHEMA_FILE);
    //    TableSchema salesBqSchema = schemaReader.getTableSchemaRecord(reservationSchema);


    Pipeline pipeline = Pipeline.create(pipelineOptions);

    //PCollection<String> input = pipeline.apply(TextIO.read().from("gs://batch_processing/input/*").watchForNewFiles(Duration.standardMinutes(1),Watch.Growth.<String>never()));
    PCollection<String> input = pipeline.apply(TextIO.read().from("gs://batch_processing/input/*"));
    PCollection<TableRow> bqrow = input.apply(ParDo.of(new Extract()));
    ValueProvider<String> tempPath = new ValueProvider<String>() {
      @Override
      public String get() {
        return "gs://batch_processing/temp";
      }

      @Override
      public boolean isAccessible() {
        return false;
      }
    };

    bqrow.apply(BigQueryIO.writeTableRows().to("BatchPipeline.sales2")
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
      .withCustomGcsTempLocation(tempPath));

    pipeline.run().waitUntilFinish();
  }

  private static class Extract extends DoFn<String,TableRow> {

    @ProcessElement
    public void process(ProcessContext processContext){

      String row = processContext.element().toString();
      if(!row.contains("SALES")){
        List<String> columns = Arrays.asList(row.split(","));
        TableRow tablerow = new TableRow().set("order_number",Integer.valueOf(columns.get(0)))
          .set("product_line",columns.get(10))
          .set("sales",Float.valueOf(columns.get(4)))
          .set("msrp",Float.valueOf(columns.get(11)))
          .set("customer_name",columns.get(13))
          .set("phone",columns.get(14))
          .set("city",columns.get(17))
          .set("country",columns.get(20));

        processContext.output(tablerow);
      }

    }
  }
}
