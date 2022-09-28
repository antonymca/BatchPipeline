import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

public class SchemaReader {
    public static final String SALES_SCHEMA_FILE = "avsc/schema.avsc";


    private static final Logger LOG = LoggerFactory.getLogger(SchemaReader.class);

    public TableSchema readBQTableSchema(String path) throws IOException {
        MatchResult matchResult = FileSystems.match(path);
        if (!matchResult.status().equals(Status.OK)) {
            throw new IOException("Problem while reading BQ schema file!");
        }
        if (matchResult.metadata().isEmpty()) {
            throw new IllegalArgumentException("No BQ schema found!");
        }
        for (Metadata fileMetadata : matchResult.metadata()) {
            InputStream is = Channels.newInputStream(FileSystems.open(fileMetadata.resourceId()));
            Schema avroSchema = new Schema.Parser().parse(is);
            return getTableSchemaRecord(avroSchema);
        }
        return null;
    }

    private static Schema parseAvroSchema(String schemaPath) {
        Schema.Parser parser = new Schema.Parser();
        try (InputStream inputStream = DataPipeline.class.getClassLoader().getResourceAsStream(schemaPath)) {
            return parser.parse(inputStream);
        } catch (Exception e) {
            LOG.error("Unable to parse schema [{}][{}]", e.getMessage(), e);
            throw new IllegalStateException(e.getMessage());
        }
    }

    public static Schema readAvroSchema(String path) {
        return parseAvroSchema(path);
    }

    public TableSchema getTableSchemaRecord(Schema schema) {
        List<Schema.Field> avroFields = schema.getFields();
        List<TableFieldSchema> bqFields = new ArrayList<>();
        for (Schema.Field avroField : avroFields) {
            bqFields.add(tryArrayFieldSchema(avroField));
        }
        return new TableSchema().setFields(bqFields);
    }

    private TableFieldSchema tryArrayFieldSchema(Schema.Field field) {
        String fieldName = field.name();
        TableFieldSchema tableFieldSchema = new TableFieldSchema().setName(fieldName).setDescription(field.doc());
        boolean nullable = isNullable(field.schema());
        if (!nullable) {
            tableFieldSchema = tableFieldSchema.setMode("REQUIRED");
        }
        Schema fieldSchema = unwrapIfNullable(field.schema());
        if (fieldSchema.getType() == Type.ARRAY) {
            return tryFieldSchema(tableFieldSchema.setMode("REPEATED"), fieldSchema.getElementType());
        }
        return tryFieldSchema(tableFieldSchema, fieldSchema);
    }

    private TableFieldSchema tryFieldSchema(TableFieldSchema fieldSchema, Schema avroSchema) {
        fieldSchema = fieldSchema.setType(getBQFieldType(avroSchema));
        if (avroSchema.getType() == Type.RECORD) {
            List<TableFieldSchema> childFields = new ArrayList<>();
            List<Schema.Field> avroChildFields = avroSchema.getFields();
            for (Schema.Field avroChildField : avroChildFields) {
                childFields.add(tryArrayFieldSchema(avroChildField));
            }
            fieldSchema.setFields(childFields);
        }
        return fieldSchema;
    }

    private String getBQFieldType(Schema schema) {
        Type type = schema.getType();
        switch (type) {
            case UNION: {
                if (schema.getTypes().size() == 2
                        && schema.getTypes().get(0).getType() == Type.NULL) {
                    return getBQFieldType(schema.getTypes().get(1));
                }
                return "RECORD";
            }
            case RECORD:
                return "RECORD";
            case INT:
            case LONG:
                if (schema.getLogicalType() != null) {
                    switch (schema.getLogicalType().getName()) {
                        case "timestamp-millis":
                            return "TIMESTAMP";
                        case "time-millis":
                            return "TIME";
                        case "date":
                            return "DATE";
                    }
                    return "INTEGER";
                }
                return "INTEGER";
            case BOOLEAN:
                return "BOOLEAN";
            case FLOAT:
            case DOUBLE:
                return "FLOAT";
            case BYTES:
                if(schema.getLogicalType().getName().equals("decimal")) {
                    return "NUMERIC";
                }
                return "BYTES";
            default:
                return "STRING";
        }
    }


    public static boolean isNullable(Schema schema) {
        if (schema.getType() == Type.NULL) {
            return true;
        }
        if (schema.getType() == Type.UNION) {
            for (Schema unionType : schema.getTypes()) {
                if (unionType.getType() == Type.NULL) {
                    return true;
                }
            }
        }
        return false;
    }

    public static Schema unwrapIfNullable(Schema schema) {
        if (schema.getType() == Type.UNION) {
            List<Schema> unionTypes = schema.getTypes();
            if (unionTypes.size() == 2) {
                if (unionTypes.get(0).getType().equals(Type.NULL)) {
                    return unionTypes.get(1);
                } else if (unionTypes.get(1).getType().equals(Type.NULL)) {
                    return unionTypes.get(0);
                }
            } else if (unionTypes.contains(Schema.create(Type.NULL))) {
                ArrayList<Schema> typesWithoutNullable = new ArrayList<>(unionTypes);
                typesWithoutNullable.remove(Schema.create(Type.NULL));
                return Schema.createUnion(typesWithoutNullable);
            }
        }
        return schema;
    }
}
