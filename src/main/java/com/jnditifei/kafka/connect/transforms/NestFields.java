package com.jnditifei.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;

import org.apache.kafka.connect.transforms.Transformation;

import java.util.*;
import java.util.stream.Collectors;

public class NestFields<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELDS_TO_NEST_CONFIG = "fields.to.nest";

    private Map<String, String> fieldsToNest; // Map<FieldName, StructName>

    @Override
    public void configure(Map<String, ?> configs) {
        String configString = (String) configs.get(FIELDS_TO_NEST_CONFIG);
        fieldsToNest = Arrays.stream(configString.split(","))
                .map(String::trim)
                .map(pair -> pair.split(":"))
                .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1])); // Map field -> struct
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct value)) {
            return record;
        }

        Schema originalSchema = value.schema();
        Map<String, Schema> nestedSchemas = new HashMap<>();
        Map<String, Struct> nestedStructs = new HashMap<>();

        // Step 1: Build the nested schemas
        for (Map.Entry<String, String> entry : fieldsToNest.entrySet()) {
            String fieldName = entry.getKey();
            String structName = entry.getValue();

            Field field = originalSchema.field(fieldName);
            if (field == null) continue; // Skip if the field doesn't exist

            Schema fieldSchema = field.schema();
            Schema nestedSchema = new ConnectSchema(
                    Schema.Type.STRUCT,
                    fieldSchema.isOptional(),
                    null,
                    structName,
                    fieldSchema.version(),
                    null,
                    null,
                    Collections.singletonList(new Field(fieldName, 0, fieldSchema)), // Single field inside struct
                    null,
                    null
            );

            nestedSchemas.put(structName, nestedSchema);
        }

        // Step 2: Create the updated schema
        Schema updatedSchema = new ConnectSchema(
                Schema.Type.STRUCT,
                originalSchema.isOptional(),
                originalSchema.defaultValue(),
                originalSchema.name(),
                originalSchema.version(),
                originalSchema.doc(),
                originalSchema.parameters(),
                originalSchema.fields().stream()
                        .map(field -> fieldsToNest.containsKey(field.name())
                                ? new Field(fieldsToNest.get(field.name()), field.index(), nestedSchemas.get(fieldsToNest.get(field.name()))) // Replace field with struct
                                : field)
                        .collect(Collectors.toList()),
                originalSchema.type() == Schema.Type.ARRAY ? originalSchema.keySchema() : null,
                originalSchema.type() == Schema.Type.MAP ? originalSchema.valueSchema() : null
        );

        // Step 3: Build the updated struct
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : originalSchema.fields()) {
            if (fieldsToNest.containsKey(field.name())) {
                String structName = fieldsToNest.get(field.name());
                Schema nestedSchema = nestedSchemas.get(structName);
                Object fieldValue = value.get(field.name());

                Struct nestedStruct = new Struct(nestedSchema).put(field.name(), fieldValue);
                updatedValue.put(structName, nestedStruct);
            } else {
                updatedValue.put(field.name(), value.get(field.name()));
            }
        }

        // Step 4: Return new record
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_TO_NEST_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "Comma-separated list of fields to nest in the format fieldName:structName");
    }

    @Override
    public void close() {
    }
}
