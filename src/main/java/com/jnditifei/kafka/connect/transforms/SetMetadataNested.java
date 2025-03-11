package com.jnditifei.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class SetMetadataNested<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String NESTED_FIELD_NAME_CONFIG = "nested.field.name";
    public static final String NEW_SCHEMA_NAME_CONFIG = "new.schema.name";

    private String nestedFieldName;
    private String newSchemaName;

    @Override
    public void configure(Map<String, ?> configs) {
        nestedFieldName = (String) configs.get(NESTED_FIELD_NAME_CONFIG);
        newSchemaName = (String) configs.get(NEW_SCHEMA_NAME_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct value)) {
            return record;
        }

        // Ensure the specified nested field exists in the schema
        Field nestedField = value.schema().field(nestedFieldName);
        if (nestedField == null || nestedField.schema().type() != Schema.Type.STRUCT) {
            return record; // If not found, return the original record
        }

        Schema originalNestedSchema = nestedField.schema();

        // Create the updated schema with the new name
        final Schema updatedNestedSchema = new ConnectSchema(
                originalNestedSchema.type(),
                originalNestedSchema.isOptional(),
                originalNestedSchema.defaultValue(),
                newSchemaName,  // Use the provided new schema name
                originalNestedSchema.version(),
                originalNestedSchema.doc(),
                originalNestedSchema.parameters(),
                originalNestedSchema.fields(),
                originalNestedSchema.type() == Schema.Type.ARRAY ? originalNestedSchema.keySchema() : null,
                originalNestedSchema.type() == Schema.Type.MAP ? originalNestedSchema.valueSchema() : null
        );

        // 🔥 Update the value schema dynamically
        final Schema updatedValueSchema = new ConnectSchema(
                value.schema().type(),
                value.schema().isOptional(),
                value.schema().defaultValue(),
                value.schema().name(),
                value.schema().version(),
                value.schema().doc(),
                value.schema().parameters(),
                value.schema().fields().stream()
                        .map(field -> field.name().equals(nestedFieldName)
                                ? new Field(nestedFieldName, field.index(), updatedNestedSchema) // Update dynamically
                                : field
                        )
                        .toList(),
                value.schema().type() == Schema.Type.ARRAY ? value.schema().keySchema() : null,
                value.schema().type() == Schema.Type.MAP ? value.schema().valueSchema() : null
        );

        // 🔄 Rebuild the value struct with updated schema
        Struct updatedValue = new Struct(updatedValueSchema);
        for (Field field : value.schema().fields()) {
            if (field.name().equals(nestedFieldName)) {
                Struct nestedStruct = value.getStruct(nestedFieldName);
                Struct updatedNestedStruct = null;
                if (nestedStruct != null) {
                    updatedNestedStruct = new Struct(updatedNestedSchema);
                    for (Field nestedSubField : originalNestedSchema.fields()) {
                        updatedNestedStruct.put(nestedSubField.name(), nestedStruct.get(nestedSubField.name()));
                    }
                }
                updatedValue.put(nestedFieldName, updatedNestedStruct);
            } else {
                updatedValue.put(field.name(), value.get(field.name()));
            }
        }

        // Return the new record with the updated schema and value
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedValueSchema,
                updatedValue,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(NESTED_FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field name of the nested schema to modify")
                .define(NEW_SCHEMA_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "New schema name for the nested struct");
    }

    @Override
    public void close() {
    }
}
