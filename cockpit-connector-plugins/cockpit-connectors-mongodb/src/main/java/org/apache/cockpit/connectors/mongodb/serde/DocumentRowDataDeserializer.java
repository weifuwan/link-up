package org.apache.cockpit.connectors.mongodb.serde;


import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.mongodb.exception.MongodbConnectorException;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import static org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT;
import static org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION;
import static org.apache.cockpit.connectors.api.type.SqlType.STRING;


public class DocumentRowDataDeserializer implements DocumentDeserializer<SeaTunnelRow> {

    private final String[] fieldNames;

    private final SeaTunnelDataType<?>[] fieldTypes;

    private final BsonToRowDataConverters bsonConverters;

    private final boolean flatSyncString;

    public DocumentRowDataDeserializer(
            String[] fieldNames, SeaTunnelDataType<?> dataTypes, boolean flatSyncString) {
        if (fieldNames == null || fieldNames.length < 1) {
            throw new MongodbConnectorException(ILLEGAL_ARGUMENT, "fieldName is empty");
        }
        this.bsonConverters = new BsonToRowDataConverters();
        this.fieldNames = fieldNames;
        this.fieldTypes = ((SeaTunnelRowType) dataTypes).getFieldTypes();
        this.flatSyncString = flatSyncString;
    }

    @Override
    public SeaTunnelRow deserialize(BsonDocument bsonDocument) {
        if (flatSyncString) {
            if (fieldNames.length != 1 && fieldTypes[0].getSqlType() != STRING) {
                throw new MongodbConnectorException(
                        UNSUPPORTED_OPERATION,
                        "By utilizing flatSyncString, only one field attribute value can be set, and the field type must be a String. This operation will perform a string mapping on a single MongoDB data entry.");
            }
            SeaTunnelRow rowData = new SeaTunnelRow(fieldNames.length);
            rowData.setField(
                    0, bsonConverters.createConverter(fieldTypes[0]).convert(bsonDocument));
            return rowData;
        }
        SeaTunnelRow rowData = new SeaTunnelRow(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = this.fieldNames[i];
            BsonValue o = bsonDocument.get(fieldName);
            SeaTunnelDataType<?> fieldType = fieldTypes[i];
            rowData.setField(i, bsonConverters.createConverter(fieldType).convert(o));
        }
        return rowData;
    }
}
