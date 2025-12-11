package org.apache.cockpit.connectors.doris.rest.models;

import java.util.Objects;

public class Field {
    private String name;
    private String type;
    private String comment;
    private int precision;
    private int scale;
    private String aggregationType;

    public Field() {}

    public Field(
            String name,
            String type,
            String comment,
            int precision,
            int scale,
            String aggregationType) {
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.precision = precision;
        this.scale = scale;
        this.aggregationType = aggregationType;
    }

    public String getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(String aggregationType) {
        this.aggregationType = aggregationType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field field = (Field) o;
        return precision == field.precision
                && scale == field.scale
                && Objects.equals(name, field.name)
                && Objects.equals(type, field.type)
                && Objects.equals(comment, field.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, comment, precision, scale);
    }

    @Override
    public String toString() {
        return "Field{"
                + "name='"
                + name
                + '\''
                + ", type='"
                + type
                + '\''
                + ", comment='"
                + comment
                + '\''
                + ", precision="
                + precision
                + ", scale="
                + scale
                + '}';
    }
}
