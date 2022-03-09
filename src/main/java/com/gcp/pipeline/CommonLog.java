package com.gcp.pipeline;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * A class used for parsing JSON web server events
 * Annotated with @DefaultSchema to the allow the use of Beam Schema and <Row> object
 */
@DefaultSchema(JavaFieldSchema.class)
public class CommonLog {
    @javax.annotation.Nullable int id;
    @javax.annotation.Nullable String name;
    @javax.annotation.Nullable String surname;

}