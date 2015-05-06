** Introduction **

This project provides an input pipeline for [Oracle GoldenGate](http://www.oracle.com/us/products/middleware/data-integration/goldengate/overview/index.html) Length Delimited files (LDV). This works by breaking LDV files into events and writing them to the flume pipeline. An EventDeserializer is used to read data from a LDV file to an [Avro](https://avro.apache.org/) message. 

** Installation **

Download the jar and put it in the plugins folder for flume. Follow the [Installing third-party plugins](https://flume.apache.org/FlumeUserGuide.html#installing-third-party-plugins) section of the Flume user guide.

** Configuration Properties **  

| First Header              | Second Header |
| ------------------------- | ------------- |
| record.length             | The length in bytes for the record prefix. This must match the goldengate configuration. Valid values are 2,4, or 8. |
| record.length.encoding    | The type of encoding for the record. Valid values are ASCII or BINARY.  |
| field.length              | The length in bytes for the field prefix. This must match the goldengate configuration. Valid values are 2,4, or 8. |
| field.length.encoding     | The type of encoding for the field. Valid values are ASCII or BINARY. |
| metadata.columns          | The metadata columns for each row. This must match the goldengate configuration. Metadata columns are added as headers with the values found in the source LDV.|
| headers.                  | Additional headers that will be applied to each event. These values are statically applied to each event.|



** Example Configuration **

```
goldengate.sources.goldengate_spool.type                                = spooldir
goldengate.sources.goldengate_spool.channels                            = goldengate_filechannel
goldengate.sources.goldengate_spool.spoolDir                            = /mnt/goldengate/flume/
goldengate.sources.goldengate_spool.ignorePattern                       = .*(?<!\.ldv)$
goldengate.sources.goldengate_spool.deserializer                        = com.cloudera.integration.oracle.goldengate.ldv.flume.LDVEventDeserializer$Builder
goldengate.sources.goldengate_spool.deserializer.record.length          = 8
goldengate.sources.goldengate_spool.deserializer.record.length.encoding = ASCII
goldengate.sources.goldengate_spool.deserializer.field.length           = 8
goldengate.sources.goldengate_spool.deserializer.field.length.encoding  = ASCII
goldengate.sources.goldengate_spool.deserializer.metadata.columns       = opcode,timestamp
```