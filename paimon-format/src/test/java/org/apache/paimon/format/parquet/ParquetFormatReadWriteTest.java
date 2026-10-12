/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** A parquet {@link FormatReadWriteTest}. */
public class ParquetFormatReadWriteTest extends FormatReadWriteTest {

    protected ParquetFormatReadWriteTest() {
        super("parquet");
    }

    @Override
    protected FileFormat fileFormat() {
        return new ParquetFileFormat(
                new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEnableBloomFilter(boolean enabled) throws Exception {
        Options options = new Options();
        options.set("parquet.bloom.filter.enabled", String.valueOf(enabled));
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.BIGINT());

        if (ThreadLocalRandom.current().nextBoolean()) {
            rowType = rowType.notNull();
        }

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(GenericRow.of(1, 1L));
        writer.addElement(GenericRow.of(2, 2L));
        writer.addElement(GenericRow.of(3, null));
        writer.close();
        out.close();

        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, file, fileIO.getFileSize(file), new Options())) {
            ParquetMetadata parquetMetadata = reader.getFooter();
            List<BlockMetaData> blockMetaDataList = parquetMetadata.getBlocks();
            for (BlockMetaData blockMetaData : blockMetaDataList) {
                List<ColumnChunkMetaData> columnChunkMetaDataList = blockMetaData.getColumns();
                for (ColumnChunkMetaData columnChunkMetaData : columnChunkMetaDataList) {
                    BloomFilter filter = reader.readBloomFilter(columnChunkMetaData);
                    Assertions.assertThat(enabled == (filter != null)).isTrue();
                }
            }
        }
    }

    @Test
    public void testIcebergTimestampEncoding() throws Exception {
        Options options = new Options();
        options.set("metadata.iceberg.storage", "table-location");
        ParquetFileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
        RowType rowType =
                RowType.builder()
                        .field("local_timestamp", DataTypes.TIMESTAMP(3))
                        .field("utc_timestamp", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .build();
        Timestamp timestamp = Timestamp.fromEpochMillis(1691827200123L);

        write(format.createWriterFactory(rowType), file, GenericRow.of(timestamp, timestamp));

        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, file, fileIO.getFileSize(file), new Options())) {
            Assertions.assertThat(
                    reader.getFooter()
                                    .getFileMetaData()
                                    .getSchema()
                                    .getType("local_timestamp")
                                    .getLogicalTypeAnnotation())
                    .isEqualTo(
                            LogicalTypeAnnotation.timestampType(
                                    false, LogicalTypeAnnotation.TimeUnit.MILLIS));
            Assertions.assertThat(
                            reader.getFooter()
                                    .getFileMetaData()
                                    .getSchema()
                                    .getType("utc_timestamp")
                                    .getLogicalTypeAnnotation())
                    .isEqualTo(
                            LogicalTypeAnnotation.timestampType(
                                    true, LogicalTypeAnnotation.TimeUnit.MICROS));
        }

        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, Collections.emptyList())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            reader.forEachRemaining(rows::add);
        }
        Assertions.assertThat(rows).hasSize(1);
        Assertions.assertThat(rows.get(0).getTimestamp(0, 3)).isEqualTo(timestamp);
        Assertions.assertThat(rows.get(0).getTimestamp(1, 3)).isEqualTo(timestamp);
    }

    @Test
    public void testIcebergReaderReadsLegacyMillisecondTimestamps() throws Exception {
        Options legacyOptions = new Options();
        ParquetFileFormat legacyFormat =
                new ParquetFileFormat(
                        new FileFormatFactory.FormatContext(legacyOptions, 1024, 1024));
        Options icebergOptions = new Options();
        icebergOptions.set("metadata.iceberg.storage", "table-location");
        ParquetFileFormat icebergFormat =
                new ParquetFileFormat(
                        new FileFormatFactory.FormatContext(icebergOptions, 1024, 1024));
        RowType rowType =
                RowType.builder()
                        .field("local_timestamp", DataTypes.TIMESTAMP(3))
                        .field("utc_timestamp", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .build();
        Timestamp timestamp = Timestamp.fromEpochMillis(1691827200123L);

        write(legacyFormat.createWriterFactory(rowType), file, GenericRow.of(timestamp, timestamp));

        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                icebergFormat
                        .createReaderFactory(rowType, rowType, Collections.emptyList())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            reader.forEachRemaining(rows::add);
        }
        Assertions.assertThat(rows).hasSize(1);
        Assertions.assertThat(rows.get(0).getTimestamp(0, 3)).isEqualTo(timestamp);
        Assertions.assertThat(rows.get(0).getTimestamp(1, 3)).isEqualTo(timestamp);
    }
}
