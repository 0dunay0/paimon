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

package org.apache.paimon.metastore;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.tag.TagPreview;

import java.util.List;
import java.util.Optional;

/** A {@link CommitCallback} to add partitions to metastore for tag preview. */
public class TagPreviewCommitCallback implements CommitCallback {

    private final AddPartitionTagCallback tagCallback;
    private final TagPreview tagPreview;

    public TagPreviewCommitCallback(AddPartitionTagCallback tagCallback, TagPreview tagPreview) {
        this.tagCallback = tagCallback;
        this.tagPreview = tagPreview;
    }

    @Override
    public void call(
            List<ManifestEntry> committedEntries,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot) {
        long currentMillis = System.currentTimeMillis();
        Optional<String> tagOptional = tagPreview.extractTag(currentMillis, snapshot.watermark());
        tagOptional.ifPresent(tagCallback::notifyCreation);
    }

    @Override
    public void retry(ManifestCommittable committable) {
        long currentMillis = System.currentTimeMillis();
        Optional<String> tagOptional =
                tagPreview.extractTag(currentMillis, committable.watermark());
        tagOptional.ifPresent(tagCallback::notifyCreation);
    }

    @Override
    public void close() throws Exception {
        tagCallback.close();
    }
}
