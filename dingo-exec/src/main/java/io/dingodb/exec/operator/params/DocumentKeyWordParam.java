/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

@Getter
@JsonTypeName("documentKeyword")
public class DocumentKeyWordParam extends AbstractParams {

    private final RangeDistribution rangeDistribution;

    private final Integer documentIdIndex;

    private final String targetkeyword;

    private final CommonId indexTableId;

    private final CommonId tableId;

    private final List<Object[]> cache;

    private final TupleMapping selection;

    private final String tableName;

    private final String indexName;

    private final Integer topK;

    private long  scanTs;

    protected CommonId partId;

    public DocumentKeyWordParam(
        RangeDistribution rangeDistribution,
        CommonId tableId,
        Integer documentIdIndex,
        CommonId indexTableId,
        String targetkeyword,
        TupleMapping selection,
        String tableName,
        String indexName,
        long scanTs,
        Integer topK
    ) {
        this.rangeDistribution = rangeDistribution;
        this.tableId = tableId;
        this.documentIdIndex = documentIdIndex;
        this.targetkeyword = targetkeyword;
        this.indexTableId = indexTableId;
        cache = new LinkedList<>();
        this.selection = selection;
        this.tableName = tableName;
        this.indexName = indexName;
        this.scanTs = scanTs;
        this.topK = topK;
    }

    public void clear() {
        cache.clear();
    }

}
