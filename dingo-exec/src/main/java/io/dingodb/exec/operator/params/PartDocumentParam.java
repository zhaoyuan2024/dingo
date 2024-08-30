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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

import java.util.Map;
import java.util.NavigableMap;

@Getter
@JsonTypeName("partDocument")
@JsonPropertyOrder({
    "tableId", "part", "schema", "schemaVersion", "keyMapping", "filter", "selection", "indexId", "indexRegionId"
})
public class PartDocumentParam extends FilterProjectSourceParam {

    private final KeyValueCodec codec;
    private final Table table;
    private final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;
    private final CommonId indexId;
    private final String queryString;
    private final int topN;

    public PartDocumentParam(
        CommonId tableId,
        CommonId partId,
        DingoType schema,
        TupleMapping keyMapping,
        SqlExpr filter,
        TupleMapping selection,
        Table table,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions,
        CommonId indexId,
        String queryString,
        int topN
    ) {
        super(tableId, partId, schema, table.version, filter, selection, keyMapping);
        this.codec = CodecService.getDefault().createKeyValueCodec(table.version, table.tupleType(), table.keyMapping());
        this.table = table;
        this.distributions = distributions;
        this.indexId = indexId;
        this.queryString= queryString;
        this.topN = topN;
    }
}