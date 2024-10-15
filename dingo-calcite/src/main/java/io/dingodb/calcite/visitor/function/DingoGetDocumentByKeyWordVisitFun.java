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

package io.dingodb.calcite.visitor.function;

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoGetDocumentByKeyWord;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.DocumentKeyWordParam;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.function.Supplier;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.calcite.visitor.function.DingoDocumentVisitFun.getDocumentKeyword;
import static io.dingodb.exec.utils.OperatorCodeUtils.DOCUMENT_KEYWORD;

public final class DingoGetDocumentByKeyWordVisitFun {

    private DingoGetDocumentByKeyWordVisitFun() {
    }

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        @NonNull DingoGetDocumentByKeyWord rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        return DingoBridge.bridge(idGenerator, inputs, new OperatorSupplier(rel));
    }

    @AllArgsConstructor
    static class OperatorSupplier implements Supplier<Vertex> {

        final DingoGetDocumentByKeyWord rel;

        @Override
        public Vertex get() {
            DingoRelOptTable dingoRelOptTable = (DingoRelOptTable) rel.getTable();
            String targetDocument = getTargetKeyword(rel.getOperands());
            IndexTable indexTable = getDocumentIndexTable(dingoRelOptTable);
            if (indexTable == null) {
                throw new RuntimeException("not found document index");
            }
            MetaService metaService = MetaService.root().getSubMetaService(dingoRelOptTable.getSchemaName());
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
                = metaService.getRangeDistribution(rel.getIndexTableId());

            DocumentKeyWordParam param = new DocumentKeyWordParam(
                distributions.firstEntry().getValue(),
                rel.getDocumentIndex(),
                rel.getIndexTableId(),
                targetDocument,
                indexTable.getProperties().getProperty("metricType"),
                rel.getSelection()
            );

            return new Vertex(DOCUMENT_KEYWORD, param);
        }
    }

    public static String getTargetKeyword(List<Object> operandList) {
        String keyword = getDocumentKeyword(operandList);
        return keyword;
    }

    private static IndexTable getDocumentIndexTable(DingoRelOptTable dingoRelOptTable) {
        DingoTable dingoTable = dingoRelOptTable.unwrap(DingoTable.class);
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (IndexTable index : indexes) {
            if (index.getIndexType() != IndexType.DOCUMENT) {
                continue;
            }
                return index;
        }
        return null;
    }
}
