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

package io.dingodb.exec.operator;

import com.google.common.collect.Lists;
import io.dingodb.common.CommonId;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.DocumentKeyWordParam;
import io.dingodb.meta.entity.Column;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.transaction.data.DocumentSearchParameter;
import io.dingodb.store.api.transaction.data.DocumentValue;
import io.dingodb.store.api.transaction.data.DocumentWithScore;
import io.dingodb.store.api.transaction.data.ScalarField;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.exec.operator.TxnGetByKeysOperator.getLocalStore;

@Slf4j
public class DocumentKeyWordOperator extends SoleOutOperator {

    public static final DocumentKeyWordOperator INSTANCE = new DocumentKeyWordOperator();
    public DocumentKeyWordOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        DocumentKeyWordParam param = vertex.getParam();
        param.setContext(context);
        param.getCache().add(tuple);
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        DocumentKeyWordParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("documentKeyWord");
        long start = System.currentTimeMillis();
        TupleMapping selection = param.getSelection();
        List<Object[]> cache = param.getCache();
        if (fin instanceof FinWithException) {
            edge.fin(fin);
            return;
        }
        List<Long> rightList = cache.stream().map(e ->
            (Long) e[param.getDocumentIdIndex()]
        ).collect(Collectors.toList());

        if (rightList.isEmpty()) {
            edge.fin(fin);
            return;
        }

        String keyword = param.getTargetkeyword();
        Integer topK = param.getTopK();
        StoreInstance instance = Services.KV_STORE.getInstance(param.getTableId(), param.getRangeDistribution().getId());
        DocumentSearchParameter documentSearchParameter = DocumentSearchParameter.builder()
            .topN(topK)
            .documentIds(rightList)
            .queryString(keyword)
            .useIdFilter(true)
            .build();
        List<DocumentWithScore> documentWithScores = instance.documentSearch(
            param.getScanTs(),
            param.getIndexTableId(),
            documentSearchParameter);
        List<Object[]> results = new ArrayList<>();
        List<Column> columns = param.getTable().getColumns();
        Object[] priTuples = new Object[param.getTable().columns.size() + 1];
        for (DocumentWithScore document : documentWithScores) {
            if(document.getDocumentWithId().getDocument().getTableData() != null){
                KeyValue tableData = new KeyValue(document.getDocumentWithId().getDocument().getTableData().getTableKey(),
                    document.getDocumentWithId().getDocument().getTableData().getTableValue());
                byte[] tmp1 = new byte[tableData.getKey().length];
                System.arraycopy(tableData.getKey(), 0, tmp1, 0, tableData.getKey().length);
                CommonId regionId = PartitionService.getService(
                        Optional.ofNullable(param.getTable().getPartitionStrategy())
                            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
                    .calcPartId(tableData.getKey(), param.getRangeDistributions());
                CodecService.getDefault().setId(tmp1, regionId.domain);
                CommonId txnId = vertex.getTask().getTxnId();
//                Iterator<Object[]> local = getLocalStore(
//                    regionId,
//                    param.getCodec(),
//                    tmp1,
//                    param.getTableId(),
//                    txnId,
//                    regionId.encode(),
//                    vertex.getTask().getTransactionType()
//                );
//                if (local != null) {
//                    while (local.hasNext()) {
//                        Object[] objects = local.next();
//                        results.add(objects);
//                    }
//                    continue;
//                }

//                StoreInstance storeInstance = StoreService.getDefault().getInstance(param.getTableId(), regionId);
//                KeyValue keyValue = storeInstance.txnGet(param.getScanTs(), tableData.getKey(), param.getTimeOut());
//                if (keyValue == null || keyValue.getValue() == null) {
//                    continue;
//                }
//                Object[] decode = param.getCodec().decode(keyValue);
//                decode[decode.length - 1] = document.getScore();
//                results.add(decode);
            } else {
                Map<String, DocumentValue> documentData = document.getDocumentWithId().getDocument().getDocumentData();
                Set<Map.Entry<String, DocumentValue>> entries = documentData.entrySet();
                for (Map.Entry<String, DocumentValue> entry : entries) {
                    String key = entry.getKey();
                    DocumentValue value = entry.getValue();
                    ScalarField fieldValue = value.getFieldValue();
                    int idx = 0;
//                    for (int i = 0; i < columns.size(); i++) {
//                        if (columns.get(i).getName().equals(key)) {
//                            idx = i;
//                            break;
//                        }
//                    }
//                    priTuples[idx] = fieldValue.getData();
                }

                float score = document.getScore();
//                priTuples[priTuples.length - 1] = score;
//                results.add(priTuples);
            }
        }

//        for (int i = 0; i < cache.size(); i ++) {
//            Object[] tuple = cache.get(i);
//            Object[] result = Arrays.copyOf(tuple, tuple.length + 1);
//            result[tuple.length] = scores.get(i);
//            edge.transformToNext(param.getContext(), selection.revMap(result));
//        }
        param.clear();
        profile.time(start);
        edge.fin(fin);
    }
}
