package com.cassisi.openeventstore.core.impl

import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.FactStore

fun buildFdbFactStore(
    clusterFilePath: String = "/etc/foundationdb/fdb.cluster",
    name: String = DEFAULT_FACT_STORE_NAME
): FactStore {
    val db = FDB.instance().open(clusterFilePath)
    val fdbFactStore = FdbFactStore(db, name)
    return FactStore(
        factAppender = FdbFactAppender(fdbFactStore),
        factFinder = FdbFactFinder(fdbFactStore),
        factStreamer = FdbFactStreamer(fdbFactStore),
        conditionalSubjectFactAppender = ConditionalFdbFactAppender(fdbFactStore),
        conditionalTagQueryFactAppender = ConditionalTagQueryFdbFactAppender(fdbFactStore),
    )
}
