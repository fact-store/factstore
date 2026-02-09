package org.factstore.server

import jakarta.enterprise.context.ApplicationScoped
import org.factstore.core.FactStore
import org.factstore.foundationdb.buildFdbFactStore

@ApplicationScoped
class FactStoreProvider {

    val clusterFilePath: String = "/etc/foundationdb/fdb.cluster"
    val name: String = "test-factstore"


    val fdbFactStore = buildFdbFactStore(clusterFilePath, name)


    fun findByName(factStoreName: String): FactStore = fdbFactStore

}