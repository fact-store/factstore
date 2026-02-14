package org.factstore.server.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithName

@ConfigMapping(prefix = "factstore")
interface FactStoreConfig {

    fun foundationdb(): FoundationDbConfig

    interface FoundationDbConfig {
        @WithName("cluster-file")
        fun clusterFile(): String

        @WithName("api-version")
        fun apiVersion(): Int
    }
}