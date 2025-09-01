package com.cassisi.openeventstore

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.dcb.FactStore
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces

class FdbConfig {

    @Produces
    @ApplicationScoped
    fun foundationDb(): Database {
        println("opening fdb cluster...")
        FDB.selectAPIVersion(730)
        val fdb = FDB.instance()
        val db = fdb.open("/etc/foundationdb/fdb.cluster")
        println("successfully opened...")
        return db
    }

    @Produces
    @ApplicationScoped
    fun factStore(db: Database) = FactStore(db)

}