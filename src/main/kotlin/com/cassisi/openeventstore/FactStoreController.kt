package com.cassisi.openeventstore

import com.cassisi.openeventstore.core.Fact
import com.cassisi.openeventstore.core.FactStore
import com.cassisi.openeventstore.core.Subject
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import java.time.Instant
import java.util.UUID

@Path("/test")
class FactStoreController(
    private val db: FactStore,
) {

    @POST
    suspend fun test() {
        db.append(
            Fact(
                id = UUID.randomUUID(),
                type = "TEST_TYPE",
                payload = """ { "test": 123 } """.toByteArray(),
                createdAt = Instant.now(),
                subject = Subject(
                    type = "TEST",
                    id = "123"
                ),
            )
        )

    }

}