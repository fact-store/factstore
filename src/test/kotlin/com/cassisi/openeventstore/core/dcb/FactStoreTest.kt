package com.cassisi.openeventstore.core.dcb

import com.apple.foundationdb.FDB
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FactStoreTest {

    private lateinit var store: FactStore

    @BeforeAll
    fun setupFDB() {
        FDB.selectAPIVersion(730)
        val db = FDB.instance().open("/etc/foundationdb/fdb.cluster")
        store = FactStore(db)
    }

    @BeforeEach
    fun clearEventStore() {
        store.reset()
    }

    @Test
    fun testSimpleAppend(): Unit = runBlocking {
        val id = UUID.randomUUID()
        val payload = """ { "username": "Peter" } """
        val createdAt = Instant.now()

        val fact = Fact(
            id = id,
            type = "USER_ONBOARDED",
            payload = payload,
            createdAt = createdAt
        )

        store.append(fact)

        // validate existence of fact
        assertThat(store.existsById(id)).isTrue()

        // find fact by ID
        val findResult = store.findById(id)
        assertThat(findResult).isNotNull().isEqualTo(fact)
    }

    @Test
    fun testExists(): Unit = runBlocking {
        val nonExistingFactId = UUID.randomUUID()
        assertThat(store.existsById(nonExistingFactId)).isFalse()
    }
}