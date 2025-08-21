package com.cassisi.openeventstore.core.classic

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import com.apple.foundationdb.MutationType
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.cassisi.openeventstore.core.classic.ClassicEventStore.Event
import kotlinx.serialization.json.Json
import java.util.*
import java.util.concurrent.CompletionException
import kotlin.text.Charsets.UTF_8

const val EVENT_STORE = "event-store"
const val EVENT_ID = "event-id"
const val EVENT_DATA = "event-data"
const val SUBJECT = "subject"
const val EVENT_TYPE = "event-type"
const val GLOBAL_EVENT_POSITION = "global"

/**
 *
 * A simple event store implementation based on FoundationDB.
 *
 *
 * EVENT KEYS
 * /event-data/{eventId} = payload
 * /event-type/{eventId} = type
 * /event-id/{eventId}   = ∅   (existence / deduplication anchor)
 *
 *
 *
 * INDEXES (PLAN)
 * /global/{versionstamp}/{index}/{eventId} = ∅
 * /subject/{subjectId}/{versionstamp}/{index}/{eventId} = ∅
 * /by-type/{type}/{versionstamp}/{index}/{eventId} = ∅
 *
 */
class ClassicEventStore(
    private val db: Database,
) {

    private val root = DirectoryLayer.getDefault().createOrOpen(db, listOf(EVENT_STORE)).get()
    private val eventIdSubspace = root.subspace(Tuple.from(EVENT_ID))
    private val eventDataSubspace = root.subspace(Tuple.from(EVENT_DATA))
    private val eventTypeSubspace = root.subspace(Tuple.from(EVENT_TYPE))
    private val subjectSubspace = root.subspace(Tuple.from(SUBJECT))
    private val globalEventPositionSubspace = root.subspace(Tuple.from(GLOBAL_EVENT_POSITION))


    class ConcurrencyException : RuntimeException("Subject version mismatch")

    fun appendWithExpectedVersion(subjectId: String, expectedVersion: Pair<Versionstamp, Long>?, events: List<Event>) {
        db.run { tr ->
            // Find last version in DB
            val subjectRange = subjectSubspace.range(Tuple.from(subjectId))
            val lastKv = tr.getRange(subjectRange, 1, true).asList().get().firstOrNull()
            val currentVersion = lastKv?.let {
                val tup = subjectSubspace.unpack(it.key)
                tup.getVersionstamp(1) to tup.getLong(2)
            }

            if (currentVersion != expectedVersion) {
                throw ConcurrencyException()
            }

            // safe to append
            events.forEachIndexed { index, event ->
                val eventId = event.id.toString() // stable key (event identifier)

                val idKey = eventIdSubspace.pack(Tuple.from(eventId))
                tr[idKey] = ByteArray(0)

                val dataKey = eventDataSubspace.pack(Tuple.from(eventId))
                tr[dataKey] = event.data

                val typeKey = eventTypeSubspace.pack(Tuple.from(eventId))
                tr[typeKey] = event.type.toByteArray(UTF_8)

                // BUILD INDEXES

                // subject index
                val subjectKey = subjectSubspace.packWithVersionstamp(
                    Tuple.from(subjectId, Versionstamp.incomplete(), index, eventId)
                )
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subjectKey, ByteArray(0))

                // global position index
                val globalPositionKey = globalEventPositionSubspace.packWithVersionstamp(
                    Tuple.from(Versionstamp.incomplete(), index, eventId)
                )
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalPositionKey, ByteArray(0))
            }
        }
    }

    fun fetchEvents(subjectId: String): SubjectEvents {
        return db.read { tr ->
            val subjectRange = subjectSubspace.range(Tuple.from(subjectId))
            val kvs = tr.getRange(subjectRange).asList().get()

            val seen = mutableSetOf<UUID>()
            val events = kvs.mapNotNull { kv ->
                val keyTuple = subjectSubspace.unpack(kv.key)
                val eventId = UUID.fromString(keyTuple.getString(3))

                if (eventId in seen) return@mapNotNull null
                seen += eventId

                val eventDataKey = eventDataSubspace.pack(Tuple.from(eventId.toString()))
                val eventData = tr[eventDataKey].join() ?: return@mapNotNull null

                val eventTypeKey = eventTypeSubspace.pack(Tuple.from(eventId.toString()))
                val eventType = tr[eventTypeKey].join() ?: return@mapNotNull null

                Event(
                    id = eventId,
                    subject = subjectId,
                    type = eventType.toString(UTF_8),
                    data = eventData
                )
            }

            val last = kvs.lastOrNull()?.let { kv ->
                val tup = subjectSubspace.unpack(kv.key)
                tup.getVersionstamp(1) to tup.getLong(2)
            }

            SubjectEvents(subjectId, events, last)
        }
    }

    fun fetchAll(): List<Event> {
        val range = globalEventPositionSubspace.range()
        return db.read { tr ->
            val kvs = tr.getRange(range).asList().get()

            val events = kvs.mapNotNull { kv ->

                val keyTuple = globalEventPositionSubspace.unpack(kv.key)
                val eventId = UUID.fromString(keyTuple.getString(2))

                val eventDataKey = eventDataSubspace.pack(Tuple.from(eventId.toString()))
                val eventData = tr[eventDataKey].join() ?: return@mapNotNull null

                val eventTypeKey = eventTypeSubspace.pack(Tuple.from(eventId.toString()))
                val eventType = tr[eventTypeKey].join() ?: return@mapNotNull null



                Event(
                    id = eventId,
                    subject = "TODO!!!",
                    type = eventType.toString(UTF_8),
                    data = eventData
                )
            }
            events
        }

    }

    data class SubjectEvents(
        val subject: String,
        val events: List<Event>,
        val lastVersion: Pair<Versionstamp, Long>?, // (versionstamp, index)
    )

    data class Event(
        val id: UUID = UUID.randomUUID(),
        val subject: String,
        val type: String,
        val data: ByteArray,
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Event

            if (type != other.type) return false
            if (subject != other.subject) return false
            if (!data.contentEquals(other.data)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = type.hashCode()
            result = 31 * result + subject.hashCode()
            result = 31 * result + data.contentHashCode()
            return result
        }
    }
}


fun main() {
    FDB.selectAPIVersion(730)
    val fdb = FDB.instance()
    val db = fdb.open("/etc/foundationdb/fdb.cluster")

    val classicEventStore = ClassicEventStore(db)

    val randomSubject = "subject:${UUID.randomUUID()}"

    val eventsToSave = listOf(
        Event(
            id = UUID.randomUUID(),
            subject = randomSubject,
            type = "USER_ONBOARDED",
            data = """{ "username": "test123" }""".toByteArray(UTF_8)
        ),
        Event(
            id = UUID.randomUUID(),
            subject = randomSubject,
            type = "USER_LOCKED",
            data = """{ "locked": true }""".toByteArray(UTF_8)
        )
    )

    classicEventStore.appendWithExpectedVersion(randomSubject, null, eventsToSave)

    classicEventStore.fetchEvents(randomSubject).lastVersion.also { println(it) }

    classicEventStore.fetchEvents(randomSubject).events.forEach {
        println(
            Json.parseToJsonElement(
                it.data.toString(
                    UTF_8
                )
            )
        )
    }

    println()

    testOptimisticLocking()

}

fun testOptimisticLocking() {
    FDB.selectAPIVersion(730)
    val db = FDB.instance().open("/etc/foundationdb/fdb.cluster")
    val store = ClassicEventStore(db)

    val subjectId = "subject:${UUID.randomUUID()}"

    // 1️⃣ First append (expectedVersion = null)
    val firstEvents = listOf(
        ClassicEventStore.Event(UUID.randomUUID(), subjectId, "USER_CREATED", """{"id":"$subjectId"}""".toByteArray())
    )
    store.appendWithExpectedVersion(subjectId, null, firstEvents)
    println("First append OK")

    // Fetch version after first append
    val afterFirst = store.fetchEvents(subjectId)
    val v1 = afterFirst.lastVersion
    println("Version after first append: $v1")

    // 2️⃣ Second append with correct expectedVersion
    val secondEvents = listOf(
        Event(
            id = UUID.randomUUID(),
            subject = subjectId,
            type = "USER_LOCKED",
            data = """{"locked":true}""".toByteArray()
        )
    )
    store.appendWithExpectedVersion(subjectId, v1, secondEvents)
    println("Second append OK")

    // Fetch version after second append
    val afterSecond = store.fetchEvents(subjectId)
    val v2 = afterSecond.lastVersion
    println("Version after second append: $v2")

    // 3️⃣ Try stale write (still using v1, but store is already at v2)
    try {
        val staleEvents = listOf(
            Event(
                id = UUID.randomUUID(),
                subject = subjectId,
                type = "USER_DELETED",
                data = """{"deleted":true}""".toByteArray()
            )
        )
        store.appendWithExpectedVersion(subjectId, v1, staleEvents)
        println("❌ Expected ConcurrencyException but append succeeded")
    } catch (ex: CompletionException) {
        println("✅ ConcurrencyException correctly thrown on stale write")
    }

    println()

    println("Fetching all")

    store.fetchAll().forEach {
        println("${it.id} : type->${it.type}")
    }
}