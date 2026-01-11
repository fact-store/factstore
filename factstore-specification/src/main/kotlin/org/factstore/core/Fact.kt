package org.factstore.core

import java.time.Instant
import java.util.*

/**
 * Represents an immutable fact stored in the FactStore.
 *
 * A fact captures something that happened at a specific point in time and is
 * identified by a globally unique [FactId]. Facts are append-only and must not
 * be modified after they have been stored.
 *
 * The payload is treated as opaque binary data by the FactStore. Interpretation
 * and schema validation are the responsibility of the client.
 *
 * @property id the globally unique identifier of the fact
 * @property type the logical type of the fact
 * @property payload the serialized fact payload
 * @property subjectRef the subject the fact is associated with
 * @property createdAt the time the fact was created
 * @property metadata optional metadata associated with the fact
 * @property tags optional tags used for querying and classification
 *
 * @author Domenic Cassisi
 */
data class Fact(
    val id: FactId,
    val type: String,
    val payload: ByteArray,
    val subjectRef: SubjectRef,
    val createdAt: Instant,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap(),
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Fact

        if (id != other.id) return false
        if (type != other.type) return false
        if (!payload.contentEquals(other.payload)) return false
        if (subjectRef != other.subjectRef) return false
        if (createdAt != other.createdAt) return false
        if (metadata != other.metadata) return false
        if (tags != other.tags) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + payload.contentHashCode()
        result = 31 * result + subjectRef.hashCode()
        result = 31 * result + createdAt.hashCode()
        result = 31 * result + metadata.hashCode()
        result = 31 * result + tags.hashCode()
        return result
    }
}

/**
 * Identifies the subject a fact belongs to.
 *
 * Subjects define logical groupings of facts and are commonly used to model
 * entities, aggregates, or other consistency boundaries.
 *
 * @property type the subject type
 * @property id the unique identifier of the subject within its type
 *
 * @author Domenic Cassisi
 */
data class SubjectRef(
    val type: String,
    val id: String
)

/**
 * Globally unique identifier of a fact.
 *
 * FactIds must be unique across the entire FactStore and are used to enforce
 * idempotency and uniqueness guarantees.
 *
 * @property uuid the underlying UUID value
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class FactId(val uuid: UUID) {

    companion object {

        /**
         * Generates a new random [FactId].
         */
        fun generate() = FactId(UUID.randomUUID())

    }
}

/**
 * Converts a [UUID] to a [FactId].
 */
fun UUID.toFactId() = FactId(this)
