package com.cassisi.openeventstore.core.dcb

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToByteArray
import kotlinx.serialization.*
import org.apache.avro.Schema
import java.time.Instant
import java.util.UUID
import kotlin.annotation.AnnotationRetention.RUNTIME
import kotlin.annotation.AnnotationTarget.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.*

class AvroFdbStore(
    private val factStore: FactStore
) {

    private val schemaMapping: MutableMap<String, Schema> = mutableMapOf()
    private val typeMapping: MutableMap<String, KClass<*>> = mutableMapOf()

    private val serializers: MutableMap<String, FactSerializerAndDeserializer<out Any>> = mutableMapOf()

    fun register(eventType: String, type: Schema) {
        schemaMapping[eventType] = type
    }

    fun registerAll() {
        this.serializers["USER_ONBOARDED"] = UserOnboardSerializer()
    }

    suspend fun <T : Any> append(event: T) {
        val classType = event::class
        val factType = classType.findAnnotation<FactType>()?.name ?: classType.simpleName ?: throw IllegalArgumentException("Cannot extract simple name")

        val schema = schemaMapping[factType]!!
       // val payloadBytes = Avro.encodeToByteArray(schema, event)
        val serializer: FactSerializerAndDeserializer<out Any> = serializers[factType]!!
        with(serializer) {
            event.serialize()
        }

        val subjectType = classType.findAnnotation<SubjectType>()?.value ?: throw IllegalArgumentException("Missing SubjectType")
        println(subjectType)

        val subjectId = classType
            .memberProperties
            .first { it.hasAnnotation<SubjectId>() }
            .let { (it as KProperty1<Any, *>).get(event) }
            .toString()

        val tags = classType.memberProperties
            .mapNotNull {
                val key = it.findAnnotation<Tag>()?.name
                if (key != null) {
                    val value = (it as KProperty1<Any, *>).get(event).toString()
                    Pair(key, value)
                } else {
                    null
                }
            }
            .toMap()


        val fact = Fact(
            id = UUID.randomUUID(),
            type = factType,
            payload = payloadBytes,
            subject = Subject(
                type = subjectType,
                id = subjectId
            ),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = tags
        )

        println(fact)

        factStore.append(fact)
    }


    suspend fun readSubject(type: String, id: String): List<*> {
        return factStore.findBySubject(type, id)
            .map {
                val schema = schemaMapping[it.type]!!
                Avro.decodeFromByteArray<Any>(schema, it.payload)
            }
    }

}


@Serializable
@FactType("USER_ONBOARDED")
@SubjectType("USER")
data class UserOnboarded(
    @SerialName("userId")
    @Contextual
    @SubjectId
    val userId: UUID,

    @SerialName("username")
    @Tag("username")
    val username: String,

    @Contextual
    @SerialName("onboardedAt")
    val onboardedAt: Instant,
)


@Serializable
@FactType("USERNAME_CHANGED")
@SubjectType("USER")
data class UsernameChanged(
    @SerialName("userId")
    @Contextual
    @SubjectId
    val userId: UUID,

    @SerialName("username")
    @Tag("username")
    val username: String,

    @Contextual
    @SerialName("onboardedAt")
    val onboardedAt: Instant,
)

@Retention(RUNTIME)
@Target(CLASS)
annotation class FactType(val name: String)

@Retention(RUNTIME)
@Target(PROPERTY)
annotation class Tag(val name: String)

@Retention(RUNTIME)
@Target(CLASS)
annotation class SubjectType(val value: String)

@Retention(RUNTIME)
@Target(PROPERTY)
annotation class SubjectId


interface FactSerializerAndDeserializer<T> {

    fun T.serialize(): ByteArray

    fun ByteArray.deserialize(): T

}

class UserOnboardSerializer : FactSerializerAndDeserializer<UserOnboarded> {

    override fun UserOnboarded.serialize(): ByteArray =
        Avro.encodeToByteArray(this)


    override fun ByteArray.deserialize(): UserOnboarded =
        Avro.decodeFromByteArray(this)

}

