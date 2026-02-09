package org.factstore.server.http

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import org.factstore.core.AppendCondition
import org.factstore.core.AppendRequest
import org.factstore.core.AppendResult
import org.factstore.core.Fact
import org.factstore.core.FactId
import org.factstore.core.FactPayload
import org.factstore.core.IdempotencyKey
import org.factstore.core.SubjectRef
import org.factstore.core.TagKey
import org.factstore.core.TagOnlyQueryItem
import org.factstore.core.TagQuery
import org.factstore.core.TagQueryItem
import org.factstore.core.TagValue
import org.factstore.core.toFactId
import org.factstore.core.toFactType
import org.factstore.core.toTagKey
import org.factstore.core.toTagValue
import org.factstore.server.FactStoreProvider
import java.time.Instant
import java.util.UUID

@Path("/v1/fact-store/{factStoreName}/facts/append")
class AppendResource(
    private val factStoreProvider: FactStoreProvider
) {

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    suspend fun appendFacts(
        @PathParam("factStoreName") factStoreName: String,
        httpRequest: AppendHttpRequest
    ): Response {
        val factStore = factStoreProvider.findByName(factStoreName)

        val appendRequest = httpRequest.toAppendRequest()
        val appendResult = factStore.append(appendRequest)
        return appendResult.toResponse()
    }

}

private fun AppendResult.toResponse(): Response {
    return Response.ok(this::class.qualifiedName).build()
}

data class AppendHttpRequest(
    val facts: List<FactHttp>,
    val idempotencyKey: UUID? = null,
    val condition: AppendConditionHttp? = null
)

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(
        value = AppendConditionHttp.None::class,
        name = "none"
    ),
    JsonSubTypes.Type(
        value = AppendConditionHttp.ExpectedLastFact::class,
        name = "expectedLastFact"
    ),
    JsonSubTypes.Type(
        value = AppendConditionHttp.ExpectedMultiSubjectLastFact::class,
        name = "expectedMultiSubjectLastFact"
    ),
    JsonSubTypes.Type(
        value = AppendConditionHttp.TagQueryBased::class,
        name = "tagQueryBased"
    )
)
sealed interface AppendConditionHttp {

    data object None : AppendConditionHttp

    data class ExpectedLastFact(
        val subjectRef: SubjectRefHttp,
        val expectedLastFactId: UUID?
    ) : AppendConditionHttp

    data class ExpectedMultiSubjectLastFact(
        val expectations: Map<SubjectRefHttp, UUID?>
    ) : AppendConditionHttp

    data class TagQueryBased(
        val failIfEventsMatch: TagQueryHttp,
        val after: UUID?
    ) : AppendConditionHttp
}

data class TagQueryHttp(
    val queryItems: List<TagQueryItemHttp>
)

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(
        value = TagQueryItemHttp.TagOnly::class,
        name = "tagOnly"
    ),
    JsonSubTypes.Type(
        value = TagQueryItemHttp.TagType::class,
        name = "tagType"
    )
)
sealed interface TagQueryItemHttp {

    data class TagType(
        val types: List<String>,
        val tags: Map<String, String>
    ) : TagQueryItemHttp

    data class TagOnly(
        val tags: Map<String, String>
    ) : TagQueryItemHttp
}

data class FactHttp(
    val id: UUID?,
    val type: String,
    val subjectRef: SubjectRefHttp,
    val payload: FactHttpPayload,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap()
)

data class FactHttpPayload(
    val data: ByteArray,
)

data class SubjectRefHttp(
    val type: String,
    val id: String
)

fun AppendHttpRequest.toAppendRequest(): AppendRequest = AppendRequest(
    facts = facts.toFacts(),
    idempotencyKey = idempotencyKey?.let { IdempotencyKey(it) } ?: IdempotencyKey(),
    condition = condition?.toAppendCondition() ?: AppendCondition.None
)

fun AppendConditionHttp.toAppendCondition(): AppendCondition =
    when (this) {
        is AppendConditionHttp.None ->
            AppendCondition.None

        is AppendConditionHttp.ExpectedLastFact ->
            AppendCondition.ExpectedLastFact(
                subjectRef = subjectRef.toSubjectRef(),
                expectedLastFactId = expectedLastFactId?.toFactId()
            )

        is AppendConditionHttp.ExpectedMultiSubjectLastFact ->
            AppendCondition.ExpectedMultiSubjectLastFact(
                expectations = expectations.mapKeys { (subject, _) ->
                    subject.toSubjectRef()
                }.mapValues { (_, factId) ->
                    factId?.toFactId()
                }
            )

        is AppendConditionHttp.TagQueryBased ->
            AppendCondition.TagQueryBased(
                failIfEventsMatch = failIfEventsMatch.toTagQuery(),
                after = after?.toFactId()
            )
    }

fun TagQueryHttp.toTagQuery(): TagQuery =
    TagQuery(
        queryItems = queryItems.map { it.toTagQueryItem() }
    )

fun TagQueryItemHttp.toTagQueryItem(): TagQueryItem =
    when (this) {
        is TagQueryItemHttp.TagOnly ->
            TagOnlyQueryItem(
                tags = tags.entries.map { (k, v) ->
                    k.toTagKey() to v.toTagValue()
                }
            )

        is TagQueryItemHttp.TagType ->
            org.factstore.core.TagTypeItem(
                types = types.map { it.toFactType() },
                tags = tags.entries.map { (k, v) ->
                    k.toTagKey() to v.toTagValue()
                }
            )
    }


fun List<FactHttp>.toFacts() = map { it.toFact() }

fun FactHttp.toFact() = Fact(
    id = id?.toFactId() ?: FactId.generate(),
    type = type.toFactType(),
    payload = payload.toPayload(),
    subjectRef = subjectRef.toSubjectRef(),
    appendedAt = Instant.now(),
    metadata = metadata,
    tags = tags.entries.associate { Pair(it.key.toTagKey(), it.value.toTagValue()) }
)

private fun FactHttpPayload.toPayload(): FactPayload = FactPayload(
    data = data,
)

private fun SubjectRefHttp.toSubjectRef() = SubjectRef(
    type = type,
    id = id
)
