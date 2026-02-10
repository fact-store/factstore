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
