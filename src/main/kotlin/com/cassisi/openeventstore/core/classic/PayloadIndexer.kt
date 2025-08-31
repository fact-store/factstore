package com.cassisi.openeventstore.core.classic

import kotlinx.serialization.json.*

interface PayloadIndexer {

    fun extractIndexableFields(eventType: String, payload: ByteArray): Map<String, Any>

}

class JSONPayloadIndexerKotlinx : PayloadIndexer {

    override fun extractIndexableFields(eventType: String, payload: ByteArray): Map<String, Any> {
        val jsonElement = Json.parseToJsonElement(payload.decodeToString())
        return flattenJson(jsonElement)
    }

    private fun flattenJson(element: JsonElement, prefix: String = ""): Map<String, Any> {
        val result = mutableMapOf<String, Any>()

        when (element) {
            is JsonObject -> {
                element.forEach { (key, value) ->
                    val newPrefix = if (prefix.isEmpty()) key else "$prefix.$key"
                    result.putAll(flattenJson(value, newPrefix))
                }
            }
            is JsonArray -> {
                element.forEachIndexed { index, value ->
                    val newPrefix = "$prefix[$index]"
                    result.putAll(flattenJson(value, newPrefix))
                }
            }
            is JsonPrimitive -> {
                val value: Any = when {
                    element.isString -> element.content
                    element.booleanOrNull != null -> element.boolean
                    element.longOrNull != null -> element.long
                    element.doubleOrNull != null -> element.double
                    else -> element.content
                }
                result[prefix] = value
            }
            JsonNull -> result[prefix] = "null"
        }

        return result
    }
}