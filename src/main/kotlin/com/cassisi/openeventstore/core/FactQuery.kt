package com.cassisi.openeventstore.core

sealed interface FactQuery

data class FactQueryItem(
    val types: List<String>,
    val tags: List<Pair<String, String>>
) {
    init {
        require(!(types.isEmpty() && tags.isEmpty())) { "At least types or tags must be defined!" }
    }
}

data class TagQuery(
    val queryItems: List<FactQueryItem>
) : FactQuery