package com.cassisi.openeventstore.core

interface FactAppender {

    suspend fun append(fact: Fact)

    suspend fun append(facts: List<Fact>)

}