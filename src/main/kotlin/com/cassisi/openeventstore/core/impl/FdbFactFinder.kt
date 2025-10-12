package com.cassisi.openeventstore.core.impl

import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.tuple.Tuple
import com.cassisi.openeventstore.core.Fact
import com.cassisi.openeventstore.core.FactFinder
import kotlinx.coroutines.future.await
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture

class FdbFactFinder(private val fdbFactStore: FdbFactStore) : FactFinder {

    private val db = fdbFactStore.db

    private val factIdSubspace = fdbFactStore.factIdSubspace

    private val createdAtIndexSubspace = fdbFactStore.createdAtIndexSubspace
    private val subjectIndexSubspace = fdbFactStore.subjectIndexSubspace
    private val tagsIndexSubspace = fdbFactStore.tagsIndexSubspace

    override suspend fun findById(factId: UUID): Fact? {
        return db.readAsync { tr ->
            val factIdKey = factIdSubspace.pack(Tuple.from(factId))
            tr[factIdKey].thenCompose { exists ->
                if (exists == null) {
                    CompletableFuture.completedFuture(null)
                } else {
                    with(fdbFactStore) {
                        tr.loadFact(factId)
                    }
                }
            }
        }.await()?.fact
    }


    override suspend fun existsById(factId: UUID): Boolean {
        return db.readAsync { tr ->
            val factIdKey = factIdSubspace.pack(Tuple.from(factId))
            tr[factIdKey]
        }.await() != null
    }

    override suspend fun findInTimeRange(start: Instant, end: Instant): List<Fact> {
        val startTuple = Tuple.from(start.epochSecond, start.nano)
        val endTuple = Tuple.from(end.epochSecond, end.nano)

        return db.readAsync { tr ->
            val begin = createdAtIndexSubspace.pack(startTuple)
            val endKey = createdAtIndexSubspace.pack(endTuple)

            tr.getRange(begin, endKey).asList().thenCompose { kvs ->
                val factFutures: List<CompletableFuture<FdbFact?>> = kvs.map { kv ->
                    val tuple = createdAtIndexSubspace.unpack(kv.key)
                    val factId = tuple.getUUID(tuple.size() - 1)
                    tr.loadFact(factId)
                }

                // wait for all facts to complete
                CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                    factFutures.mapNotNull { it.getNow(null)?.fact }
                }
            }
        }.await()
    }

    override suspend fun findBySubject(subjectType: String, subjectId: String): List<Fact> {
        return db.readAsync { tr ->
            val subjectRange = subjectIndexSubspace.range(Tuple.from(subjectType, subjectId))
            tr.getRange(subjectRange).asList().thenCompose { kvs ->
                val factFutures: List<CompletableFuture<FdbFact?>> = kvs.map { kv ->
                    val tuple = subjectIndexSubspace.unpack(kv.key)
                    val factId = tuple.getUUID(tuple.size() - 1)
                    tr.loadFact(factId)
                }

                CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                    factFutures.mapNotNull { it.getNow(null)?.fact }
                }
            }
        }.await()
    }

    override suspend fun findByTags(tags: List<Pair<String, String>>): List<Fact> {
        if (tags.isEmpty()) return emptyList()
        return db.readAsync { tr ->
            // For each (key, value) pair, get matching factIds
            val tagFutures: List<CompletableFuture<Set<UUID>>> = tags.map { (key, value) ->
                val range = tagsIndexSubspace.range(Tuple.from(key, value))
                tr.getRange(range).asList().thenApply { kvs ->
                    kvs.mapTo(mutableSetOf()) { kv ->
                        val tuple = tagsIndexSubspace.unpack(kv.key)
                        tuple.getUUID(tuple.size() - 1)
                    }
                }
            }

            // Once all tag lookups finish, union them and load facts
            CompletableFuture.allOf(*tagFutures.toTypedArray()).thenCompose {
                val allFactIds: Set<UUID> = tagFutures
                    .flatMap { it.getNow(emptySet()) }
                    .toSet() // OR semantics = union

                val loadFutures: List<CompletableFuture<FdbFact?>> = allFactIds.map { tr.loadFact(it) }

                CompletableFuture.allOf(*loadFutures.toTypedArray()).thenApply {
                    loadFutures
                        .mapNotNull { it.getNow(null) }
                        .sortedBy { it.positionTuple }
                        .map { it.fact }
                }
            }
        }.await()
    }

    private fun ReadTransaction.loadFact(factId: UUID): CompletableFuture<FdbFact?> =
        with(fdbFactStore) {
            this@loadFact.loadFact(factId)
        }

}
