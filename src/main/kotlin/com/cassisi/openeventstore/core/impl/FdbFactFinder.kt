package com.cassisi.openeventstore.core.impl

import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.tuple.Tuple
import com.cassisi.openeventstore.core.Fact
import com.cassisi.openeventstore.core.FactFinder
import com.cassisi.openeventstore.core.FactQueryItem
import com.cassisi.openeventstore.core.TagQuery
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
    private val tagsTypeIndexSubspace = fdbFactStore.tagsTypeIndexSubspace
    private val typesIndexSubspace = fdbFactStore.eventTypeIndexSubspace

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

    override suspend fun findByTagQuery(query: TagQuery): List<Fact> {

        return db.readAsync { tr ->
            val snapshot = tr.snapshot()
            val queryItemFutures = query.queryItems
                .map { queryItem ->
                    // map query item to list of fact IDs
                    queryItem.resolveFactIds(snapshot)
                }

                CompletableFuture.allOf(*queryItemFutures.toTypedArray()).thenCompose {
                val allFactIds: Set<UUID> = queryItemFutures
                    .flatMap { it.getNow(emptySet()) }
                    .toSet() // OR semantics = union

                val loadFutures: List<CompletableFuture<FdbFact?>> = allFactIds.map { snapshot.loadFact(it) }

                CompletableFuture.allOf(*loadFutures.toTypedArray()).thenApply {
                    loadFutures
                        .mapNotNull { it.getNow(null) }
                        .sortedBy { it.positionTuple }
                        .map { it.fact }
                }
            }
        }.await()
    }

    private fun FactQueryItem.resolveFactIds(tr: ReadTransaction): CompletableFuture<Set<UUID>> {
        val hasTags = this.tags.isNotEmpty()
        val hasTypes = this.types.isNotEmpty()

        return when {
            hasTags && hasTypes -> {
                // use composite "type+tag" index
                val futures: List<CompletableFuture<Set<UUID>>> = types.map { type ->
                    val tagFutures = tags.map { tag ->
                        val range = tagsTypeIndexSubspace.range(Tuple.from(type, tag.first, tag.second))
                        tr.getRange(range).asList().thenApply { keyValues ->
                            keyValues.map {
                                val tuple = tagsTypeIndexSubspace.unpack(it.key)
                                tuple.getUUID(tuple.size() - 1)
                            }.toSet()
                        }
                    }

                    // we want to logically "AND" the result of the tag queries
                    CompletableFuture.allOf(*tagFutures.toTypedArray()).thenApply {
                        tagFutures
                            .map { it.getNow(emptySet()) } // Extract the result of each CompletableFuture
                            .reduce { acc, set -> acc.intersect(set) } // Reduce by intersecting each set
                            .orEmpty() // If there are no sets to intersect, return an empty set
                    }
                }

                // we finally union the found UUIDs
                CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                    futures
                        .map { it.getNow(emptySet()) }
                        .reduce { acc, set -> acc.union(set)  }
                        .orEmpty()
                }
            }

            hasTags -> {
                val futures: List<CompletableFuture<Set<UUID>>> = tags.map { tag ->
                    val range = tagsIndexSubspace.range(Tuple.from(tag.first, tag.second))
                    tr.getRange(range).asList().thenApply { keyValues ->
                        keyValues.map {
                            val tuple = tagsIndexSubspace.unpack(it.key)
                            tuple.getUUID(tuple.size() - 1)
                        }.toSet() // Convert to a Set to easily combine results
                    }
                }
                // After all futures complete, perform the union of the sets
                CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                    // Union the sets from all futures
                    futures
                        .map { it.getNow(emptySet()) } // Extract the result of each CompletableFuture
                        .reduce { acc, set -> acc.union(set) } // Union all sets to get all matching fact IDs
                        .orEmpty() // Return empty set if no sets are present
                }
            }

            hasTypes -> {
                val futures: List<CompletableFuture<Set<UUID>>> = types.map { type ->
                    val range = typesIndexSubspace.range(Tuple.from(type))
                    tr.getRange(range).asList().thenApply { keyValues ->
                        keyValues.map {
                            val tuple = typesIndexSubspace.unpack(it.key)
                            tuple.getUUID(tuple.size() - 1)
                        }.toSet() // Convert to a Set to easily combine results
                    }
                }
                // After all futures complete, perform the union of the sets
                CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                    futures
                        .map { it.getNow(emptySet()) } // Extract the result of each CompletableFuture
                        .reduce { acc, set -> acc.union(set) } // Union all sets to get all matching fact IDs
                        .orEmpty() // Return empty set if no sets are present
                }
            }

            else -> CompletableFuture.completedFuture(emptySet())
        }

    }

    private fun ReadTransaction.loadFact(factId: UUID): CompletableFuture<FdbFact?> =
        with(fdbFactStore) {
            this@loadFact.loadFact(factId)
        }

}
