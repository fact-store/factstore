package org.factstore.core

/**
 * Base exception type for all FactStore-related errors.
 *
 * This exception represents failures that indicate incorrect usage,
 * violated invariants, or unrecoverable errors within the FactStore.
 */
sealed class FactStoreException(message: String) : RuntimeException(message)

/**
 * Base type for exceptions caused by invalid client requests.
 *
 * These exceptions indicate that the provided input violates fundamental
 * constraints or invariants of the FactStore and cannot be processed
 * successfully without modifying the request.
 */
sealed class InvalidAppendRequestException(message: String) : FactStoreException(message)

/**
 * Thrown when one or more facts with the same [FactId] already exist in the store.
 *
 * Fact identifiers are required to be globally unique. Attempting to append
 * facts with identifiers that have already been used violates this invariant
 * and results in a permanent failure.
 */
class DuplicateFactIdException(val factIds: List<FactId>) : InvalidAppendRequestException("FactId(s) $factIds already exists")
