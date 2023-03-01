package com.orbital.client

import org.reactivestreams.Publisher

interface OrbitalTransport {
    fun execute(querySpec: QuerySpec): Publisher<ByteArray>
}

class QueryFailedException(
    statusCode: Int,
    message: String
) : RuntimeException(message)