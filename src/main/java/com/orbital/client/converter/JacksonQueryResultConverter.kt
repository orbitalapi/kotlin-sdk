package com.orbital.client.converter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.orbital.client.OrbitalTransport
import com.orbital.client.QueryBuilder
import org.reactivestreams.Publisher
import reactor.kotlin.core.publisher.toFlux

object JacksonQueryResultConverter {
    val converter = jacksonObjectMapper()
}


inline fun <S, reified T> QueryBuilder<S, T>.run(
    transport: OrbitalTransport,
    objectMapper: ObjectMapper = JacksonQueryResultConverter.converter
): Publisher<T> {
    return this.sendQuery(transport)
        .toFlux()
        .map { byteArray ->
            objectMapper.readValue(byteArray, this.targetTypeReference!!)
        }
}