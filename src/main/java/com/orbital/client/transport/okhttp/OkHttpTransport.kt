package com.orbital.client.transport.okhttp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.orbital.client.OrbitalTransport
import com.orbital.client.QueryFailedException
import com.orbital.client.QuerySpec
import com.orbital.client.Verb
import mu.KotlinLogging
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.net.URI

class OkHttpTransport(
    private val address: String,
    private val client: OkHttpClient = OkHttpClient(),
    private val preferStreaming: Boolean = false
) :
    OrbitalTransport {

    private val uri: URI = URI.create(address)

    companion object {
        private val logger = mu.KotlinLogging.logger {}
    }

    override fun execute(querySpec: QuerySpec): Publisher<ByteArray> {
        logger.debug { "Query ${querySpec.clientQueryId}: ${querySpec.query}" }
        return when (querySpec.verb) {
            Verb.FIND -> {
                if (preferStreaming) {
                    executeStreamingRequest(querySpec)
                } else {
                    executeRequestResponseQuery(querySpec)
                }
            }

            Verb.STREAM -> executeStreamingRequest(querySpec)
        }


    }

    private fun executeStreamingRequest(querySpec: QuerySpec): Publisher<ByteArray> {
        val websocketScheme = when (uri.scheme) {
            "http" -> "ws"
            "https" -> "wss"
            "ws" -> "ws"
            "wss" -> "wss"
            else -> error("Can't map protocol ${uri.scheme} to a websocket protocol - expected http or https")
        }
        val requestUrl =
            address.replace(uri.scheme, websocketScheme)
                .removeSuffix("/") + "/api/query/taxiql"

        logger.debug { "Query ${querySpec.clientQueryId} sending over websocket to $requestUrl" }
        val request = Request.Builder()
            .url(requestUrl)
            .build()

        val listener = QueryResultWebsocketListener(querySpec)
        client.newWebSocket(request, listener)
        return listener.messages
    }

    private fun executeRequestResponseQuery(querySpec: QuerySpec): Publisher<ByteArray> {

        val requestUrl = address.removeSuffix("/") + "/api/taxiql"
        logger.debug { "Query ${querySpec.clientQueryId} sending over http to $requestUrl" }

        val request = Request.Builder()
            .url(requestUrl)
            .addHeader("Accept", "application/json")
            .post(
                querySpec.query
                    .toRequestBody("application/taxiql".toMediaType())
            )
            .build()


        return Flux.create<ByteArray> { sink ->
            val response = client.newCall(request).execute()

            if (response.isSuccessful) {
                sink.next(response.body!!.bytes())
            } else {
                sink.error(QueryFailedException(response.code, response.message))
            }

            sink.complete()
        }
    }
}

private class QueryResultWebsocketListener(
    private val querySpec: QuerySpec,
    private val objectMapper: ObjectMapper = jacksonObjectMapper()
) : WebSocketListener() {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val sink = Sinks.many().multicast().onBackpressureBuffer<ByteArray>()
    val messages = sink.asFlux()

    override fun onOpen(webSocket: WebSocket, response: Response) {
        logger.debug { "Websocket for query ${querySpec.clientQueryId} opened - sending query" }
        val websocketQuery = WebsocketQuery(querySpec.clientQueryId, querySpec.query)
        if (!webSocket.send(objectMapper.writeValueAsString(websocketQuery))) {
            logger.warn { "Failed to send query for query ${querySpec.clientQueryId} - has the websocket closed?" }
        }
    }

    override fun onMessage(webSocket: WebSocket, text: String) {
        logger.debug { "Query ${querySpec.clientQueryId} received a message" }
        sink.emitNext(text.toByteArray(), Sinks.EmitFailureHandler.FAIL_FAST)
    }

    override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
        logger.debug { "Query ${querySpec.clientQueryId} received a message" }
        sink.emitNext(bytes.toByteArray(), Sinks.EmitFailureHandler.FAIL_FAST)
    }

    override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
        logger.error(t) { "Query ${querySpec.clientQueryId} has failed" }
        super.onFailure(webSocket, t, response)
    }

    override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
        logger.debug { "Websocket for query ${querySpec.clientQueryId} is closed - ending stream" }
        sink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST)
    }
}

private data class WebsocketQuery(
    val clientQueryId: String,
    val query: String,
    val resultMode: ResultMode = ResultMode.RAW
) {
    enum class ResultMode {
        /**
         * Raw results
         */
        RAW,

        /**
         * Exclude type information for each attribute in 'results'
         */
        @Deprecated("Use TYPED instead", replaceWith = ReplaceWith("ResultMode.TYPED"))
        SIMPLE,

        /**
         * Provide type metadata in results at a row level
         */
        TYPED,

        /**
         * Include type information for each attribute included in 'results'
         */
        VERBOSE;
    }

}


fun http(address: String): OkHttpTransport {
    return OkHttpTransport(address)
}

fun httpStream(address: String): OkHttpTransport {
    return OkHttpTransport(address, preferStreaming = true)
}
