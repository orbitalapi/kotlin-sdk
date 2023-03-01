package com.orbital.client

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import lang.taxi.annotations.DataType
import lang.taxi.generators.java.DefaultTypeMapper
import lang.taxi.types.Field
import org.reactivestreams.Publisher
import java.lang.reflect.AnnotatedElement
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.WildcardType


inline fun <reified T> find(): QueryBuilder<T, T> {
    return QueryBuilder(T::class.java, T::class.java, Verb.FIND, jacksonTypeRef<T>(), jacksonTypeRef<T>())
}

inline fun <reified T> stream(): QueryBuilder<T, T> {
    return QueryBuilder(T::class.java, T::class.java, Verb.STREAM, jacksonTypeRef<T>(), jacksonTypeRef<T>())
}

enum class Verb(val syntax: String) {
    FIND("find"),
    STREAM("stream")
}

data class QuerySpec(
    val query: String,
    val verb: Verb,
    val targetType: Class<*>,
    val clientQueryId: String = Ids.id("qry-")
)

class QueryBuilder<S, T>(
    val sourceType: Class<S>,
    val targetType: Class<T>,
    val verb: Verb,
    val sourceTypeRef: TypeReference<S>?,
    val targetTypeReference: TypeReference<T>?
) {
    private val typeMapper = DefaultTypeMapper()
    inline fun <reified D> asA(): QueryBuilder<S, D> {
        return QueryBuilder(
            sourceType = sourceType, targetType = D::class.java, verb = verb,
            sourceTypeRef,
            jacksonTypeRef<D>()
        )
    }

    // IDea: We're returning Publsher<ByteArray> here, so that
    // our core module doesn't need to bring in converter dependencies.
    // Then, add a converter (possibly bundled in the transport),
    // to get richer deserialization through extension methods.
    // Will it work?
    // Let's find out.
    fun sendQuery(transport: OrbitalTransport): Publisher<ByteArray> {
        val taxi = buildTaxiStatement()
        val query = QuerySpec(
            taxi,
            verb, targetType
        )
        return transport.execute(query)
    }

    private fun buildTaxiStatement(): String {
        val sourceDataType = getSourceDataType()
        val verbClause = "${verb.syntax} { $sourceDataType } "
        val projection = if (this.targetTypeReference != sourceTypeRef) {
            buildProjectionClause()
        } else ""

        return (verbClause + projection).trim()
    }

    private data class QueryType(
        val typeName: String,
        val collection: Boolean,
    ) {
        override fun toString(): String {
            return if (collection) {
                "$typeName[]"
            } else {
                typeName
            }
        }
    }

    private fun resolveCollection(type: Type): Pair<Type, Boolean> {
        val isCollection =
            type is ParameterizedType && Collection::class.java.isAssignableFrom(type.rawType as Class<*>)
        return when {
            isCollection -> {
                val (collectionType, _) = resolveCollection((type as ParameterizedType).actualTypeArguments[0])
                collectionType to true
            }

            type is WildcardType -> resolveCollection(type.upperBounds[0])
            else -> type to false
        }
    }

    private fun getSourceDataType(type: Type = sourceTypeRef!!.type): QueryType {
        val (underlyingType, isCol) = resolveCollection(type)
        val isCollection =
            type is ParameterizedType && Collection::class.java.isAssignableFrom(type.rawType as Class<*>)
        return when {
            isCollection -> {
                val collectionType = getSourceDataType((type as ParameterizedType).actualTypeArguments[0])
                collectionType.copy(collection = true)
            }

            type is WildcardType -> {
                val upperBoundType = type.upperBounds[0]
                getSourceDataType(upperBoundType)
            }

            else -> {
                val dataTypeAnnotation = typeMapper.getDataTypeAnnotation(type as AnnotatedElement)
                    ?: error("Type ${sourceType.simpleName} does not have a ${DataType::class.simpleName} annotation")
                QueryType(dataTypeAnnotation.value, false)
            }
        }
    }

    private fun buildProjectionClause(): String {
        // If there's an annotation declaring a type for the result,
        // just use that.
        val (type, isCollection) = resolveCollection(targetTypeReference!!.type)
        val collectionSuffix = if (isCollection || verb == Verb.STREAM) {
            "[]"
        } else ""
        val targetDataType = typeMapper.getDataTypeAnnotation(type as AnnotatedElement)
        if (targetDataType != null) {
            return targetDataType.value + collectionSuffix
        }

        // Otherwise, build an anonymous projection type.
        val fields = typeMapper.mapTaxiFields(type as Class<*>, "", mutableSetOf())
        val clause = buildProjectionClauseFor(fields).joinToString("\n")

        return """as {
            |$clause
            |}$collectionSuffix
        """.trimMargin()
    }

    private fun buildProjectionClauseFor(fields: List<Field>): List<String> {
        return fields.map { field ->
            // TODO : Support nested anonymous types
            "${field.name}: ${field.type.toQualifiedName().parameterizedName}"
        }
    }

}