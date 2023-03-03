package com.orbital.client

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import lang.taxi.annotations.DataType
import lang.taxi.generators.java.DefaultTypeMapper
import lang.taxi.types.Field
import lang.taxi.utils.log
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

    private val generator:TaxiGenerator = TaxiGenerator()
    private var criteria:Criterion? = null

    fun addCriterion(criterion: Criterion):QueryBuilder<S,T> {
        if (this.criteria != null) {
            error("Criteria has already been set, instead of overwriting, compose the criteria with and/or")
        }
        this.criteria = criterion
        return this
    }

    inline fun <reified C> where():CriteriaBuilder<C,S,T> {
        val vlazz = C::class
        log().info(vlazz.simpleName)
        return CriteriaBuilder(this, jacksonTypeRef())
    }
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
        val sourceDataType = generator.getTypeName(sourceTypeRef!!.type)
        val criteria = this.criteria?.asTaxi(generator)

        val verbClause = "${verb.syntax} { $sourceDataType } "
        val projection = if (this.targetTypeReference != sourceTypeRef) {
            buildProjectionClause()
        } else ""

        return (verbClause + projection).trim()
    }





    private fun buildProjectionClause(): String {
        // If there's an annotation declaring a type for the result,
        // just use that.
        val (type, isCollection) = generator.resolveCollection(targetTypeReference!!.type)
        val collectionSuffix = if (isCollection || verb == Verb.STREAM) {
            "[]"
        } else ""
        val targetDataType = generator.getDataTypeAnnotation(type as AnnotatedElement)
        if (targetDataType != null) {
            return targetDataType.value + collectionSuffix
        }

        // Otherwise, build an anonymous projection type.
        val fields = generator.mapTaxiFields(type as Class<*>, "", mutableSetOf())
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

    companion object {


    }

}

internal data class QueryType(
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
