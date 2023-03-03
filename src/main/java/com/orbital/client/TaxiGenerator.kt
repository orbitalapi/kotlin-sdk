package com.orbital.client

import lang.taxi.annotations.DataType
import lang.taxi.generators.java.DefaultTypeMapper
import lang.taxi.generators.java.TypeMapper
import java.lang.reflect.AnnotatedElement
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.WildcardType

class TaxiGenerator(private val typeMapper:DefaultTypeMapper = DefaultTypeMapper()) : TypeMapper by typeMapper {

    fun mapTaxiFields(
        javaClass: Class<*>,
        defaultNamespace: String,
        existingTypes: MutableSet<lang.taxi.types.Type>
    ): List<lang.taxi.types.Field> = typeMapper.mapTaxiFields(javaClass, defaultNamespace, existingTypes)
    fun getDataTypeAnnotation(element: AnnotatedElement) = typeMapper.getDataTypeAnnotation(element)
    fun resolveCollection(type: Type): Pair<Type, Boolean> {
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

    internal fun getTypeName(type: Type): QueryType {
        val (underlyingType, isCol) = resolveCollection(type)
        val isCollection =
            type is ParameterizedType && Collection::class.java.isAssignableFrom(type.rawType as Class<*>)
        return when {
            isCollection -> {
                val collectionType = getTypeName((type as ParameterizedType).actualTypeArguments[0])
                collectionType.copy(collection = true)
            }

            type is WildcardType -> {
                val upperBoundType = type.upperBounds[0]
                getTypeName(upperBoundType)
            }

            else -> {
                val dataTypeAnnotation = typeMapper.getDataTypeAnnotation(type as AnnotatedElement)
                    ?: error("Type ${type.typeName} does not have a ${DataType::class.simpleName} annotation")
                QueryType(dataTypeAnnotation.value, false)
            }
        }
    }
}