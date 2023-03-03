package com.orbital.client

import com.fasterxml.jackson.core.type.TypeReference

class CriteriaBuilder<C, S, T>(
    private val queryBuilder: QueryBuilder<S, T>,
    private val fieldTypeRef: TypeReference<C>
) {

    fun eq(other: C): QueryBuilder<S, T> = setCriteria(Operator.Equal, other)

    private fun setCriteria(operator: Operator, value: C): QueryBuilder<S, T> =
        queryBuilder.addCriterion(criteria(operator, value))

    private fun criteria(operator: Operator, value: C): Criterion {
        return SimpleCriterion(this.fieldTypeRef, operator, value)
    }

    fun then(): QueryBuilder<S, T> {
        return queryBuilder
    }


    data class SimpleCriterion<C>(
        val sourceType: TypeReference<C>,
        val operator: Operator,
        val value: C
    ) : Criterion {
        override fun asTaxi(generator: TaxiGenerator): String {
            val criteriaType = generator.getTypeName(sourceType.type)

            TODO("Not yet implemented")
        }

    }
}


interface Criterion {
    fun asTaxi(generator: TaxiGenerator): String
}

// See FormulaOperator in taxi
enum class Operator(val symbol: String) {
    Add(
        "+"
    ),
    Subtract("-"),
    Multiply("*"),
    Divide("/"),
    GreaterThan(">"),
    LessThan("<"),
    GreaterThanOrEqual(">="),
    LessThanOrEqual("<="),
    LogicalAnd("&&"),
    LogicalOr("||"),
    Equal("=="),
    NotEqual("!=");


}