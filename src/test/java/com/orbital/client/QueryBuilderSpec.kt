package com.orbital.client

import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.Matcher
import io.kotest.matchers.MatcherResult
import io.kotest.matchers.should
import lang.taxi.annotations.DataType
import org.reactivestreams.Publisher
import reactor.core.publisher.Sinks
import java.time.LocalDate

@DataType(value = "FirstName")
typealias FirstName = String
@DataType(value = "LastName")
typealias LastName = String
@DataType(value = "DateOfBirth")
typealias DateOfBirth = LocalDate


@DataType("addresses.HouseNumber")
typealias HouseNumber = String

@DataType("addresses.StreetName")
typealias StreetName = String

// To test a nested model
data class Address(
    val houseNumber: HouseNumber,
    val streetName: StreetName
)

@DataType(value = "Person")
data class Person(
    val firstName: FirstName,
    val lastName: LastName,
    val dateOfBirth: DateOfBirth,
    @TaxiExpression("concat(FirstName, ' ', LastName)")
    val fullName: String
)

annotation class TaxiExpression(val value: String)

class QueryBuilderSpec : DescribeSpec({

    data class Target(
        val firstName: FirstName,
        val address: Address
    )
    describe("building a tql query") {
        it("generates a valid query for a find with projection") {
            val transport = MockTransport()
            find<Person>()
                .asA<Target>()
                .sendQuery(transport)

            val expected = """
            find { Person } as {
                address: com.orbital.client.Address
                firstName: FirstName
            }
        """
            transport.capturedQuery.shouldBeEqualIgnoringWhitespace(expected)
        }

        it("generates a valid query for a find of a list") {
            val transport = MockTransport()
            find<List<Person>>()
                .asA<List<Target>>()
                .sendQuery(transport)

            val expected = """
            find { Person[] } as {
                address: com.orbital.client.Address
                firstName: FirstName
            }[]
        """
            transport.capturedQuery.shouldBeEqualIgnoringWhitespace(expected)
        }

        it("generates a valid query for a stream with projection") {
            val transport = MockTransport()
            stream<Person>()
                .asA<Target>()
                .sendQuery(transport)

            val expected = """
            stream { Person } as {
                address: com.orbital.client.Address
                firstName: FirstName
            }[]
        """
            transport.capturedQuery.shouldBeEqualIgnoringWhitespace(expected)
        }

    }




    it("generates a list") {
        find<List<Person>>()
            .asA<List<Target>>()
    }
    it("streams data") {
    }
})

class MockTransport : OrbitalTransport {
    val sink = Sinks.many().unicast().onBackpressureBuffer<String>()

    fun emit(value: String) {
        sink.emitNext(value, Sinks.EmitFailureHandler.FAIL_FAST)
    }


    var capturedQuery: String? = null
        private set

    override fun execute(querySpec: QuerySpec): Publisher<ByteArray> {
        this.capturedQuery = querySpec.query;
        return sink.asFlux()
            .map { it.toByteArray() }

    }
}

fun equalIgnoringWhitespace(other: String): Matcher<String> {
    return object : Matcher<String> {
        override fun test(value: String): MatcherResult {
            return MatcherResult(
                value.withoutWhitespace() == other.withoutWhitespace(),
                { "$other should equal $value" },
                { "$other should equal $value" }
            )
        }

    }

}

fun String?.shouldBeEqualIgnoringWhitespace(other: String): String? {
    this.orEmpty() should equalIgnoringWhitespace(other)
    return this
}

fun String.withoutWhitespace(): String {
    return this
        .lines()
        .map { it.trim().replace(" ", "") }
        .filter { it.isNotEmpty() }
        .joinToString("")
}
