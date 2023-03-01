package com.orbital.client.converter

import com.orbital.client.FirstName
import com.orbital.client.LastName
import com.orbital.client.MockTransport
import com.orbital.client.Person
import com.orbital.client.find
import io.kotest.core.spec.style.DescribeSpec
import lang.taxi.generators.kotlin.TypeAliasRegister
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.test.test

class JacksonQueryResultConverterTest : DescribeSpec({

    it("converts a flux of json responses") {
        TypeAliasRegister.registerPackage(Person::class)


        data class Target(
            val firstName: FirstName,
            val lastName: LastName
        )

        val transport = MockTransport()
        find<Person>()
            .asA<Target>()
            .run(transport)
            .toFlux()
            .test()
            .expectSubscription()
            .then {
                transport.emit("""{ "firstName" : "Jimmy", "lastName" : "Spitts" }""")
            }
            .expectNextMatches { next ->
                next == Target("Jimmy", "Spitts")
            }
            .thenCancel()
            .verify()
    }

})
