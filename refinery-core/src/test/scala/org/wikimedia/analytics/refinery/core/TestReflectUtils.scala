package org.wikimedia.analytics.refinery.core

import org.scalatest.{FlatSpec, Matchers}

object TestObject {
    def apply(s: String): String = {
        "apply " + s
    }

    def otherMethod(i: Int): Int = {
        i + 10
    }
}

class TestReflectUtils extends FlatSpec with Matchers {

    it should "Lookup object by name with apply method name" in {
        val methodMirror = ReflectUtils.getStaticMethodMirror(
            "org.wikimedia.analytics.refinery.core.TestObject"
        )
        methodMirror.symbol.fullName should equal("org.wikimedia.analytics.refinery.core.TestObject.apply")
    }

    it should "Lookup object by name with any method name" in {
        val methodMirror = ReflectUtils.getStaticMethodMirror(
            "org.wikimedia.analytics.refinery.core.TestObject", "otherMethod"
        )
        methodMirror.symbol.fullName should equal("org.wikimedia.analytics.refinery.core.TestObject.otherMethod")
    }

    it should "MethodMirror returned should be callable" in {
        val methodMirror = ReflectUtils.getStaticMethodMirror(
            "org.wikimedia.analytics.refinery.core.TestObject"
        )
        methodMirror("TEST") should equal ("apply TEST")
    }
}