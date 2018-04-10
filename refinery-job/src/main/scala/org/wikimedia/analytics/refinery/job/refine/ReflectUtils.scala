package org.wikimedia.analytics.refinery.job.refine

import scala.reflect.runtime.universe

object ReflectUtils {

    /**
      * Given a fully qualified String package.ObjectName and String method name, this
      * Function will return a scala.reflect.runtime.universe.MethodMirror that can be
      * used for calling the method on the object.  Note that MethodMirror is not a direct
      * reference to the actual method, and as such does not have compile time type
      * and signature checking.  You must ensure that you call the method with exactly the
      * same arguments and types that the method expects, or you will get a runtime exception.
      *
      * @param moduleName Fully qualified name for an object, e.g. org.wikimedia.analytics.refinery.core.DeduplicateEventLogging
      * @param methodName Name of method in the object.  Default "apply".
      * @return
      */
    def getStaticMethodMirror(moduleName: String, methodName: String = "apply"): universe.MethodMirror = {
        val mirror = universe.runtimeMirror(getClass.getClassLoader)
        val module = mirror.staticModule(moduleName)
        val method = module.typeSignature.member(universe.TermName(methodName)).asMethod
        val methodMirror = mirror.reflect(mirror.reflectModule(module).instance).reflectMethod(method)
        if (!methodMirror.symbol.isMethod || !methodMirror.symbol.isStatic) {
            throw new RuntimeException(
                s"Cannot get static method for $moduleName.$methodName, it is not a static method"
            )
        }
        methodMirror
    }

}
