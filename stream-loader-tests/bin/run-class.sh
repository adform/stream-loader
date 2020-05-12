#!/bin/bash

set -euo pipefail

HOST=${HOST:-"127.0.0.1"}

JAVA_OPTS=${JAVA_OPTS:-}
JMX_OPTS=${JMX_OPTS:-}
JVM_GC_OPTS=${JVM_GC_OPTS:-}

APP_OPTS=${APP_OPTS:-}
APP_MAIN_CLASS=${APP_MAIN_CLASS:-}

if [ ! -z ${PORT_JMX+x} ]; then
  export JMX_OPTS=`
  `" -Dcom.sun.management.jmxremote"`
  `" -Dcom.sun.management.jmxremote.local.only=false"`
  `" -Dcom.sun.management.jmxremote.authenticate=false"`
  `" -Dcom.sun.management.jmxremote.ssl=false"`
  `" -Dcom.sun.management.jmxremote.port=$PORT_JMX"`
  `" -Dcom.sun.management.jmxremote.rmi.port=$PORT_JMX"`
  `" -Djava.rmi.server.hostname=$HOST"`
  `" $JMX_OPTS"
fi

ENABLE_JVM_PROFILING=${ENABLE_JVM_PROFILING:-"false"}

if [ ${ENABLE_JVM_PROFILING} == "true" ]; then
  export JAVA_OPTS=`
  `" -XX:+PreserveFramePointer -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:-OmitStackTraceInFastThrow -XX:+ShowHiddenFrames"`
  `" $JAVA_OPTS"
fi

USE_GRAAL=${USE_GRAAL:-"false"}

if [ ${USE_GRAAL} == "true" ]; then
  export JAVA_OPTS=`
  `" -XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI -XX:+UseJVMCICompiler"`
  `" $JAVA_OPTS"
fi

exec ${JAVA_HOME}/bin/java -XX:+UseContainerSupport \
  $JAVA_OPTS $JMX_OPTS $JVM_GC_OPTS $APP_OPTS \
  -cp "$APP_CLASS_PATH" \
  $APP_MAIN_CLASS \
  $@
