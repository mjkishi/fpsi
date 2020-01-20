#!/bin/bash

echo "Params : $*"
#killall -9 java
#java -Xms42048m -Xmx84096m -ea -Djava.util.logging.config.file=logging.properties -cp lib/kryo-2.24.0.jar:lib/minlog-1.2.jar:lib/objenesis-1.2.jar:lib/reflectasm-1.09-shaded.jar:ppaxos.jar stm.benchmark.bank.BankServer $*
java -ea -Djava.util.logging.config.file=logging.properties -cp lib/kryo-2.24.0.jar:lib/minlog-1.2.jar:lib/objenesis-1.2.jar:lib/reflectasm-1.09-shaded.jar:ppaxos.jar wcc.benchmark.bank.BankServer $*
