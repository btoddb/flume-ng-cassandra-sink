FLUME_LIB_DIR=/btoddb/apache-flume-1.3.0-SNAPSHOT/lib

rm ${FLUME_LIB_DIR}/libthrift*

rm -r tmp-staging > /dev/null
mkdir tmp-staging
pushd tmp-staging > /dev/null
tar xvfz ../target/flume-cassandra-sink-1.0.0-SNAPSHOT-test-client.tar.gz > /dev/null
pushd lib > /dev/null

cp -v flume-cassandra-sink* \
      hector-core* \
      cassandra-all* \
      guava* \
      speed4j* \
      uuid* \
      snappy-java* \
      compress-lzf* \
      concurrentlinkedhashmap-lru* \
      antlr* \
      antlr-runtime* \
      stringtemplate* \
      jline* \
      json-simple* \
      snakeyaml* \
      snaptree* \
      metrics-core* \
      libthrift* \
      cassandra-thrift* \
   ${FLUME_LIB_DIR}/.

popd > /dev/null
popd > /dev/null