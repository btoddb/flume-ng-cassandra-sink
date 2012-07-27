FLUME_LIB_DIR=/btoddb/flume-dist/server-node/lib

rm ${FLUME_LIB_DIR}/libthrift*

rm -r tmp-staging > /dev/null
mkdir tmp-staging
pushd tmp-staging > /dev/null
tar xvfz ../target/flume-cassandra-sink-1.0.0-SNAPSHOT-dist.tar.gz > /dev/null
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
      je-5.0.58.jar \
   ${FLUME_LIB_DIR}/.

cp -v flume-cassandra-sink* /btoddb/flume-dist/client-node/lib/.
cp -v flume-cassandra-sink* /btoddb/flume-dist/avro-test-client/lib/.

popd > /dev/null
popd > /dev/null
