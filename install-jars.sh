FLUME_SERVER_LIB_DIR=/btoddb/flume-dist/flume-server-node/lib
FLUME_CLIENT_LIB_DIR=/btoddb/flume-dist/flume-client-node/lib

rm ${FLUME_SERVER_LIB_DIR}/*
rm ${FLUME_CLIENT_LIB_DIR}/*

rm -r tmp-staging > /dev/null
mkdir tmp-staging
pushd tmp-staging > /dev/null
tar xvfz ../target/flume-cassandra-sink-1.0.0-SNAPSHOT-dist.tar.gz > /dev/null
pushd lib > /dev/null

fileList="flume-cassandra-sink* \
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
      hornetq-core-2.2.18.Final.jar \
"






cp -v ${fileList} ${FLUME_SERVER_LIB_DIR}/.
cp -v ${fileList} ${FLUME_CLIENT_LIB_DIR}/.

cp -v flume-cassandra-sink* ${FLUME_CLIENT_LIB_DIR}/.
cp -v flume-cassandra-sink* /btoddb/flume-dist/avro-test-client/lib/.

popd > /dev/null
popd > /dev/null

rm -r tmp-staging > /dev/null
