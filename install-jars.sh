FLUME_SERVER_LIB_DIR=/btoddb/flume-dist/flume-server-node/lib
FLUME_CLIENT_LIB_DIR=/btoddb/flume-dist/flume-client-node/lib

#rm ${FLUME_SERVER_LIB_DIR}/*
#rm ${FLUME_CLIENT_LIB_DIR}/*

rm -r tmp-staging > /dev/null
mkdir tmp-staging
pushd tmp-staging > /dev/null
tar xvfz ../target/flume-ng-cassandra-sink-1.0.0-SNAPSHOT-dist.tar.gz > /dev/null
pushd lib > /dev/null

fileList="flume-ng-cassandra-sink*.jar \
      hector-core* \
      guava* \
      speed4j* \
      uuid* \
      libthrift* \
      cassandra-thrift* \
"

cp -v ${fileList} ${FLUME_SERVER_LIB_DIR}/.
cp -v ${fileList} ${FLUME_CLIENT_LIB_DIR}/.

popd > /dev/null
popd > /dev/null

rm -r tmp-staging > /dev/null
