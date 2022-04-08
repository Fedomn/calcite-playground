# generate TPC-H dataset in Lucene and prepare for calcite-tutorial code
gen-tpch-dataset:
	test -d ./target || rm -rf ./target
	./gradlew clean build -x test
	java -jar indexer/build/libs/indexer.jar