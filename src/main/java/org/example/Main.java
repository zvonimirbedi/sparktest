package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) {
        // spark, java, Windows configuration
        //System.setProperty("hadoop.home.dir", "c:/Program Files/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // remove DAG, Scheduler, Spark etc logs
        sc.setLogLevel("ERROR");


        SparkSession sparkSession = SparkSession.builder()
                .config(conf)
                // Windows conf only
                .config("spark.sql.warehouse.dir", "file:///c/tmp_spark/")
                .getOrCreate();
        sparkSession.conf().set("spark.sql.shuffle.partitions","25");

        // Task 1 - Lambda, collect, reduce, forEach, tuples
        System.out.println("Task 1!");
        List<Integer> testList = List.of(
                35,
                12,
                13,
                56,
                324
        );
        JavaRDD<Integer> myRdd = sc.parallelize(testList);
        JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);

        //
        //sqrtRdd.collect().forEach(value -> System.out.println("Value: " + value));

        // custom count example
        /*
        Long count = sqrtRdd
                .map(value -> 1L)
                .reduce((a,b) -> a+b);
        System.out.println("Count is: " + count);
         */

        // basic tuple example
        JavaRDD<Tuple2<Integer, Double>> tupleRdd = myRdd.map(val -> new Tuple2<>(val, Math.sqrt(val)));
        //tupleRdd.collect().forEach(System.out::println);

        // Task 2 - parallelize, mapToPair, ReduceByKey and GroupByKey
        System.out.println("Task 2!");
        List<String> testListString = List.of(
                "WARN: adaasdacessd 1",
                "ERROR: adasdfcxsassd 2",
                "WARN: sfdtfdsdf 3",
                "ERROR: fdhjdsdjkjht 4",
                "FATAL: kgjdsdddgf 5"
        );
        JavaRDD<String> myRdd2 = sc.parallelize(testListString);
        JavaPairRDD<String, Long> pairRdd = myRdd2.mapToPair(rawVal -> {
            String[] stringSplit = rawVal.split(":");
            return new Tuple2<>(stringSplit[0], 1L);
        });

        // ReduceByKey and GroupByKey
        JavaPairRDD<String, Long> pairReduceByKeyRdd = pairRdd.reduceByKey((key, value) -> value + 1L);
        System.out.println("Log Stats: ReduceByKey");
        //pairReduceByKeyRdd.foreach(keyValue -> System.out.println(keyValue._1 + " " + keyValue._2));

        JavaPairRDD<String, Iterable<Long>> pairGroupByKeyRdd = pairRdd.groupByKey();
        System.out.println("Log Stats: GroupByKey");
        //pairGroupByKeyRdd.foreach(keyValue -> System.out.println(keyValue._1 + " " + Iterables.size(keyValue._2)));


        // Task 3 - flatmap
        System.out.println("Task 3!");

        // flatmap (from object create 0 or more objects)
        myRdd2.flatMap(row -> Arrays.asList(row.split(" ")).iterator())
                .filter(word -> word.length() > 1)
                //.foreach(word -> System.out.println("Word: " + word))
        ;


        // Task 4 - reduceByKey, sortByKey
        System.out.println("Task 4!");
        JavaRDD<String> myFileRdd = sc.textFile("src/main/resources/input.txt");
        myFileRdd.flatMap(row -> Arrays.asList(row.split(" ")).iterator())
                .map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase() )
                .filter(word -> Utils.isNotBoring(word)
                        && !word.isBlank()
                )
                .mapToPair(word ->  new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(tuple ->  new Tuple2<>(tuple._2, tuple._1))
                //.sortByKey(false)
                //.collect()
                //.take(150)
                //.forEach(System.out::println)
        ;

        System.out.println("There are " + myFileRdd.getNumPartitions() + " partitions");


        // Task 5 - read csv, Dataset Filtering syntax query, in memory table
        System.out.println("Task 5!");

        Dataset<Row> myFileDataset = sparkSession.read()
                .option("header",true)
                .csv("src/main/resources/biglog.txt");

        // myFileDataset.show(5);
        // System.out.println("Number of rows in dataset: " + myFileDataset.count());

        Row firstRow = myFileDataset.first();
        String datetime = firstRow.getString(1);
        String level = firstRow.getAs("level");
        System.out.println("First row level: " + level);
        System.out.println("First row datetime: " + datetime);

        // Dataset Filtering

        // SQL syntax query
        // Dataset<Row> datasetFiltered = myFileDataset.filter("level = 'DEBUG' AND datetime <= '2015-1-1 00:00:00'");

        // Lambda syntax query
        /*
        Dataset<Row> datasetFiltered = myFileDataset.filter((Row row) ->
                "DEBUG".equals(row.getAs("level")) &&
                getLocalDateTimeFromString("2015-1-1 00:00:00").isAfter(getLocalDateTimeFromString(row.getAs("datetime")))
        );
        */

        // Column syntax query
        /*
        Column datetimeColumn = col("datetime");
        Column levelColumn = col("level");
        Dataset<Row> datasetFiltered = myFileDataset.filter(datetimeColumn.like("2015%").and(levelColumn.equalTo("DEBUG")));
        */

        // FULL SQL query
        /*
        myFileDataset.createOrReplaceTempView("view_logs_table");
        Dataset<Row> datasetFiltered = sparkSession.sql("select min(datetime) from view_logs_table where level = 'DEBUG'");
        */

        // in memory table
        System.out.println("Task 6!");
        List<Row> inMemoryTable = new ArrayList<>();
        inMemoryTable.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemoryTable.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemoryTable.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemoryTable.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemoryTable.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));


        StructField[] tableFields = new StructField[]{
                new StructField ("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField ("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType tableSchema = new StructType(tableFields);
        Dataset<Row> datasetFiltered = sparkSession.createDataFrame(inMemoryTable, tableSchema);

        //datasetFiltered.show();

        //myFileDataset.createOrReplaceTempView("view_logs_table");
        datasetFiltered.createOrReplaceTempView("view_logs_table");
        //sparkSession.sql("select level, collect_list(datetime) from view_logs_table group by level order by level").show();
        Dataset<Row> resultsView = sparkSession.sql("select level, date_format(datetime, 'MMMM') as month, count(datetime) as count from view_logs_table group by level, month");

        resultsView = resultsView.drop("month");
        //resultsView.show(50);
        //resultsView.explain();

        //resultsView.createOrReplaceTempView("view_results");
        //sparkSession.sql("select sum(count) from view_results").show();

        // Task 7 ML
        System.out.println("Task 7!");

        Dataset<Row> mlGymDataset = sparkSession.read()
                .option("header",true)
                // auto guess schema data type
                .option("inferSchema", true)
                .csv("src/main/resources/GymCompetition.csv");

        //mlGymDataset.printSchema();

        StringIndexer genderIndexer = new StringIndexer();
        genderIndexer.setInputCol("Gender").setOutputCol("GenderIndex");
        mlGymDataset = genderIndexer.fit(mlGymDataset).transform(mlGymDataset);
        //mlGymDataset.show(50);

        OneHotEncoder genderEncoder = new OneHotEncoder();
        genderEncoder.setInputCols(new String[]{"GenderIndex"})
                .setOutputCols(new String[]{"GenderVector"});
        mlGymDataset = genderEncoder.fit(mlGymDataset).transform(mlGymDataset);
        //mlGymDataset.show(50);

        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[]{"Age","Height","Weight", "GenderVector"});
        vectorAssembler.setOutputCol("features");

        Dataset<Row> mlGymDatasetWithFeature = vectorAssembler.transform(mlGymDataset);
        //mlGymDatasetWithFeature.show(10);
        Dataset<Row> mlGymDatasetWithFeatureClean =  mlGymDatasetWithFeature.select("NoOfReps", "features")
                .withColumnRenamed("NoOfReps","label");

        mlGymDatasetWithFeatureClean.show();

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model = linearRegression.fit(mlGymDatasetWithFeatureClean);
        System.out.println("Model has intercept: " + model.intercept() + " and coefficients: " + model.coefficients());
        //model.transform(mlGymDatasetWithFeatureClean).show();

        Dataset<Row> mlHouseDataset = sparkSession.read()
                .option("header",true)
                // auto guess schema data type
                .option("inferSchema", true)
                .csv("src/main/resources/kc_house_data.csv");
        mlHouseDataset.printSchema();
        mlHouseDataset.describe().show();

        mlHouseDataset = mlHouseDataset.drop(
                "id",
                "date",
                "waterfront",
                "view",
                //"condition",
                //"grade",
                "yr_renovated",
                //"zipcode",
                "lat",
                "long");

        for (String columnName: mlHouseDataset.columns()) {
            System.out.println("Correlation between price and " + columnName + ": " + mlHouseDataset.stat().corr("price",columnName));
        }

        mlHouseDataset = mlHouseDataset.drop(
                "sqft_lot",
                "sqft_lot15",
                "sqft_living15",
                "yr_built");


        /*
        for (String columnNameX: mlHouseDataset.columns()) {
            for (String columnNameY : mlHouseDataset.columns()) {
                if (!columnNameX.equals(columnNameY))
                    System.out.println("Correlation between " + columnNameX +" and " + columnNameY + ": " + mlHouseDataset.stat().corr(columnNameX, columnNameY));
            }
        }
         */

        mlHouseDataset = mlHouseDataset
            .withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living")))
            .withColumnRenamed("price", "label");
        mlHouseDataset.show();

        Dataset<Row>[] randomSplitDataSet = mlHouseDataset.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingDataset = randomSplitDataSet[0];
        Dataset<Row> testingDataset = randomSplitDataSet[1];

        StringIndexer houseIndexers = new StringIndexer();
        houseIndexers
                .setInputCols(new String[]{"condition", "grade", "zipcode"})
                .setOutputCols(new String[]{"conditionIndex", "gradeIndex", "zipcodeIndex"});
        //mlHouseDataset = houseIndexers.fit(mlHouseDataset).transform(mlHouseDataset);

        OneHotEncoder housenEncoder = new OneHotEncoder();
        housenEncoder
                .setInputCols(new String[]{"conditionIndex", "gradeIndex", "zipcodeIndex"})
                .setOutputCols(new String[]{"conditionVector", "gradeVector", "zipcodeVector"});
        //mlHouseDataset = housenEncoder.fit(mlHouseDataset).transform(mlHouseDataset);


        VectorAssembler vectorHouse = new VectorAssembler()
                .setInputCols(new String[]{"bedrooms","bathrooms","sqft_living",
                        "sqft_above_percentage", "floors", "conditionVector", "gradeVector", "zipcodeVector"})
                .setOutputCol("features");

        /*
        Dataset<Row> mlHouseDatasetWithFeature = vectorHouse
                .transform(mlHouseDataset)
                .select("price", "features")
                .withColumnRenamed("price", "label");
        mlHouseDatasetWithFeature.show(10);
         */




        ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
        ParamMap[] paramMaps = paramGridBuilder
                .addGrid(linearRegression.regParam(), new double[]{0.01,0.1,0.5})
                .addGrid(linearRegression.elasticNetParam(), new double[]{0,0.5,1})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.8)
                ;

        //TrainValidationSplitModel modelHouses = trainValidationSplit.fit(trainingDataset);
        //LinearRegressionModel modelHouse = (LinearRegressionModel) modelHouses.bestModel();

        // custom handpicked params
        /*
        LinearRegressionModel modelHouse = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .fit(trainingDataset);
        */


        // Task 8 Pipeline flow
        System.out.println("Task 8!");

        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{houseIndexers, housenEncoder, vectorHouse, trainValidationSplit});
        PipelineModel pipelineModel = pipeline.fit(trainingDataset);
        TrainValidationSplitModel trainHouseModels = (TrainValidationSplitModel)pipelineModel.stages()[3];
        LinearRegressionModel modelHouse = (LinearRegressionModel) trainHouseModels.bestModel();

        //workoroud?
        testingDataset = pipelineModel.transform(testingDataset).drop("prediction");

        System.out.println("Model training dataset  accuracy for LR RMSE(smaller is better): " + modelHouse.summary().rootMeanSquaredError()
                + " r2(closer to 1 is better): " + modelHouse.summary().r2());

        //modelHouse.transform(testingDataset).show();
        System.out.println("Model testing dataset   accuracy for LR RMSE(smaller is better): " + modelHouse.evaluate(testingDataset).rootMeanSquaredError()
                + " r2(closer to 1 is better): " + modelHouse.evaluate(testingDataset).r2());

        System.out.println("Model has intercept: " + modelHouse.intercept() + " and coefficients: " + modelHouse.coefficients());
        System.out.println("Model has regparam: " + modelHouse.getRegParam() + " and elastic netparam: " + modelHouse.getElasticNetParam());


        // Uncomment for web UI
        //new Scanner(System.in).nextLine();
        // UI link http://localhost:4040/jobs/

        // spark close cleanup
        System.out.println("Good by world!");
        sc.close();
    }



}