package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.util.stream.Collectors;
import java.util.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class IDReduce {

        public static Map<Long, Double> calcZScores(Map<Long, Long> data){
                double mean = data.values().stream().mapToLong(Long::longValue).average().orElse(0.0);

                // Step 2: Calculate the standard deviation (σ)
                double variance = data.values().stream()
                               .mapToDouble(value -> Math.pow(value - mean, 2))
                               .average().orElse(0.0);
                double standardDeviation = Math.sqrt(variance);

                // Step 3: Calculate Z-scores for each entry (Z = (X - μ) / σ)
                Map<Long, Double> zScores = new HashMap<>();
                for (Map.Entry<Long, Long> entry : data.entrySet()) {
                        double zScore = (entry.getValue() - mean) / standardDeviation;
                        zScores.put(entry.getKey(), zScore);  // Store the z-score for the corresponding score
                }

                return zScores;
        }

        public static Map<String, Double> calcZScoresString(Map<String, Long> data){
                double mean = data.values().stream().mapToLong(Long::longValue).average().orElse(0.0);

                // Step 2: Calculate the standard deviation (σ)
                double variance = data.values().stream()
                               .mapToDouble(value -> Math.pow(value - mean, 2))
                               .average().orElse(0.0);
                double standardDeviation = Math.sqrt(variance);

                // Step 3: Calculate Z-scores for each entry (Z = (X - μ) / σ)
                Map<String, Double> zScores = new HashMap<>();
                for (Map.Entry<String, Long> entry : data.entrySet()) {
                        double zScore = (entry.getValue() - mean) / standardDeviation;
                        zScores.put(entry.getKey(), zScore);  // Store the z-score for the corresponding score
                }

                return zScores;
        }

    // Function to create a regex pattern for keywords
    public static String createPattern(List<String> keywords) {
        return ".*\\b(" + String.join("|", keywords) + ")\\b.*";
    }

    public static void run(String csvPath, String playerId, String outputPath) {
        if (csvPath == null || playerId == null || outputPath == null) {
            throw new IllegalArgumentException("CSV path, player ID, and output path are required.");
        }

        // Define keyword lists
        List<String> goodKeywords = Arrays.asList(
                "shot", "layup", "dunk", "3pt", "free throw", "hook shot", "fadeaway",
                "finger roll", "putback", "alley oop", "tip layup", "bank shot", "assist", "ast", "pts", "points", "steal", "block"
        );

        List<String> badKeywords = Arrays.asList(
                "miss", "missed", "blocked", "turnover", "lost ball", "bad pass",
                "foul", "offensive foul", "charge foul", "traveling", "out of bounds", "double technical"
        );

        List<String> neutralKeywords = Arrays.asList(
                "rebound", "timeout", "sub", "jump ball", "halftime", "start"
        );

        // Patterns for regex matching
        String goodPattern = createPattern(goodKeywords);
        String badPattern = createPattern(badKeywords);
        String neutralPattern = createPattern(neutralKeywords);

        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("IDReduce")
                .getOrCreate();

        try {
            // Read the CSV file into a Dataset<Row>
            Dataset<Row> df = spark.read()
                    .option("header", "true") 
                    .csv(csvPath);

            // Filter Dataset<Row> based on player ID
            Dataset<Row> filtered = df.filter(
                    col("player1_id").equalTo(playerId)
                            .or(col("player2_id").equalTo(playerId))
                            .or(col("player3_id").equalTo(playerId))
            );

            // Update play_impact column using regex patterns
            filtered = filtered.withColumn(
                    "play_impact",
                    when(
                            lower(col("homedescription")).rlike(goodPattern)
                                    .or(lower(col("visitordescription")).rlike(goodPattern))
                                    .or(lower(col("neutraldescription")).rlike(goodPattern)),
                            "Good"
                    ).when(
                            lower(col("homedescription")).rlike(badPattern)
                                    .or(lower(col("visitordescription")).rlike(badPattern))
                                    .or(lower(col("neutraldescription")).rlike(badPattern)),
                            "Bad"
                    ).when(
                            lower(col("homedescription")).rlike(neutralPattern)
                                    .or(lower(col("visitordescription")).rlike(neutralPattern))
                                    .or(lower(col("neutraldescription")).rlike(neutralPattern)),
                            "Neutral"
                    ).otherwise("Error")
            );

            // Select only the required columns
            Dataset<Row> finalOutput = filtered.select(
                    "game_id", "period", "pctimestring", "homedescription", "visitordescription", "neutraldescription", "play_impact", "score"
            );

            finalOutput = finalOutput.filter(
                    col("homedescription").isNotNull().and(col("homedescription").notEqual(""))
                            .or(col("visitordescription").isNotNull().and(col("visitordescription").notEqual("")))
                            .or(col("neutraldescription").isNotNull().and(col("neutraldescription").notEqual("")))
            );
            

            //Determine home or away
            Dataset<Row> gameteam = df.filter(
                df.col("player1_id").equalTo(playerId)
                .or(df.col("player2_id").equalTo(playerId))
                .or(df.col("player3_id").equalTo(playerId))
            ).withColumn("team_id_for_target_player", 
            functions.when(df.col("player1_id").equalTo(playerId), df.col("player1_team_id"))
                .when(df.col("player2_id").equalTo(playerId), df.col("player2_team_id"))
                .when(df.col("player3_id").equalTo(playerId), df.col("player3_team_id"))
            );
        

            Dataset<Row> castedResult = gameteam.withColumn("game_id", gameteam.col("game_id").cast("long"))
                                                   .withColumn("team_id_for_target_player", gameteam.col("team_id_for_target_player").cast("int"));

            Dataset<Row> result = castedResult.groupBy("game_id")
                .agg(functions.first("team_id_for_target_player").alias("team_id"))
                .select("game_id", "team_id");
                
           Dataset<Row> game_summary = spark.read()
           .option("header", "true") 
           .csv("/csv/game_summary.csv");

           Dataset<Row> game_info = game_summary
                .withColumnRenamed("game_id", "game_summary_game_id");

           Dataset<Row> final_info = result.join(game_info, result.col("game_id").equalTo(game_info.col("game_summary_game_id")),"left")
                .withColumn("home_or_away", 
                functions.when(result.col("team_id").equalTo(game_info.col("home_team_id")), functions.lit("home"))
                        .otherwise(functions.when(result.col("team_id").equalTo(game_info.col("visitor_team_id")), functions.lit("away"))
                                .otherwise(functions.lit("unknown"))));
        
        Dataset<Row> outputDataset = final_info.select("game_id", "home_or_away");

        Dataset<Row> o_d = outputDataset
                .withColumnRenamed("game_id", "game_summary_game_id");
        Dataset<Row> withAway = finalOutput.join(o_d, finalOutput.col("game_id").equalTo(o_d.col("game_summary_game_id")),"left");
                

        //fix scores
        WindowSpec scoreWindow = Window.partitionBy("game_id")
                .orderBy(
                        functions.col("period").asc(),
                        functions.col("pctimestring").desc() 
                );
        Dataset<Row> updatedDataset = withAway.withColumn(
                "filled_score",
                functions.last("score", true).over(scoreWindow)
        );

        
        Dataset<Row> datasetWithScores = updatedDataset
            .withColumn("home_score", functions.expr("split(filled_score, '-')[0]").cast("int"))
            .withColumn("away_score", functions.expr("split(filled_score, '-')[1]").cast("int"));
            
        
        Dataset<Row> datasetWithScoreDef = datasetWithScores.withColumn(
        "score_def",
                functions.when(functions.col("home_or_away").equalTo("home"),
                   functions.col("home_score").minus(functions.col("away_score")))
                .when(functions.col("home_or_away").equalTo("away"),
                   functions.col("away_score").minus(functions.col("home_score")))
                .otherwise(functions.lit(0)) // Assign a default value for "unknown" rows
);

        
            //Calculate z-scores
        
            //Score bins
            int[] periods = {1, 2, 3, 4};
            //Quarter bins

                Dataset<Row> withRange = datasetWithScoreDef.withColumn(
                "score_range",
                        functions.floor(functions.col("score_def").divide(5)).multiply(5)
                );

                
                Dataset<Row> aggregated = withRange.groupBy("score_range", "play_impact").count();


                Dataset<Row> pivoted = aggregated.groupBy("score_range")
                .pivot("play_impact", Arrays.asList("Good", "Bad"))
                .sum("count")
                .na().fill(0);


                Dataset<Row> finalScores = pivoted.withColumn(
                "total",
                        functions.col("Good").minus(functions.col("Bad"))
                );


                Map<Long, Long> scores = finalScores.select("score_range", "total")
                .collectAsList()
                .stream()
                .collect(Collectors.toMap(
                        row -> row.getAs("score_range"),  // Key: score range
                        row -> ((Long) row.getAs("total")),    // Value: total score
                        Long::sum
                ));

                Map<Long, Double> zScores = calcZScores(scores);

                //Find the sum for periods
                Dataset<Row> scoredDF = finalOutput.withColumn("sum", when(col("play_impact").equalTo("Good"), 1)
                        .when(col("play_impact").equalTo("Bad"), -1)
                        .otherwise(0)); 

                Dataset<Row> period_result = scoredDF.groupBy("period")
                        .agg(sum("sum").alias("total_score"));

                Map<Long, Long> periodScores = period_result.collectAsList().stream()
                        .collect(Collectors.toMap(
                                row -> Long.valueOf(row.getAs("period").toString()), 
                                row -> Long.valueOf(row.getAs("total_score").toString()) 
                        ));
                
                Map<Long, Double> zPeriod = calcZScores(periodScores);

                Dataset<Row> scoredDF2 = datasetWithScores.withColumn("sum", when(col("play_impact").equalTo("Good"), 1)
                        .when(col("play_impact").equalTo("Bad"), -1)
                        .otherwise(0)); 

                //Find the sum for home/away
                Dataset<Row> ha_result = scoredDF2.groupBy("home_or_away")
                        .agg(sum("sum").alias("total_score"));

                 
                Map<String, Long> haScores = ha_result.collectAsList().stream()
                        .collect(Collectors.toMap(
                                row -> row.getAs("home_or_away").toString(), 
                                row -> Long.valueOf(row.getAs("total_score").toString())
                        ));
                
                Map<String, Double> zHome = calcZScoresString(haScores);
                

                




            String myContents = "";
            for (Long i : zScores.keySet()){
                myContents += "(" + i + " : " + zScores.get(i) + ")";
            }
            Row row = RowFactory.create("Score", myContents);

            myContents = "";
            for (Long i : zPeriod.keySet()){
                myContents += "(" + i + " : " + zPeriod.get(i) + ")";
            }
            Row row2 = RowFactory.create("Period", myContents);

            
            myContents = "";
            for (String i : zHome.keySet()){
                myContents += "(" + i + " : " + zHome.get(i) + ")";
            }
            Row row3 = RowFactory.create("Home/Away", myContents);
            

            List<Row> rows = Arrays.asList(row, row2, row3);

            StructType schema = new StructType()
                .add("id", DataTypes.StringType)
                .add("contents", DataTypes.StringType);

        
                Dataset<Row> ugggh = spark.createDataFrame(rows, schema);
                
                ugggh.coalesce(1)
                    .write()
                    .option("header", "true")
                    .csv("/output/heatmap_data");

            

            // Write the filtered results to the specified output path
                datasetWithScoreDef.coalesce(1)
                    .write()
                    .option("header", "true")
                    .csv(outputPath);

            System.out.println("Filtered results successfully written to: " + outputPath);

        } catch (Exception e) {
            System.err.println("Error processing the Spark job: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: IDReduce <csvPath> <playerId> <outputPath>");
            System.exit(1);
        }

        String csvPath = args[0];
        String playerId = args[1];
        String outputPath = args[2];

        // Run the Spark job
        run(csvPath, playerId, outputPath);
    }
}
