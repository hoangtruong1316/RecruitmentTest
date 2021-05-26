import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

import static org.apache.spark.sql.functions.*;

public class test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        //read file
        Dataset<Row> nYTaxiDataset = spark.read().option("header", "true").csv("F:\\truonghv\\dataTest");

        // creat column HOUR of pickup time
        nYTaxiDataset = nYTaxiDataset.withColumn("PICKUP_HOUR", hour(col("lpep_pickup_datetime")));
        // creat column DAY_OF_WEEK of pickup time
        nYTaxiDataset = nYTaxiDataset.withColumn("DAY_OF_WEEK", dayofweek(col("lpep_pickup_datetime")));

        // one-hot encoding for each hour of the day
        for(int h =0; h<24; h++){
            nYTaxiDataset = nYTaxiDataset.withColumn("HOUR_" + h, when(col("PICKUP_HOUR").equalTo(h), 1).otherwise(0));
        }

        // one-hot encoding for each day of the week
        for(int dow =1; dow<8; dow++){
            nYTaxiDataset = nYTaxiDataset.withColumn("DAY_OF_WEEK" + dow, when(col("DAY_OF_WEEK").equalTo(dow), 1).otherwise(0));
        }

        // duration of the trip
        nYTaxiDataset = nYTaxiDataset.withColumn("DURATION_OF_THE_TRIP", (unix_timestamp(col("Lpep_dropoff_datetime"))).minus(unix_timestamp(col("lpep_pickup_datetime"))));

        //is pickup or drop off in jfk airport? with bounding box of jfk airport: -73.81758, 40.63425, -73.77012, 40.66342 - get from https://boundingbox.klokantech.com/
        nYTaxiDataset = nYTaxiDataset
                .withColumn(
                        "IS_PICKUP_IN_JFK_AP",
                        when(
                                (col("Pickup_longitude").between(-73.81758, -73.77012)).and(col("Pickup_latitude").between(40.63425, 40.66342)),
                                1
                        ).otherwise(0)
                )
                .withColumn(
                        "IS_DROPOFF_IN_JFK_AP",
                        when(
                                (col("Dropoff_longitude").between(-73.81758, -73.77012)).and(col("Dropoff_latitude").between(40.63425, 40.66342)),
                                1
                        ).otherwise(0)
                )
                .withColumn(
                        "IS_PICKUP_DROPOFF_IN_JFK_AP",
                        when(
                                (col("IS_PICKUP_IN_JFK_AP").equalTo(1)).or(col("IS_DROPOFF_IN_JFK_AP").equalTo(1)),
                                1
                        ).otherwise(0)
                );
        /*nYTaxiDataset.printSchema();
        nYTaxiDataset.show(10, false);*/

        //reformat data type
        nYTaxiDataset = nYTaxiDataset
                .select(
                        col("VendorID").cast(DataTypes.IntegerType),
                        col("lpep_pickup_datetime").cast(DataTypes.TimestampType),
                        col("Lpep_dropoff_datetime").cast(DataTypes.TimestampType),
                        col("Store_and_fwd_flag").cast(DataTypes.StringType),
                        col("RateCodeID").cast(DataTypes.IntegerType),
                        col("Pickup_longitude").cast(new DecimalType(20, 15)),
                        col("Pickup_latitude").cast(new DecimalType(20, 15)),
                        col("Dropoff_longitude").cast(new DecimalType(20, 15)),
                        col("Dropoff_latitude").cast(new DecimalType(20, 15)),
                        col("Passenger_count").cast(DataTypes.IntegerType),
                        col("Trip_distance").cast(DataTypes.FloatType),
                        col("Fare_amount").cast(DataTypes.FloatType),
                        col("Extra").cast(DataTypes.FloatType),
                        col("MTA_tax").cast(DataTypes.FloatType),
                        col("Tip_amount").cast(DataTypes.FloatType),
                        col("Tolls_amount").cast(DataTypes.FloatType),
                        col("Ehail_fee").cast(DataTypes.FloatType),
                        col("Total_amount").cast(DataTypes.FloatType),
                        col("Payment_type").cast(DataTypes.IntegerType),
                        col("Trip_type ").cast(DataTypes.StringType).alias("Trip_type")
                );

        // save result
        nYTaxiDataset.coalesce(1).write().mode(SaveMode.Overwrite).option("header", "true").csv("F:\\truonghv\\csv_result");
        nYTaxiDataset.coalesce(1).write().mode(SaveMode.Overwrite).parquet("F:\\truonghv\\parquet_result");
        spark.sparkContext().setLogLevel("WARN");
        spark.close();
    }
}
