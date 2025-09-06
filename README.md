# BMW Car Sales Analysis – Hadoop MapReduce & Spark

## Overview
This project analyzes a dataset of BMW car sales using two Big Data frameworks:

- **Exercise 1 (Hadoop 3.3.6 MapReduce)**: multi-step job chaining with at least three transformations.
- **Exercise 2 (Apache Spark)**: RDD-based analysis.

Both exercises process the dataset: [BMW_Car_Sales_Classification.csv](hadoop-cluster-3.3.6-amd64/hddata/dataset/BMW_Car_Sales_Classification.csv)

---

## Dataset
The dataset contains 50K lines about BMW car sales.

The columns are:
- Model
- Year
- Region
- Color
- Fuel_Type
- Transmission
- Engine_Size_L
- Mileage_KM
- Price_USD
- Sales_Volume
- Sales_Classification

---

## Docker
The project revolves around the [Docker image](hadoop-cluster-3.3.6-amd64/Dockerfile) and the [Docker Compose file](hadoop-cluster-3.3.6-amd64/docker-compose.yml).
To build the image simply run:
```bash
cd hadoop-cluster-3.3.6-amd64
docker image build -t hadoop .
```

Then, to run all container, create the dedicated network and to share the volume (a folder on the host) run:
```bash
docker compose up -d # The "-d" flag is used to daemonize the run
```

---

## Exercise 1 – MapReduce
### Goal
For each `Region`:
- Count and aggregate sales per `Model`.
- Compute total sales volume per `Region`.
- Find Top-K models by sales share, reporting also average price and percentage of `High` classifications.

### Code structure
Located in [src/mapreduce/](./hadoop-cluster-3.3.6-amd64/hddata/src/mapreduce):
- Job 1 code files: `Mapper1.java`, `Combiner1.java`, `Reducer1.java`
- Job 2 code files: `Mapper2.java`, `Reducer2.java`
- Job 3 code files: `Mapper3.java`, `Reducer3.java`
- Driver (Job-Chaining): `DriverBMWSales.java`

### Compilation

```bash
docker exec -it master bash
cd data
mkdir -p build
javac -cp "$(hadoop classpath)" -d build $(find src/mapreduce -name "*.java")
jar -cvf BMWSales.jar -C build .
```

### Execution
```bash
hdfs dfs -put -f /data/dataset/BMW_Car_Sales_Classification.csv hdfs:///input # The dataset file is now called input on HDFS
hadoop jar BMWSales.jar mapreduce.DriverBMWSales hdfs:///input hdfs:///bmw_out1 hdfs:///bmw_out2 hdfs:///bmw_out3 10`
```

Outputs:
- `/bmw_out1`: counts per `Region` x `Model`
- `/bmw_out2`: totals per `Region`
- `/bmw_out3`: Top-K models per `Region`

If you want to get all the outputs on the shared volume, run: 
```bash
hdfs dfs -get hdfs:///bmw_out1 hdfs:///bmw_out2 hdfs:///bmw_out3 /data
```

> **NOTE**
> 
> If you want to delete the output folders from HDFS, run:
> ```bash
> hdfs dfs -rm -r hdfs:///bmw_out1 hdfs:///bmw_out2 hdfs:///bmw_out3
> ```
---

## Exercise 2 – Spark (3.5.6 built for Hadoop 3.3 and later)
### Goal
For each **age group** (bucketed from `Year`) find the most sold model.

The chosen age groups are:
- `<=2014`
- `2015_2018`
- `2019_2021`
- `>=2022`

### Code structure
Located in [`src/spark`](hadoop-cluster-3.3.6-amd64/hddata/src/spark):
- Driver: `SparkDriver.java`

> The driver is coded using RDD API: `mapToPair`, `reduceByKey`, `map`, `saveAsTextFile` with **lambda expressions**.

### Compilation
```bash
docker exec -it master bash
cd data
mkdir -p bmw_spark/build
javac -cp "$SPARK_HOME/jars/*" -d build $(find src/spark -name "*.java")
jar -cvf BMWSpark.jar -C build .
```

### Execution (local mode with HDFS I/O)
```bash
spark-submit \
--class spark.SparkDriver \
--master local[*] \
/data/BMWSpark.jar \
hdfs:///input \
hdfs:///bmw_out_spark
```

Output:
- `/bmw_out_spark`: folder which contains the text file with the best-selling model per age group

If you want to get the output on the shared volume, run:
```bash
hdfs dfs -get hdfs:///bmw_out_spark /data
```

> **NOTE**
>
> If you want to delete the output folder from HDFS, run:
> ```bash
> hdfs dfs -rm -r hdfs:///bmw_out_spark
> ```

---

## Results
After execution:
- MapReduce outputs are available in [/bmw_out1](hadoop-cluster-3.3.6-amd64/hddata/bmw_out1/part-r-00000), [/bmw_out2](hadoop-cluster-3.3.6-amd64/hddata/bmw_out2/part-r-00000), [/bmw_out3](hadoop-cluster-3.3.6-amd64/hddata/bmw_out3/part-r-00000)
- Spark output is available in [/bmw_out_spark](hadoop-cluster-3.3.6-amd64/hddata/bmw_out_spark/part-00000)

Use `hdfs dfs -cat <path>/part-*` (in HDFS) or `cat <path>/part-*` (on node master local FS) to inspect results.

---

## Notes
- Java 8 is used for both Hadoop and Spark.
- Spark JAR include only the user code, **NOT** the Spark libraries.
- MR (*MapReduce*) job uses job chaining and Combiner for efficiency.
- Spark job demonstrates lambda expressions and functional API style.
- Documentation about the project is available [here](documentation) (inside the folder you will find a PDF to read it more comfortably).

---

## Author
The author of this project is me: [Emanuele Relmi](https://github.com/Kirito-Emo).

> If you liked this project, consider starring the repo to support it.
> 
> For any error or contributions, feel free to contribute (obv wrt the [LICENSE](LICENSE)) by submitting a merge request or by opening an Issue.

---

## License
This project is licensed under the [Apache License 2.0](LICENSE).