import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class BFS_ShortestPath {

	// Constants representing vertex statuses
	public static final String ACTIVE_STATUS = "ACTIVE";
	public static final String INACTIVE_STATUS = "INACTIVE";

	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("Usage: ShortestPath <inputFile> <startVertex> <endVertex>");
			System.exit(1);
		}

		// Read the graph file, starting and ending vertex 
		String GraphFile = args[0];
		String startVertex = args[1];
		String endVertex = args[2];

		// Configure Spark
		SparkConf Conf = new SparkConf().setAppName("BFS-based Shortest Path");
		JavaSparkContext sparkContext = new JavaSparkContext(Conf);

		// Read the input file into an RDD
		JavaRDD<String> lines = sparkContext.textFile(GraphFile).cache();

		// Start timer
		long startTime = System.currentTimeMillis();

		// Initialize the Graph as a JavaPirRDD
		JavaPairRDD<String, Data> Graph = lines.mapToPair(line -> {
			// Parse each line of the input file and extract vertex information 
			int equalIndex = line.indexOf("=");
			String vertexID = line.substring(0, equalIndex);
			String[] neighborList = line.substring(equalIndex + 1).split(";");
			int neighborCount = neighborList.length;

			// Create a list of neighbors with their weights
			List<Tuple2<String, Integer>> neighbors = new ArrayList<>();
			for (int i = 0; i < neighborCount; i++) {
				String[] neighborInfo = neighborList[i].split(",");
				neighbors.add(new Tuple2<>(neighborInfo[0], Integer.parseInt(neighborInfo[1])));
			}

			// Create Data object for the vertex
			Data vertexData;
			if (vertexID.equals(startVertex)) {
				// Set initial attributes for the start vertex
				vertexData = new Data(neighbors, 0, 0, ACTIVE_STATUS);
			} else {
				// Set initial attributes for other vertices
				vertexData = new Data(neighbors, Integer.MAX_VALUE, Integer.MAX_VALUE, INACTIVE_STATUS);
			}
			return new Tuple2<>(vertexID, vertexData);
		}).cache();

		 /*
         BFS Loop: Iteratively propagate distances from ACTIVE vertices
         until there are no more ACTIVE vertices in the graph.
         */
		while (Graph.filter(f -> f._2.status.equals(ACTIVE_STATUS)).count() > 0) {
			// Propagate distances to neighbors in the network
			Graph = network.flatMapToPair(vertex -> {
				// List to store updated distances for the current vertex and its neighbors
				List<Tuple2<String, Data>> propagatedDistances = new ArrayList<>();

				// Add the current vertex to the list with updated status
				propagatedDistances.add(new Tuple2<>(vertex._1,
						new Data(vertex._2.neighbors, vertex._2.distance, vertex._2.prev,
								INACTIVE_STATUS)));

				// Propagate distances to neighbors if the vertex is ACTIV
				if (vertex._2.status.equals(ACTIVE_STATUS)) {
					for (Tuple2<String, Integer> neighbor : vertex._2.neighbors) {
						propagatedDistances.add(new Tuple2<>(neighbor._1,
								new Data(new ArrayList<>(), (neighbor._2 + vertex._2.distance), Integer.MAX_VALUE,
										INACTIVE_STATUS)));
					}
				}
				return propagatedDistances.iterator();
			});

			// Reduce step: Combine propagated data for each vertex
			Graph = network.reduceByKey((k1, k2) -> new Data(
					// Merge neighbors and update distances using the minimum
					k1.neighbors.size() == 0 ? k2.neighbors : k1.neighbors,
					Math.min(k1.distance, k2.distance),
					Math.min(k1.prev, k2.prev),
					INACTIVE_STATUS))
					 // Update vertex status to ACTIVE if the distance has changed
					.mapValues(value -> value.distance < value.prev ?
									new Data(value.neighbors, value.distance, value.distance, ACTIVE_STATUS) : value);
		}

		// Look up the result for the target vertex
		List<Data> result = Graph.lookup(endVertex);

		// Print the shortest distance and execution time
		System.out.println("Shortest Distance from " + startVertex + " to " + endVertex + ": " + result.get(0).distance);
		System.out.println("Time = " + (System.currentTimeMillis() - startTime));


		// Stop Spark Context
		sparkContext.stop();
	}
}
