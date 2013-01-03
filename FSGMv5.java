package fsgmV5;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

/**
 * This is for directed implementation
 * Documentation to be improved in future
 * @author smhill
 *
 */


public class FSGMv5 {

	public static class ParserMapper 
	extends MapReduceBase implements Mapper<Object, Text, Text, Text>{

		private Text vertexEdgeInfo = new Text();
		private Text vertexGraphidInfo = new Text();

		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {

			/**
			 * part[0] = v-from no
			 * part[1] = v-to no
			 * part[2] = edge label
			 * part[3] = v-from label
			 * part[4] = v-to label
			 * part[5] = graph id
			 */

			String[] edgeInfo = value.toString().trim().split(" ");
			String graphID = edgeInfo[5];
			String vertexEdge = "(" + edgeInfo[3] + ":" + edgeInfo[2] + "-" + edgeInfo[4] + ")";
			String vertex = "(" + edgeInfo[0] + ":" + edgeInfo[1] + ")";

			vertexEdgeInfo.set(vertexEdge);
			vertexGraphidInfo.set(vertex + "_" + graphID);
			output.collect(vertexEdgeInfo, vertexGraphidInfo);


		}
	}


	/**
	 * Mapper A - reads in edges and outputs < graph id, edge info >
	 * @author Steven Hill
	 */
	public static class MapperA
	extends MapReduceBase implements Mapper<Object, Text, Text, Text>{

		// used to store graph id and edge information
		private Text graphID = new Text();
		private Text structInfo = new Text();

		// mapping function
		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {

			/**
			 * <graph id>_<vertex-edge info>_<vertex numbers>
			 */

			String[] structureInformation = value.toString().split("_");

			graphID.set(structureInformation[0]);
			structInfo.set(structureInformation[1] + "_" + structureInformation[2]);

			output.collect(graphID, structInfo);

		}
	}


	/**
	 * Reducer A - takes in all k-sized substructures for a specific graph and builds
	 * all (k+1)-sized substructures
	 * @author Steven Hill
	 */
	public static class ReducerA 
	extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

		// used to output path info
		private Text empty = new Text("");
		private Text result = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, 
				Reporter reporter)
		throws IOException {

			// linked list holding all of the substructures made of edges
			LinkedList<LinkedList<String[]>> subgraphs = new LinkedList<LinkedList<String[]>>();	
			HashMap<String,LinkedList<String>> singleEdges = new HashMap<String,LinkedList<String>>();
			HashMap<String,LinkedList<String>> singleEdgesFrom = new HashMap<String,LinkedList<String>>();
			HashSet<String> visitedEdges = new HashSet<String>();
			
			{ // Used to remove temporary data

				// hold on substructures passed in from the mapper with same id
				LinkedList<String[]> substructures = new LinkedList<String[]>();
				while (values.hasNext()) {
					substructures.add(values.next().toString().split("_"));
				}

				for (String[] info : substructures) {
					String[] vertexEdgeInfo = info[0].replaceAll("\\(", "").split("\\)");
					String[] vertexNumbInfo = info[1].replaceAll("\\(", "").split("\\)");

					LinkedList<String[]> subgraph = new LinkedList<String[]>();

					for (int i = 0; i < vertexEdgeInfo.length; i++) {

						String[] partsOfVertexEdge = vertexEdgeInfo[i].split(":");
						String fromVertexLabel = partsOfVertexEdge[0];
						String[] verticesToGoTo = partsOfVertexEdge[1].split(",");

						String[] partsOfVertexNum = vertexNumbInfo[i].split(":");
						String fromVertexNumb = partsOfVertexNum[0];
						String[] verticesNumbToGoTo = partsOfVertexNum[1].split(",");


						for (int j = 0; j < verticesToGoTo.length; j++) {

							// <edge label>-<node label>
							String toVertex =  verticesToGoTo[j];
							String[] toVertexInfo = toVertex.split("-");
							String edgeLabel = toVertexInfo[0];
							String toNodeLabel = toVertexInfo[1];
							String toNodeNumb = verticesNumbToGoTo[j];

							String[] subgraphInfo = {fromVertexNumb, fromVertexLabel, edgeLabel, toNodeNumb, toNodeLabel};

							subgraph.add(subgraphInfo);

							// add to list of single edges
							if (singleEdges.get(fromVertexNumb + " " + fromVertexLabel) == null) {
								singleEdges.put(fromVertexNumb + " " + fromVertexLabel, new LinkedList<String>());
							}
							if (singleEdgesFrom.get(toNodeNumb + " " + toNodeLabel) == null) {
								singleEdgesFrom.put(toNodeNumb + " " + toNodeLabel, new LinkedList<String>());
							}

							
							String from = fromVertexNumb + " " + fromVertexLabel;
							String to = edgeLabel + " " + toNodeNumb + " " + toNodeLabel;
							String total = from + " " + to;
							
							if (!visitedEdges.contains(total)) {
								singleEdges.get(from).add(to);
								if (singleEdgesFrom.get(toNodeNumb + " " + toNodeLabel) == null) {
									System.out.println("IT IS NULL FOR SOME REASON!");
								}
								singleEdgesFrom.get(toNodeNumb + " " + toNodeLabel).add(edgeLabel + " " + fromVertexNumb + " " + fromVertexLabel);
								visitedEdges.add(total);
							}

						}


					}

					subgraphs.add(subgraph);

				}

			} // end of temporary data

			

			TreeSet<String> newlyFormedStructures = new TreeSet<String>();
			// build (k+1)-sized subgraphs
			for (LinkedList<String[]> substructure : subgraphs) {

				HashSet<String> seenEdges = new HashSet<String>();
				for (String[] edge : substructure) {
					seenEdges.add(edge[0] + " " + edge[1] + " " + edge[2] + " " + edge[3] + " " + edge[4]);
				}

				for (String edge[] : substructure) {
					String fromNode = edge[0] + " " + edge[1];
					String toNode = edge[3] + " " + edge[4];
					
					LinkedList<String> adjacentEdges1 = singleEdges.get(fromNode);
					LinkedList<String> adjacentEdges2 = singleEdges.get(toNode);
					LinkedList<String> adjacentFromEdges = singleEdgesFrom.get(toNode);

					if (adjacentEdges1 != null) {
						for (String sEdge : adjacentEdges1) {
							String newEdge = fromNode + " " + sEdge;
							if (seenEdges.contains(newEdge)) {
								continue;
							}
							seenEdges.add(newEdge);
							newlyFormedStructures.add(addAndSort(substructure, newEdge));
						}
					}
					
					if (adjacentEdges2 != null) {
						for (String sEdge : adjacentEdges2) {
							String newEdge = toNode + " " + sEdge;
							if (seenEdges.contains(newEdge)) {
								continue;
							}
							seenEdges.add(newEdge);
							newlyFormedStructures.add(addAndSort(substructure, newEdge));
						}
					}
					
					if (adjacentFromEdges != null) {
						for (String sEdge : adjacentFromEdges) {
							String[] partOfEdge = sEdge.split(" ");
							String newEdge = partOfEdge[1] + " " + partOfEdge[2] + " " + partOfEdge[0] + " " + toNode;
							if (seenEdges.contains(newEdge)) {
								continue;
							}
							seenEdges.add(newEdge);
							newlyFormedStructures.add(addAndSort(substructure, newEdge));
						}
						
					}
					

				}

			}

			for (String s : newlyFormedStructures) {
				result.set(s + "_" + key.toString());
				output.collect(result, empty);
			}

		}



		private String addAndSort(LinkedList<String[]> edges, String newEdge) {

			HashMap<String,Module> modules = new HashMap<String,Module>();

			String[] partsOfNewEdge = newEdge.split(" ");

			Module toModule = new Module(partsOfNewEdge[0], partsOfNewEdge[1]);
			toModule.addEdge(partsOfNewEdge[2], partsOfNewEdge[3], partsOfNewEdge[4]);
			modules.put(partsOfNewEdge[0] + " " + partsOfNewEdge[1], toModule);

			for (String[] partsOfEdge : edges) {

				if (modules.get(partsOfEdge[0] + " " + partsOfEdge[1]) != null) {
					modules.get(partsOfEdge[0] + " " + partsOfEdge[1]).addEdge(partsOfEdge[2], partsOfEdge[3], partsOfEdge[4]);	
				} else {
					Module module = new Module(partsOfEdge[0], partsOfEdge[1]);
					module.addEdge(partsOfEdge[2], partsOfEdge[3], partsOfEdge[4]);
					modules.put(partsOfEdge[0] + " " + partsOfEdge[1], module);
				}


			}

			
			TreeSet<Module> allModules = new TreeSet<Module>();
			for (Module m : modules.values()) {
				allModules.add(m);
			}
			
			HashMap<String,Integer> numberingSeenLabels = new HashMap<String,Integer>();
			HashMap<String,Integer> differingSeenLabels = new HashMap<String,Integer>();
			
			StringBuffer vertexInfoString = new StringBuffer();
			StringBuffer vertexNumbersString = new StringBuffer();
			for (Module m : allModules) {

				vertexInfoString.append("(");
				vertexNumbersString.append("(");

				
				// for the from-vertex
				if (numberingSeenLabels.get(m.label) == null) {
					numberingSeenLabels.put(m.label, 0);
				} 
				if (differingSeenLabels.get(m.label + " " + m.number) == null) {

					int val = numberingSeenLabels.get(m.label) + 1;
					numberingSeenLabels.put(m.label, val);
					differingSeenLabels.put(m.label + " " + m.number, val);
					
				} 
				
				vertexInfoString.append(m.label + "^" + differingSeenLabels.get(m.label + " " + m.number) + ":");
				vertexNumbersString.append(m.number + ":");

				for (String[] partsOfNeighbor : m.getNeighbors()) {

					// for the to-vertex
					if (numberingSeenLabels.get(partsOfNeighbor[2]) == null) {
						numberingSeenLabels.put(partsOfNeighbor[2], 0);
					} 
					if (differingSeenLabels.get(partsOfNeighbor[2] + " " + partsOfNeighbor[1]) == null) {

						int val = numberingSeenLabels.get(partsOfNeighbor[2]) + 1;
						numberingSeenLabels.put(partsOfNeighbor[2], val);
						differingSeenLabels.put(partsOfNeighbor[2] + " " + partsOfNeighbor[1], val);
						
					} 


					vertexInfoString.append(partsOfNeighbor[0] + "-" + partsOfNeighbor[2] + "^" + differingSeenLabels.get(partsOfNeighbor[2] + " " + partsOfNeighbor[1]) + ",");
					vertexNumbersString.append(partsOfNeighbor[1] + ",");

				}

				vertexInfoString.append(")");
				vertexNumbersString.append(")");

			}

			return vertexInfoString.append("_" + vertexNumbersString).toString();

		}


		// class to encompass
		private class Module implements Comparable<Module> {

			String number;
			String label;
			TreeMap<String,LinkedList<String>> adjacentEdges = new TreeMap<String,LinkedList<String>>();

			private Module(String number, String label) {
				this.number = number;
				this.label = label;
			}

			private void addEdge(String edgeLabel, String toLabel, String toNumber){
				
				String firstPart = edgeLabel + " " + toLabel;
				
				if (adjacentEdges.get(firstPart) == null) {
					adjacentEdges.put(edgeLabel + " " + toLabel, new LinkedList<String>());
				}
				
				adjacentEdges.get(edgeLabel + " " + toLabel).add(toNumber);
			}

			public LinkedList<String[]> getNeighbors(){

				LinkedList<String[]> neighbors = new LinkedList<String[]>();
				
				for (String key : adjacentEdges.keySet()) {
					String[] info = key.split(" ");
					String edgeLabel = info[0];
					String toLabel = info[1];
					for (String numb : adjacentEdges.get(key)) {
						String[] returnedValues = {edgeLabel, toLabel, numb};
						neighbors.add(returnedValues);
					}
				}
				
				return neighbors;

			}

			public String toString() {

				StringBuffer appendedString = new StringBuffer();
				StringBuffer appendedNumbers = new StringBuffer();
				appendedString.append(this.label);
				appendedNumbers.append(" " + this.number);
				for (String key : adjacentEdges.keySet()) {
					appendedString.append(" " + key);
					for (String numb : adjacentEdges.get(key)) {
						appendedNumbers.append(" " + numb);
					}
				}
				
				return appendedString.append(appendedNumbers).toString();
			}

			public int compareTo(Module other) {
				
				if (this.label.compareTo(other.label) < 0) {
					return -1;
				} else if (this.label.compareTo(other.label) > 0) {
					return 1;
				} else {
					return this.toString().compareTo(other.toString());
				}
			}

		}

	}


	/**
	 * Mapper B - takes in all structures and sends them to correct reducer for
	 * counting on structure
	 * @author smhill
	 */
	public static class MapperB
	extends MapReduceBase implements Mapper<Object, Text, Text, Text>{

		// used to store output path key and edge information
		private Text substructure = new Text();
		private Text otherInfo = new Text();

		// mapper function
		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {

			/*
			 * parts[0] - substructure with labels
			 * parts[1] - node numbers
			 * parts[2] - graph id
			 */

			// splits based on "_" marker to get [0] label-only path, and [1] edge information
			String[] parts = value.toString().split("_");
			substructure.set(parts[0]);
			otherInfo.set(parts[1] + "_" + parts[2]);

			// output: key->label-only path, value: edge information
			output.collect(substructure, otherInfo);
		}
	}


	// REDUCER B
	public static class ReducerB 
	extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

		private Text result = new Text();
		private Text empty = new Text("");
		private int support;


		public void configure(JobConf job) {
			support = Integer.parseInt(job.get("support"));
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, 
				Reporter reporter)
		throws IOException {



			HashSet<String> graphIDs = new HashSet<String>();
			LinkedList<String> vertexInfo = new LinkedList<String>();
			int count = 0;

			while (values.hasNext()) {
				String val = values.next().toString();
				vertexInfo.add(val);

				String[] parts = val.trim().split("_");
				String graphID = parts[1];

				if (!graphIDs.contains(graphID)) {
					count++;
					graphIDs.add(graphID);
				} 
			}

			if (count >= support) {
				for (String v :vertexInfo) {
					
					// ignore special bookkeeping notation
					StringBuffer realInfo = new StringBuffer();
					boolean flag = false;
					for (char c : key.toString().toCharArray()) {
						if (c == '^') {
							flag = true;
						} else if (c == ':' || c == ',' || c == ')') {
							flag = false;
						}
						if (flag == false) {
							realInfo.append(c);
						}
					}
					
					
					String[] numbersAndId = v.trim().split("_");
					result.set(numbersAndId[1] + "_" + realInfo + "_" + numbersAndId[0]);
					output.collect(result, empty);
				}
			} 

		}
	}


	public static void main(String[] args) throws Exception {


		if (args.length != 4) {
			System.err.println("Usage: <in> <out> <iterations> <support>");
			System.exit(2);
		}

		int iterations = Integer.parseInt(args[2]);
		String support = args[3];

		JobConf jobconfA;
		Job jobA;
		JobConf jobconfB;
		Job jobB;
		JobConf jobconfResults;
		Job jobResults;


		JobControl jc = new JobControl("- FSM controlled job -");
		Thread control = new Thread(jc);
		long start = System.currentTimeMillis();

		jobconfB = new JobConf(new Configuration());
		jobB = new Job(jobconfB);
		jobconfB.setJarByClass(FSGMv5.class);
		jobconfB.setMapperClass(ParserMapper.class);
		jobconfB.setReducerClass(ReducerB.class);
		jobconfB.setOutputKeyClass(Text.class);
		jobconfB.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobconfB, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobconfB, new Path(args[1]+"/outputMapRedB1"));
		jobconfB.set("support", support);

		jc.addJob(jobB);



		for (int it = 2; it <= iterations; it++) {

			jobconfA = new JobConf(new Configuration());
			jobA = new Job(jobconfA);
			jobconfA.setJarByClass(FSGMv5.class);
			jobconfA.setMapperClass(MapperA.class);
			jobconfA.setReducerClass(ReducerA.class);
			jobconfA.setOutputKeyClass(Text.class);
			jobconfA.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(jobconfA, new Path(args[1]+"/outputMapRedB" + (it-1)));
			FileOutputFormat.setOutputPath(jobconfA, new Path(args[1]+"/outputMapRedA" + it));

			jobA.addDependingJob(jobB);
			jc.addJob(jobA);

			jobconfB = new JobConf(new Configuration());
			jobB = new Job(jobconfB);
			jobconfB.setJarByClass(FSGMv5.class);
			jobconfB.setMapperClass(MapperB.class);
			jobconfB.setReducerClass(ReducerB.class);
			jobconfB.setOutputKeyClass(Text.class);
			jobconfB.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(jobconfB, new Path(args[1]+"/outputMapRedA" + it));
			FileOutputFormat.setOutputPath(jobconfB, new Path(args[1]+"/outputMapRedB" + it ));
			jobconfB.set("support", support);

			jobB.addDependingJob(jobA);
			jc.addJob(jobB);

		}


		control.start();
		while (!jc.allFinished()) {
			try {
				Thread.yield();
			}
			catch (Exception e) {}
		}
		control.stop();


		long end = System.currentTimeMillis();
		System.out.println("running time " + (end - start) / 1000 + "s");

	}




}

