package SanderGiedo;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Neo4jThings {
    GraphDatabaseService neo4j;
    BatchInserter neo4jInserter;
    PathFinder<Path> finder;
    String DB_PATH = "graph.db";
    String DATA_PATH = "fb_days_version";
    Label label;
    RelationshipType relationshipType;


    public static void main(String[] args) throws IOException {
        Neo4jThings neo4jThings = new Neo4jThings();

//        neo4jThings.loadFromFile();

        neo4jThings.openConnectionToNeo4j();
//        neo4jThings.findAllValidPathsBetweenEverything();
        neo4jThings.findPathBetweenRandomNodes();
    }

    public Neo4jThings() throws IOException {

        label = DynamicLabel.label("Node");
        relationshipType = DynamicRelationshipType.withName("edgeTo");
    }

    public void findPathBetweenRandomNodes() {
        int count = 0;
//        GlobalGraphOperations ggo = GlobalGraphOperations.at(neo4j);
//        double totalNodes = IteratorUtil.count(ggo.getAllNodes());

        long startTime = System.nanoTime();
        for (int i = 0; i < 150; i++) {
            count++;
            try {
                findValidPathBetweenNodes(Math.round(Math.random() * 50000), Math.round(Math.random() * 50000));
            } catch(Exception e) {
                System.out.println(e);
            }
        }

        long endTime = System.nanoTime();
        long totalTime = (endTime - startTime);
        System.out.println("Total time(ms): " + totalTime / 1000000 + " Average time per path(ms):" + (totalTime / (count+1)) / 1000000);

    }

    public void findAllValidPathsBetweenEverything() {
        System.out.println("findAllValidPathsBetweenEverything()");

        int count = 0;
        GlobalGraphOperations ggo = GlobalGraphOperations.at(neo4j);
        try (Transaction tx = neo4j.beginTx()) {
            double totalNodes = IteratorUtil.count(ggo.getAllNodes());
            long startTime = System.nanoTime();
            for (Node startNode : ggo.getAllNodes()) {
                System.out.println(startNode);
                count++;
                for (Node endNode : ggo.getAllNodes()) {
                    if (startNode.getId() == endNode.getId()) {
                        continue;
                    }
                    findValidPathBetweenNodes(startNode, endNode);
                    if(count % 100 == 1){
                        System.out.print("\r Progress: " + count/totalNodes + "%");
                    }
                }
            }
            long endTime = System.nanoTime();
            long totalTime = (endTime - startTime);
            System.out.println("Total time(ms): " + totalTime / 1000000 + " Average time per(ms):" + (totalTime / (count+1)) / 1000000);
        }
        System.out.println("findAllValidPathsBetweenEverything() finished");

    }

    public boolean findValidPathBetweenNodes(Node startNode, Node endNode){

        System.out.println("findValidPathBetweenNodes()" + startNode + ", " + endNode);
        Iterable<Path> paths = finder.findAllPaths(startNode, endNode); // Out of memory
        int intervalStart = Integer.MIN_VALUE;
        int intervalEnd = Integer.MAX_VALUE;
        System.out.println("paths size: " + IteratorUtil.count(paths));
        for (Path path : paths) {
            for (Relationship relationship : path.relationships()) {
                int edgeStart = (Integer) relationship.getProperty("Start");
                int edgeEnd = (Integer) relationship.getProperty("End");
                intervalStart = Math.max(intervalStart, edgeStart);
                intervalEnd = Math.min(intervalEnd, edgeEnd);
            }
            if (intervalEnd >= intervalStart) {
                System.out.println("findValidPathBetweenNodes() finished true");
                return true;
            }
        }
        System.out.println("findValidPathBetweenNodes() finished false");
        return false;
    }


    public boolean findValidPathBetweenNodes(long startNodeId, long endNodeId){
        try(Transaction tx = neo4j.beginTx()) {
            Node node1 = neo4j.getNodeById(startNodeId);
            Node node2 = neo4j.getNodeById(endNodeId);
            System.out.println("Node1: " + node1 + ", Node2: " + node2);
            Path path = finder.findSinglePath(node1, node2);
            System.out.println("Paths found!");
            if(path.length() > 0) {
                return true;

            }
//            int intervalStart = Integer.MIN_VALUE;
//            int intervalEnd = Integer.MAX_VALUE;
//            System.out.println("Going to iterate");
//            for ( Iterator<Path> it = paths.iterator(); it.hasNext(); ) {
//                Path path = it.next();
//                System.out.println("interating path: " + path);
//                Iterable<Relationship> relationships = path.relationships();
//                System.out.println("Relationships found!");
//                for (Relationship relationship : relationships) {
//                    int edgeStart = (Integer) relationship.getProperty("Start");
//                    int edgeEnd = (Integer) relationship.getProperty("End");
//                    intervalStart = Math.max(intervalStart, edgeStart);
//                    intervalEnd = Math.min(intervalEnd, edgeEnd);
//                }
//                if (intervalEnd >= intervalStart) {
//                    System.out.println("findValidPathBetweenNodes() finished true");
//                    return true;
//                }
//            }
        }
        System.out.println("findValidPathBetweenNodes() finished false");
        return false;
    }

    private void openConnectionToNeo4j(){
        System.out.println("openConnectionToNeo4j()");
        neo4j = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        int MAX_DEPTH = 15;
        finder = GraphAlgoFactory.allSimplePaths(
                PathExpanders.forTypeAndDirection(relationshipType, Direction.OUTGOING), MAX_DEPTH);
        System.out.println("openConnectionToNeo4j() finished");
    }

    public void loadFromFile() throws IOException {
        System.out.println("loadFromFile()");
        File deleteGraph = new File(DB_PATH);
        FileUtils.deleteRecursively(deleteGraph);
        neo4jInserter = BatchInserters.inserter(DB_PATH);
        String line;
        String[] splitLine;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(DATA_PATH));
        line = bufferedReader.readLine(); //Stupid header
        while((line = bufferedReader.readLine()) != null) {
            splitLine = line.split("\t");
            insert(splitLine);
        }
        bufferedReader.close();
        neo4jInserter.shutdown();
        System.out.println("loadFromFile() finished");
    }

    private void insert(String[] splitLine){
        long startNodeId = new Long(splitLine[0]);
        long endNodeId = new Long(splitLine[1]);
        String[] interval = splitLine[2].split(",");
        int intervalStart = new Integer(interval[0]);
        int intervalEnd = new Integer(interval[1]);
        getOrCreateNode(startNodeId);
        getOrCreateNode(endNodeId);
        Map<String, Object> intervalMap = new HashMap<String, Object>();
        intervalMap.put("Start", intervalStart);
        intervalMap.put("End", intervalEnd);
        neo4jInserter.createRelationship(startNodeId, endNodeId, relationshipType, intervalMap);
    }

    private void getOrCreateNode(long id){
        if(!neo4jInserter.nodeExists(id)){
            neo4jInserter.createNode(id, null, label);
        }
    }
}
