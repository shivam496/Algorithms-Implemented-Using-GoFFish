package in.dream_lab.goffish.sample;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.sample.ByteArrayHelper.Reader;
import in.dream_lab.goffish.sample.ByteArrayHelper.Writer;
import in.dream_lab.goffish.api.AbstractSubgraphComputation;

/*
 * Counts all the 4-cycles found in a directed graph. A 4-cycle 
 * or quadrilateral in a graph is a cycle containing 4 edges and 
 * vertices in which neither of them repeats. 4-cycle can be
 * classified into seven types based on the location of its vertices.
 *
 * @author Shivam Singh
 *
 * Link to Code : https://github.com/shivam496/Algorithms-Implemented-Using-GoFFish/blob/master/QuadCount.java
 * Link to Report : https://github.com/shivam496/Algorithms-Implemented-Using-GoFFish/blob/master/Report.pdf
 * Link to Video : https://drive.google.com/file/d/1P7b9jxJUtx0EraCpdH5GhqH3ySjQShGB
 *
 */

public class QuadCount extends AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, BytesWritable, LongWritable, LongWritable, LongWritable> {
    // Final Count of 4-cycles
    private long quadCount=0;
    public static final Log LOG = LogFactory.getLog(QuadCount.class);
    
    @Override
    public void compute(Iterable<IMessage<LongWritable, BytesWritable>> messageList) throws IOException {
    	// Diff. variables for different types of 4-cycles
    	float type1=0,type2=0,type3=0,type4=0,type5=0,type6=0,type7=0;
        if (getSuperstep() == 0) {
        	Map<Long, ByteArrayHelper.Writer> outputMessages = new HashMap<Long, ByteArrayHelper.Writer>();
            
        	// Counting Type1 4-cycles where all are local vertices
            for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> firstVertex : getSubgraph().getLocalVertices()) {
                for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                    IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                	if(secondVertex.isRemote())
                	{	// For Type - 5,6,7 4-cycles
                		@SuppressWarnings("unchecked")
                       	long nextSGId = ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex).getSubgraphId().get();
                        long currentSGId = getSubgraph().getSubgraphId().get();
                        step1Zip(outputMessages, nextSGId, currentSGId, secondVertex.getVertexId().get(),firstVertex.getVertexId().get(), -3);
	                    continue;
                	}
                	if(secondVertex.getVertexId().get() == firstVertex.getVertexId().get())
                		continue;
                	for (IEdge<LongWritable, LongWritable, LongWritable> secondEdge : secondVertex.getOutEdges()) {
                    	IVertex<LongWritable, LongWritable, LongWritable, LongWritable> thirdVertex = getSubgraph().getVertexById(secondEdge.getSinkVertexId());
                    	if(thirdVertex.isRemote())
	                	{	// For Type - 3,4 4-cycles
                			@SuppressWarnings("unchecked")
	                       	long nextSGId = ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) thirdVertex).getSubgraphId().get();
	                        long currentSGId = getSubgraph().getSubgraphId().get();
	                        step1Zip(outputMessages, nextSGId, currentSGId, thirdVertex.getVertexId().get(),firstVertex.getVertexId().get(), -2);
		                    continue;
	                	}
                    	if(thirdVertex.getVertexId().get() == firstVertex.getVertexId().get() || thirdVertex.getVertexId().get() == secondVertex.getVertexId().get())
                    		continue;
                    	for (IEdge<LongWritable, LongWritable, LongWritable> thirdEdge : thirdVertex.getOutEdges()) {
	                        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> fourthVertex = getSubgraph().getVertexById(thirdEdge.getSinkVertexId());
	                        if(fourthVertex.isRemote())
	                        {	// For Type - 2 4-cycle
                				@SuppressWarnings("unchecked")
                        		long nextSGId = ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) fourthVertex).getSubgraphId().get();
                        		long currentSGId = getSubgraph().getSubgraphId().get();
                        		step1Zip(outputMessages, nextSGId, currentSGId, fourthVertex.getVertexId().get(),firstVertex.getVertexId().get(), -1);
	                        	continue;
	                        }
	                        if(fourthVertex.getVertexId().get() == secondVertex.getVertexId().get() || thirdVertex.getVertexId().get() == fourthVertex.getVertexId().get() || fourthVertex.getVertexId().get() == firstVertex.getVertexId().get())
                    			continue;
                    		// Found all 4 vertices of 4-cycle in local Sub-graph, so increasing count
	                        if (fourthVertex.getOutEdge(firstVertex.getVertexId()) != null)
	                        	type1 += 0.25;
                    	}
                	}
                }
            }
            System.out.println("Count of Type1 4-cycles whose Last Vertex is in Subgraph " + getSubgraph().getSubgraphId().get() + " is : " + type1);
            quadCount += type1;
            // Send message to other Sub-graphs for next Super-Step
            sendMessages(outputMessages);
        } else if (getSuperstep() == 1) {
        	Map<Long, List<Long>> transitionIdsMap = new HashMap<Long, List<Long>>();
            Map<Long, Writer> outputMessages = new HashMap<Long, Writer>();
            step1Unzip(messageList, transitionIdsMap);
            for (Map.Entry<Long, List<Long>> secondToFirst : transitionIdsMap.entrySet()) {
                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> firstVertex = getSubgraph().getVertexById(new LongWritable(secondToFirst.getKey()));
                List<Long> firstList = secondToFirst.getValue();
                for(int i = 0; i < firstList.size(); i+=3)
                {	if(firstList.get(i) == -1)
                	{	// For Type - 2 4-cycle
                		for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                			// Found all vertices of Type-2 4-cycles
                			if(secondVertex.isRemote() && secondVertex.getVertexId().get() == firstList.get(i+2))
		                			type2 += 1;
                		}
                	}
                	else if(firstList.get(i) == -2)
                	{	// For Type - 3,4 4-cycle
                		for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                			if(secondVertex.isRemote())
		                	{	if(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex).getSubgraphId().get() != firstList.get(i+1))
		                		{	// For Type-4, 4-cycle
		                			@SuppressWarnings("unchecked")
			                       	long secondSGId = ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex).getSubgraphId().get();
			                        long firstSGId = getSubgraph().getSubgraphId().get();
			                        step2Zip(outputMessages, secondSGId, firstSGId, firstList.get(i+1), secondVertex.getVertexId().get(), firstList.get(i+2), firstVertex.getVertexId().get(), -1);
				                }
				                continue;
		                	}
		                	if(secondVertex.getVertexId().get() == firstVertex.getVertexId().get())
		                		continue;
		                	for (IEdge<LongWritable, LongWritable, LongWritable> secondEdge : secondVertex.getOutEdges()) {
	                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> thirdVertex = getSubgraph().getVertexById(secondEdge.getSinkVertexId());
	                			// Found all vertices of Type-3 4-cycle
	                			if(thirdVertex.getVertexId().get() == firstList.get(i+2))
			                		type3 += 0.5;
                			}
                		}
                	}
                	else
                	{	// For Type - 5,6 and 7, 4-cycle
                		for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                			if(!secondVertex.isRemote())
                				continue;
                			if(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex).getSubgraphId().get() != firstList.get(i+1))
		                	{	// For Type - 5 and 6, 4-cycles because third vertex is of same type in both
		                		@SuppressWarnings("unchecked")
		                       	long thirdSGId = ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex).getSubgraphId().get();
		                       	long currentSGId = getSubgraph().getSubgraphId().get();
		                        step2Zip(outputMessages, thirdSGId, currentSGId, firstList.get(i+1), secondVertex.getVertexId().get(), firstList.get(i+2), firstVertex.getVertexId().get(), -2);
		                	}
		                	else
		                	{	if(secondVertex.getVertexId().get() != firstList.get(i+2))
		                		{	// For Type-7, 4-cycle
		                			@SuppressWarnings("unchecked")
		                       		long currentSGId = getSubgraph().getSubgraphId().get();
		                        	step2Zip(outputMessages, firstList.get(i+1), currentSGId, firstList.get(i+1), secondVertex.getVertexId().get(), firstList.get(i+2), firstVertex.getVertexId().get(), -3);
		                		}
		                	}
                		}
                	}
                }
            }
            System.out.println("Count of Type2 4-cycles whose Last Vertex is in Subgraph " + getSubgraph().getSubgraphId().get() + " is : " + type2);
            System.out.println("Count of Type3 4-cycles whose Last Vertex is in Subgraph " + getSubgraph().getSubgraphId().get() + " is : " + type3);
            quadCount += type2;
            quadCount += type3;
            // Send message to other Sub-graphs for next Super-Step
            sendMessages(outputMessages);
        } else if (getSuperstep() == 2) {
            Map<Long, List<Long>> transitionIdsMap = new HashMap<Long, List<Long>>();
            Map<Long, Writer> outputMessages = new HashMap<Long, Writer>();
            step2Unzip(messageList, transitionIdsMap);
            for (Map.Entry<Long, List<Long>> secondToFirst : transitionIdsMap.entrySet()) {
                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> firstVertex = getSubgraph().getVertexById(new LongWritable(secondToFirst.getKey()));
                List<Long> firstList = secondToFirst.getValue();
                for(int i = 0; i < firstList.size(); i+=5)
                {	if(firstList.get(i) == -1)
                	{	// For Type-4, 4-cycle
                		for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                			// Got all vertices of Type-4, 4-cycles
                			if(secondVertex.getVertexId().get() == firstList.get(i+3))
		                		type4 += 1;
                		}
                	}
                	else if(firstList.get(i) == -2)
                	{	// For Type-5 and 6, 4-cycles
                		for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                			if(secondVertex.isRemote())
		                	{	if(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex).getSubgraphId().get() == firstList.get(i+1))
		                		{	// If 4th vertex belongs to second subgraph then it corresponds to Type-6, 4-cycle
		                			if(secondVertex.getVertexId().get() != firstList.get(i+4))
		                				step3Zip(outputMessages, firstList.get(i+1), secondVertex.getVertexId().get(), firstList.get(i+3), -2);
				                }
				                else
				                {	if(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex).getSubgraphId().get() != firstList.get(i+2))
		                			{	// Here we have 4th vertex of Type-5, 4-cycle (All vertices in different Sub-graphs)
		                				@SuppressWarnings("unchecked")
		                				long fourthSGId = ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex).getSubgraphId().get();
				                       	step3Zip(outputMessages, fourthSGId, secondVertex.getVertexId().get(), firstList.get(i+3), -1);
				                    }
				                }
		                	}
                		}
                	}
                	else
                	{	// For Type-7, 4-cycle
                		for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                			// Got a candidate for last vertex of Type-7, 4-cycle
                			if((secondVertex.isRemote()) && (((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex).getSubgraphId().get() == firstList.get(i+1)) && (secondVertex.getVertexId().get() != firstList.get(i+4)))
		                		step3Zip(outputMessages, firstList.get(i+1), secondVertex.getVertexId().get(),firstList.get(i+3), -3);
                		}
                	}
                }
            }
            System.out.println("Count of Type4 4-cycles whose Last Vertex is in Subgraph " + getSubgraph().getSubgraphId().get() + " is : " + type4);
            quadCount += type4;
            // Send message to other Sub-graphs for next Super-Step
            sendMessages(outputMessages);
        }
        else {
        	Map<Long, List<Long>> transitionIdsMap = new HashMap<Long, List<Long>>();
            step3Unzip(messageList, transitionIdsMap);
            for (Map.Entry<Long, List<Long>> secondToFirst : transitionIdsMap.entrySet()) {
                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> firstVertex = getSubgraph().getVertexById(new LongWritable(secondToFirst.getKey()));
                List<Long> firstList = secondToFirst.getValue();
                for(int i = 0; i < firstList.size(); i+=2)
                {	if(firstList.get(i) == -1)
                	{	// For Type-5, 4-cycle
                		for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                			// Confirms that 4th vertex forms Type-5, 4-cycle
                			if(secondVertex.getVertexId().get() == firstList.get(i+1))
		                		type5 += 0.25;
                		}
                	}
                	else if(firstList.get(i) == -2)
                	{	// For Type-6, 4-cycle
                		for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                			// Confirms that 4th vertex forms Type-6, 4-cycle
                			if(secondVertex.getVertexId().get() == firstList.get(i+1))
		                		type6 += 0.5;
                		}
                	}
                	else
                	{	// For Type-7, 4-cycle
                		for (IEdge<LongWritable, LongWritable, LongWritable> firstEdge : firstVertex.getOutEdges()) {
                			IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex = getSubgraph().getVertexById(firstEdge.getSinkVertexId());
                			// Confirms that 4th vertex forms Type-7, 4-cycle
                			if(secondVertex.getVertexId().get() == firstList.get(i+1))
		                		type7 += 0.25;
                		}
                	}
            	}
            }
            System.out.println("Count of Type5 4-cycles whose Last Vertex is in Subgraph " + getSubgraph().getSubgraphId().get() + " is : " + type5);
            quadCount += type5;
            System.out.println("Count of Type6 4-cycles whose Last Vertex is in Subgraph " + getSubgraph().getSubgraphId().get() + " is : " + type6);
            quadCount += type6;
            System.out.println("Count of Type7 4-cycles whose Last Vertex is in Subgraph " + getSubgraph().getSubgraphId().get() + " is : " + type7);
            quadCount += type7;
            
        }
        System.out.println("");
        // Passing Cummulative Count of all types of 4-cycles in one.
        getSubgraph().setSubgraphValue(new LongWritable(quadCount));
        // Every Sub-graph votes to halt the program
        voteToHalt();
    }

    // Zipping message send by each Sub-graph from Super-Step 0 to 1
    private void step1Zip(Map<Long, ByteArrayHelper.Writer> msg, long remoteSubgraphId, long localSubgraphID, long secondVid, long firstVid, long typeOfMessage) {
        Writer writer = msg.get(remoteSubgraphId);
        if (writer == null) {
            // We are taking buffer of default size, for bigger input graphs we can customize the size of buffer y passing size as parameter
            writer = new Writer();
            msg.put(remoteSubgraphId, writer);
        }
        // Pushing elements in order we want
        writer.writeLong(secondVid);
        writer.writeLong(typeOfMessage);
        writer.writeLong(localSubgraphID);
        writer.writeLong(firstVid);
    }

    // Unzipping message at Super-Step 1
    private void step1Unzip(Iterable<IMessage<LongWritable, BytesWritable>> messageList, Map<Long, List<Long>> transitionIdsMap) {
        for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
            BytesWritable message = messageItem.getMessage();
            ByteArrayHelper.Reader reader = new Reader(message.copyBytes());
            while (reader.available() >= 16) {
                // Getting elements in order we inserted
                long secondVid = reader.readLong();
                long typeOfMessage = reader.readLong();
                long localSubgraphID = reader.readLong();
                long firstVid = reader.readLong();
                List<Long> firstVidList = transitionIdsMap.get(secondVid);
                if (firstVidList == null) {
                    firstVidList = new LinkedList<Long>();
                    transitionIdsMap.put(secondVid, firstVidList);
                }
                firstVidList.add(typeOfMessage);
                firstVidList.add(localSubgraphID);
                firstVidList.add(firstVid);
            }
            // Pushing elements in order we want
            if (reader.available() > 0)
                throw new RuntimeException("reader is not empty but has less than 16 bytes. " + reader.available());
        }
    }

    // Zipping message send by each Sub-graph from Super-Step 1 to 2
    private void step2Zip(Map<Long, ByteArrayHelper.Writer> msg, long remoteSubgraphId, long localSubgraphID1, long localSubgraphID2, long secondVid, long firstVid, long previousVid, long typeOfMessage) {
        Writer writer = msg.get(remoteSubgraphId);
        if (writer == null) {
            // We are taking buffer of default size, for bigger input graphs we can customize the size of buffer y passing size as parameter
            writer = new Writer();
            msg.put(remoteSubgraphId, writer);
        }
        // Pushing elements in order we want
        writer.writeLong(secondVid);
        writer.writeLong(typeOfMessage);
        writer.writeLong(localSubgraphID1);
        writer.writeLong(localSubgraphID2);
        writer.writeLong(firstVid);
        writer.writeLong(previousVid);
    }

	// Unzipping message at Super-Step 2
    private void step2Unzip(Iterable<IMessage<LongWritable, BytesWritable>> messageList, Map<Long, List<Long>> transitionIdsMap) {
        for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
            BytesWritable message = messageItem.getMessage();
            ByteArrayHelper.Reader reader = new Reader(message.copyBytes());
            while (reader.available() >= 16) {
                // Getting elements in order we inserted
                long secondVid = reader.readLong();
                long typeOfMessage = reader.readLong();
                long localSubgraphID1 = reader.readLong();
                long localSubgraphID2 = reader.readLong();
                long firstVid = reader.readLong();
                long previousVid = reader.readLong();
                List<Long> firstVidList = transitionIdsMap.get(secondVid);
                if (firstVidList == null) {
                    firstVidList = new LinkedList<Long>();
                    transitionIdsMap.put(secondVid, firstVidList);
                }
                // Pushing elements in order we want
                firstVidList.add(typeOfMessage);
                firstVidList.add(localSubgraphID1);
                firstVidList.add(localSubgraphID2);
                firstVidList.add(firstVid);
                firstVidList.add(previousVid);
            }
            if (reader.available() > 0)
                throw new RuntimeException("reader is not empty but has less than 16 bytes. " + reader.available());
        }
    }

    // Zipping message send by each Sub-graph from Super-Step 2 to 3
    private void step3Zip(Map<Long, ByteArrayHelper.Writer> msg, long remoteSubgraphId, long secondVid, long firstVid, long typeOfMessage) {
        Writer writer = msg.get(remoteSubgraphId);
        if (writer == null) {
            // We are taking buffer of default size, for bigger input graphs we can customize the size of buffer y passing size as parameter
            writer = new Writer();
            msg.put(remoteSubgraphId, writer);
        }
        // Pushing elements in order we want
        writer.writeLong(secondVid);
        writer.writeLong(typeOfMessage);
        writer.writeLong(firstVid);
    }
	
	// Unzipping message at Super-Step 3
    private void step3Unzip(Iterable<IMessage<LongWritable, BytesWritable>> messageList, Map<Long, List<Long>> transitionIdsMap) {
        for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
            BytesWritable message = messageItem.getMessage();
            ByteArrayHelper.Reader reader = new Reader(message.copyBytes());
            while (reader.available() >= 16) {
                // Getting elements in order we inserted
                long secondVid = reader.readLong();
                long typeOfMessage = reader.readLong();
                long firstVid = reader.readLong();
                List<Long> firstVidList = transitionIdsMap.get(secondVid);
                if (firstVidList == null) {
                    firstVidList = new LinkedList<Long>();
                    transitionIdsMap.put(secondVid, firstVidList);
                }
                // Pushing elements in order we want
                firstVidList.add(typeOfMessage);
                firstVidList.add(firstVid);
            }
            if (reader.available() > 0)
                throw new RuntimeException("reader is not empty but has less than 16 bytes. " + reader.available());
        }
    }

    void sendMessages(Map<Long, ByteArrayHelper.Writer> msg) {
    	// Sending Message to all Sub-graphs according to Key set as Sub-graph Id to which it has to be sent
        for (Map.Entry<Long, ByteArrayHelper.Writer> m : msg.entrySet()) {
            BytesWritable message = new BytesWritable(m.getValue().getBytes());
            sendMessage(new LongWritable(m.getKey()), message);
        }
    }
}