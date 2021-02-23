/*
 * Licensed to the Ministry of Housing and Urban Affairs (MoHUA) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.gov.mohua.scm;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import java.util.UUID;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.util.JsonFormat;

import in.gov.mohua.ds.transit.*;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.FeedMessage;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.Junction;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.TrafficLane;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.Carriageway.VehicleDensity;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.TrafficLane.SignalTiming;

import in.gov.mohua.utils.UUID5;

@Tags({ "atcs", "json" })
@CapabilityDescription("Convert ATCS data (JSON) to ATCS standard.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class ATCSDataProcessing extends AbstractProcessor {

    Map<String, String> internalToExternalIdMap = null;
    // Map<String, JsonArray> junctionStageToLanes = null;
    Map<String, TrafficSignalRealtime.Junction.Builder> baseJunctionDataMap = null;

    Map<String, TrafficSignalRealtime.Junction.Builder> middlewareJunctionMap = null;
    Map<String, TrafficSignalRealtime.Carriageway.Builder> middlewareCarriagewayMap = null;
    Map<String, TrafficSignalRealtime.TrafficLane.Builder> middlewareTrafficLaneMap = null;
    Map<String, Integer> previousLanesUtilisedTime = null;

    TrafficSignalRealtime.FeedMessage.Builder feedMessageBuilder = null;

    // f048d42f-ffa2-3e9e-93e4-984f9b394d8e
    UUID namespace = UUID.nameUUIDFromBytes("in.gov.mohua.scm.fscl".getBytes());

    SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss aa");

    public static final PropertyDescriptor STATIC_DATA_FILE_PROPERTY = new PropertyDescriptor.Builder()
            .name("STATIC_DATA_FILE_PROPERTY").displayName("Static ATCS data file path")
            .description("Static ATCS data file").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder().name("success")
            .description("standardised data").build();

    public static final Relationship SUCCESS_PROTO_BINARY_RELATIONSHIP = new Relationship.Builder().name("success proto binary")
            .description("standardised proto binary data").build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder().name("failure")
            .description("failed to standardise").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(STATIC_DATA_FILE_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAILURE_RELATIONSHIP);
        relationships.add(SUCCESS_PROTO_BINARY_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);

        internalToExternalIdMap = new HashMap<String, String>();
        // junctionStageToLanes = new HashMap<String, JsonArray>();
        baseJunctionDataMap = new HashMap<String, TrafficSignalRealtime.Junction.Builder>();

        // This is intermidiate processed data from inbound real-time data.
        middlewareJunctionMap = new HashMap<String, TrafficSignalRealtime.Junction.Builder>();
        middlewareCarriagewayMap = new HashMap<String, TrafficSignalRealtime.Carriageway.Builder>();
        middlewareTrafficLaneMap = new HashMap<String, TrafficSignalRealtime.TrafficLane.Builder>();

        // Lane utilised time if only available in the next cycle.
        previousLanesUtilisedTime = new HashMap<String, Integer>();

        feedMessageBuilder = TrafficSignalRealtime.FeedMessage.newBuilder();
        feedMessageBuilder.setHeader(HeaderOuterClass.Header.newBuilder()
            .setIncrementality(HeaderOuterClass.Header.Incrementality.DIFFERENTIAL)
            .setProvider(HeaderOuterClass.Provider.newBuilder()
                .setId(namespace.toString())
                .setName("Faridabad Smart City Limited")
            )
            .setVersion("v0.0.1")
        );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);

        if (descriptor == STATIC_DATA_FILE_PROPERTY) {
            if (newValue == null || newValue.isEmpty()) {
                return;
            }

            baseJunctionDataMap.clear();

            TrafficSignalStatic.FeedMessage.Builder messageBuilder = TrafficSignalStatic.FeedMessage.newBuilder();

            try {
                JsonFormat.parser().merge(new FileReader(newValue), messageBuilder);

                TrafficSignalStatic.FeedMessage message = messageBuilder.build();

                for (TrafficSignalStatic.Junction junction : message.getJunctionsList()) {
                    TrafficSignalRealtime.Junction.Builder realtimeJunctionBuilder = TrafficSignalRealtime.Junction
                            .newBuilder();
                    realtimeJunctionBuilder.setId(junction.getId());

                    for (TrafficSignalStatic.Carriageway carriageway : junction.getCarriagewaysList()) {
                        TrafficSignalRealtime.Carriageway.Builder realtimeCarriagewayBuilder = TrafficSignalRealtime.Carriageway
                                .newBuilder();
                        realtimeCarriagewayBuilder.setId(carriageway.getId());

                        for (TrafficSignalStatic.TrafficLane trafficLane : carriageway.getTrafficLanesList()) {
                            TrafficSignalRealtime.TrafficLane.Builder realtimeTrafficLaneBuilder = TrafficSignalRealtime.TrafficLane
                                    .newBuilder();
                            realtimeTrafficLaneBuilder.setId(trafficLane.getId());

                            realtimeCarriagewayBuilder.addTrafficLanes(realtimeTrafficLaneBuilder);
                        }

                        realtimeJunctionBuilder.addCarriageways(realtimeCarriagewayBuilder);
                    }

                    baseJunctionDataMap.put(junction.getId(), realtimeJunctionBuilder);
                }

            } catch (FileNotFoundException e) {
                getLogger().error(e.getMessage());
                e.printStackTrace();
            } catch (IOException e) {
                getLogger().error(e.getMessage());
                e.printStackTrace();
            }

        } /*
           * else if (descriptor == MAPPING_FILE_PROPERTY) { if (newValue == null ||
           * newValue.isEmpty()) { return; }
           * 
           * getLogger().info("Modifying map for ids");
           * 
           * if (!newValue.equals(oldValue)) { try { JsonElement element =
           * JsonParser.parseReader(new FileReader(newValue)); JsonObject globalMapObj =
           * element.getAsJsonObject();
           * 
           * JsonArray junctionArray = globalMapObj.get("junctions").getAsJsonArray();
           * 
           * for(int i = 0; i < junctionArray.size(); i++) { JsonObject junctionElement =
           * junctionArray.get(i).getAsJsonObject(); String junctionInternalId =
           * junctionElement.get("internalId").getAsString();
           * 
           * internalToExternalIdMap.put( junctionInternalId,
           * junctionElement.get("externalId").getAsString() );
           * 
           * JsonArray wayArray = junctionElement.get("ways").getAsJsonArray();
           * 
           * for(int j = 0; j < wayArray.size(); j++) { JsonObject wayElement =
           * wayArray.get(j).getAsJsonObject(); String wayInternalId =
           * wayElement.get("internalId").getAsString();
           * 
           * internalToExternalIdMap.put( junctionInternalId + wayInternalId,
           * wayElement.get("externalId").getAsString() ); }
           * 
           * JsonArray laneArray = junctionElement.get("lanes").getAsJsonArray();
           * 
           * //junctionStageToLanes.put(junctionInternalId + wayInternalId, laneArray);
           * 
           * for(int k = 0; k < laneArray.size(); k++) { JsonObject laneElement =
           * laneArray.get(k).getAsJsonObject(); String laneInternalId =
           * laneElement.get("internalId").getAsString();
           * 
           * internalToExternalIdMap.put( junctionInternalId + laneInternalId,
           * laneElement.get("externalId").getAsString() ); } }
           * 
           * } catch (FileNotFoundException e) { e.printStackTrace(); }
           * 
           * } }
           */
    }

    private void processLanes(JsonObject internalData, JsonObject stageObject, String junctionInternalId,
            String junctionExternalId, String stageInternalId, String activeStageInternalId, Integer currentSeqNumber,
            Integer setSeqNumber) {
        if (stageInternalId.equals(activeStageInternalId)) {
            JsonArray trafficLaneArray = internalData.get("alLinkedPhaseJSON").getAsJsonArray();

            for (int l = 0; l < trafficLaneArray.size(); l++) {
                String phaseInternalId = trafficLaneArray.get(l).getAsJsonObject().get("nPhaseNo").getAsString();
                String phaseExternalId = getTrafficLaneUUID(junctionInternalId, phaseInternalId);

                // Timestamp in seconds.
                long updateTimestamp = 0;
                try {
                    updateTimestamp = sdf.parse(stageObject.get("tSystemTime").getAsString()).getTime() / 1000;
                } catch (ParseException e) {
                    updateTimestamp = System.currentTimeMillis() / 1000;
                    e.printStackTrace();
                }

                TrafficLane.Builder trafficLaneBuider = TrafficLane.newBuilder()
                    .setId(phaseExternalId)
                    .setSignalStatusValue(TrafficLane.SignalStatus.SIGNAL_GO_VALUE)
                    .setSignalTiming(SignalTiming.newBuilder()
                        .setAllocatedGreenSeconds(stageObject.get("nAllocatedGreen").getAsInt())
                        .setAvailableGreenSeconds(stageObject.get("nAvailableGreen").getAsInt())
                    )
                    .setUpdateTimestamp(updateTimestamp)
                    .setOperationalStatusValue(TrafficSignalRealtime.OperationalStatus.STATUS_NORMAL_OPERATION_VALUE);

                middlewareTrafficLaneMap.put(
                    phaseExternalId,
                    trafficLaneBuider
                );
            }
            
        } else {
            // Identify the previous sequence to fetch the utilisation time.

            // TODO: If currentSeqNo == 1??
            int seqNo = stageObject.get("nSeqno").getAsInt();
            if(seqNo < currentSeqNumber && seqNo > setSeqNumber) {
                setSeqNumber = seqNo;
                previousLanesUtilisedTime.put(
                    junctionExternalId,
                    stageObject.get("nUtilizedGreen").getAsInt()
                );
            }
        }
    }

    private String getUUID(String assetPath) {
        String assetCompletePath = "/" + assetPath;
        return UUID5.fromUTF8(namespace, assetCompletePath).toString();
    }

    private String getJunctionUUID(String junctionInternalId) {
        return getUUID("junction/" + junctionInternalId);
    }

    private String getCarriagewayUUID(String junctionInternalId, String stageInternalId) {
        return getUUID("junction/" + junctionInternalId + "/carriageway/" + stageInternalId);
    }

    private String getTrafficLaneUUID(String junctionInternalId, String phaseInternalId) {
        return getUUID("junction/" + junctionInternalId + "/traffic_lane/" + phaseInternalId);
    }

    // Called when junction status is on.
    private void processOnStateMessage(JsonObject internalData) {
        
        String junctionInternalId = internalData.get("sName").getAsString();
        String junctionExternalId = getJunctionUUID(junctionInternalId);

        // Set junction details in the intermidiate state.
        middlewareJunctionMap.put(
            junctionExternalId,
            TrafficSignalRealtime.Junction.newBuilder()
                .setId(junctionExternalId)
                .setOperationalStatusValue(
                    TrafficSignalRealtime.OperationalStatus.STATUS_NORMAL_OPERATION_VALUE
                )
        );
        
        JsonArray stageArray = internalData.get("alLinkedStageJSON").getAsJsonArray();
        String activeStageInternalId = internalData.get("nCurrentStageNo").getAsString();
        Integer currentSeqNumber = internalData.get("nCurrentSequenceNo").getAsInt();
        Integer setSeqNumber = 0;

        // Fetching stage (carriageway) level information
        for(int s = 0; s < stageArray.size(); s++) {
            JsonObject stageObject = stageArray.get(s).getAsJsonObject();

            String stageInternalId = stageObject.get("nStageno").getAsString();
            String stageExternalId = getCarriagewayUUID(junctionInternalId, stageInternalId);

            TrafficSignalRealtime.Carriageway.Builder carriagewayBuilder = TrafficSignalRealtime.Carriageway.newBuilder()
                .setId(stageExternalId)
                .setOperationalStatusValue(
                    TrafficSignalRealtime.OperationalStatus.STATUS_NORMAL_OPERATION_VALUE
                );
            
            if(stageObject.get("nVehicleCount").getAsInt() > -1) {
                carriagewayBuilder.setVehicleDensity(
                    VehicleDensity.newBuilder().setVehicleCountPerHour(
                        stageObject.get("nVehicleCount").getAsInt()
                    )
                );
            }

            processLanes(
                internalData, 
                stageObject, 
                junctionInternalId, 
                junctionExternalId,
                stageInternalId, 
                activeStageInternalId,
                currentSeqNumber,
                setSeqNumber
            );

            // Store carriageway information in the internal dataset.
            middlewareCarriagewayMap.put(
                stageExternalId,
                carriagewayBuilder
            );
        }
    }

    private void processFlashingStateMessage(JsonObject internalData) {
        String junctionInternalId = internalData.get("sName").getAsString();
        String junctionExternalId = getJunctionUUID(junctionInternalId);

        TrafficSignalRealtime.Junction.Builder junctionBuilder = TrafficSignalRealtime.Junction.newBuilder()
            .setId(junctionExternalId)
            .setOperationalStatusValue(TrafficSignalRealtime.OperationalStatus.STATUS_AMBER_FLASHING_VALUE);
        
        // Store carriageway information in the internal dataset.
        middlewareJunctionMap.put(
            junctionExternalId,
            junctionBuilder
        );
    }

    private void processOffStateMessage(JsonObject internalData) {
        String junctionInternalId = internalData.get("sName").getAsString();
        String junctionExternalId = getJunctionUUID(junctionInternalId);

        TrafficSignalRealtime.Junction.Builder junctionBuilder = TrafficSignalRealtime.Junction.newBuilder()
            .setId(junctionExternalId)
            .setOperationalStatusValue(TrafficSignalRealtime.OperationalStatus.STATUS_CLOSED_OR_OFF_VALUE);

        // Store carriageway information in the internal dataset.
        middlewareJunctionMap.put(
            junctionExternalId,
            junctionBuilder
        );
    }

    private TrafficSignalRealtime.Junction createJunctionMessageFromBaseData(String junctionExternalId) {
        TrafficSignalRealtime.Junction.Builder junctionBuilder = baseJunctionDataMap.get(junctionExternalId);

        if(junctionBuilder == null) {
            return null;
        }

        // Setting Junction level information.
        junctionBuilder.setOperationalStatusValue(
            middlewareJunctionMap.get(junctionExternalId).getOperationalStatusValue()
        );
        middlewareJunctionMap.remove(junctionExternalId);

        List<TrafficSignalRealtime.Carriageway.Builder> carriagewayList = junctionBuilder.getCarriagewaysBuilderList();
        for(int c = 0; c < carriagewayList.size(); c++) {
            TrafficSignalRealtime.Carriageway.Builder carriagewayBuilder = carriagewayList.get(c);
            
            // Setting Carriage level information.
            if(middlewareCarriagewayMap.containsKey(carriagewayBuilder.getId())) {

                // Set operational status of the carriageway.
                carriagewayBuilder.setOperationalStatusValue(
                    middlewareCarriagewayMap.get(carriagewayBuilder.getId()).getOperationalStatusValue()
                );

                // Set the carriageway vehicle density if available.
                if(middlewareCarriagewayMap.get(carriagewayBuilder.getId()).hasVehicleDensity()) {
                    carriagewayBuilder.setVehicleDensity(
                        middlewareCarriagewayMap.get(carriagewayBuilder.getId()).getVehicleDensityBuilder()
                    );
                }

                // Clear out the intermidiate data.
                middlewareCarriagewayMap.remove(carriagewayBuilder.getId());
            }
        
            List<TrafficSignalRealtime.TrafficLane.Builder> trafficLaneList = carriagewayBuilder.getTrafficLanesBuilderList();
            
            // Iterate over the lane base data to set its value on by one.
            for(int l = 0; l < trafficLaneList.size(); l++) {
                TrafficSignalRealtime.TrafficLane.Builder trafficLaneBuilder = trafficLaneList.get(l);
                
                // Need to flip the green signal (from the previous turn) to red. 
                if(!middlewareTrafficLaneMap.containsKey(trafficLaneBuilder.getId())) {

                    // Check if the signal is green.
                    if(trafficLaneBuilder.getSignalStatusValue() == TrafficSignalRealtime.TrafficLane.SignalStatus.SIGNAL_GO_VALUE) {
                        // If green then flip it to red.
                        trafficLaneBuilder.setSignalStatusValue(TrafficSignalRealtime.TrafficLane.SignalStatus.SIGNAL_STOP_VALUE);

                        // This cycle will also contain data on utilised green for previous cycle.
                        if(previousLanesUtilisedTime.containsKey(junctionExternalId)) {
                            TrafficSignalRealtime.TrafficLane.SignalTiming.Builder signalTimingBuilder = trafficLaneBuilder.getSignalTimingBuilder();
                            
                            signalTimingBuilder.setUtilisedGreenSeconds(
                                previousLanesUtilisedTime.get(junctionExternalId)
                            );

                            trafficLaneBuilder.setSignalTiming(signalTimingBuilder);
                        }

                        carriagewayBuilder.setTrafficLanes(
                            l, 
                            trafficLaneBuilder
                        );
                    }
                    
                } else {
                    // If we have the intermidiate data then set it directly to the carriageway.
                    carriagewayBuilder.setTrafficLanes(
                        l, 
                        middlewareTrafficLaneMap.get(trafficLaneBuilder.getId())
                    );
                    middlewareTrafficLaneMap.remove(trafficLaneBuilder.getId());
                }
            }

            junctionBuilder.setCarriageways(c, carriagewayBuilder);
        }

        return junctionBuilder.build();
    }

    private TrafficSignalRealtime.Junction createJunctionMessage(JsonObject internalData, String mode) {
        // Step 1: Process incoming data and store it in an intermidiate state.
        // Step 2: Use the intermidiate state and the base data to create the standardised dataset.
        switch(mode) {
            case "on":
                processOnStateMessage(internalData);
                break;
            case "flashing":
                processFlashingStateMessage(internalData);
                break;
            case "off":
                processOffStateMessage(internalData);
                break;
        }
        
        String junctionExternalId = getJunctionUUID(internalData.get("sName").getAsString());
        return createJunctionMessageFromBaseData(junctionExternalId);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicReference<JsonObject> value = new AtomicReference<>();

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        session.read(flowFile, new InputStreamCallback(){
            @Override
            public void process(InputStream in) throws IOException {
                value.set(
                    JsonParser.parseString(IOUtils.toString(in, "utf-8")).getAsJsonObject()
                );
            }
        });

        TrafficSignalRealtime.Junction junction;
        

        if(value.get().get("nStatus").getAsString().equals("1")) {
            if(value.get().get("sMode").getAsString().equals("FullVA-Split") || value.get().get("sMode").getAsString().equals("FixedTime")) {
                junction = createJunctionMessage(value.get(), "on");
            } else {
                junction = createJunctionMessage(value.get(), "flashing");
            }
        } else {
            junction = createJunctionMessage(value.get(), "off");
        }

        if(junction == null) {
            getLogger().error("Could not parse junction (" + value.get().get("sName").getAsString() + ") details.");

        }
        
        flowFile = session.putAttribute(flowFile, "Content-Type", "application/json");
        flowFile = session.putAttribute(flowFile, "mime.type", "application/json");

        TrafficSignalRealtime.FeedMessage.Builder localFeedMessageBuilder = feedMessageBuilder.clone();
        localFeedMessageBuilder.getHeaderBuilder().setTimestamp(System.currentTimeMillis() / 1000);
        TrafficSignalRealtime.FeedMessage feedMessage = localFeedMessageBuilder.addJunctions(junction).build();

        session.write(flowFile, new OutputStreamCallback(){
            @Override
            public void process(OutputStream out) throws IOException {
                String junctionJsonString = JsonFormat.printer().sortingMapKeys().print(feedMessage);
                out.write(junctionJsonString.getBytes());
                // out.write((previousLanesUtilisedTime.toString()).getBytes());
            }
        });
        session.transfer(flowFile, SUCCESS_RELATIONSHIP);

        FlowFile binaryFlowFile = session.create();
        binaryFlowFile = session.putAttribute(binaryFlowFile, "Content-Type", "application/x-binary");
        binaryFlowFile = session.putAttribute(binaryFlowFile, "mime.type", "application/x-binary");
        session.write(binaryFlowFile, new OutputStreamCallback(){
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(feedMessage.toByteArray());
            }
        });
        session.transfer(binaryFlowFile, SUCCESS_PROTO_BINARY_RELATIONSHIP);
    }
}
