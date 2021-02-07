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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import in.gov.mohua.ds.transit.TrafficSignalRealtime.TrafficLane;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.Carriageway.VehicleDensity;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.TrafficLane.SignalTiming;

@Tags({ "atcs", "json" })
@CapabilityDescription("Convert ATCS data (JSON) to ATCS standard.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class ATCSDataProcessing extends AbstractProcessor {

    Map<String, String> internalToExternalIdMap = null;
    Map<String, JsonArray> junctionStageToLanes = null;

    public static final PropertyDescriptor MAPPING_FILE_PROPERTY = new PropertyDescriptor.Builder()
            .name("MAPPING_FILE_PROPERTY").displayName("Mapping file path")
            .description("Internal ID to external ID mapping file").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder().name("success")
            .description("standardised data").build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder().name("failure")
            .description("failed to standardise").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MAPPING_FILE_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAILURE_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);

        internalToExternalIdMap = new HashMap<String, String>();
        junctionStageToLanes = new HashMap<String, JsonArray>();
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

        if (descriptor == MAPPING_FILE_PROPERTY) {
            if (newValue == null || newValue.isEmpty()) {
                return;
            }

            getLogger().info("Modifying map for ids");

            if (!newValue.equals(oldValue)) {
                try {
                    JsonElement element = JsonParser.parseReader(new FileReader(newValue));
                    JsonObject globalMapObj = element.getAsJsonObject();

                    JsonArray junctionArray = globalMapObj.get("junctions").getAsJsonArray();

                    for(int i = 0; i < junctionArray.size(); i++) {
                        JsonObject junctionElement = junctionArray.get(i).getAsJsonObject();
                        String junctionInternalId = junctionElement.get("internalId").getAsString();

                        internalToExternalIdMap.put(
                            junctionInternalId, 
                            junctionElement.get("externalId").getAsString()
                        );

                        JsonArray wayArray = junctionElement.get("ways").getAsJsonArray();

                        for(int j = 0; j < wayArray.size(); j++) {
                            JsonObject wayElement = wayArray.get(j).getAsJsonObject();
                            String wayInternalId = wayElement.get("internalId").getAsString();

                            internalToExternalIdMap.put(
                                junctionInternalId + wayInternalId,
                                wayElement.get("externalId").getAsString()
                            );

                            JsonArray laneArray = wayElement.get("lanes").getAsJsonArray();

                            junctionStageToLanes.put(junctionInternalId + wayInternalId, laneArray);

                            for(int k = 0; k < laneArray.size(); k++) {
                                JsonObject laneElement = laneArray.get(k).getAsJsonObject();
                                String laneInternalId = laneElement.get("internalId").getAsString();

                                internalToExternalIdMap.put(
                                    junctionInternalId + wayInternalId + laneInternalId,
                                    laneElement.get("externalId").getAsString()
                                );
                            }
                        }
                    }

                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private TrafficSignalRealtime.Junction createOnStateMessage(JsonObject internalData) {
        String junctionId = internalData.get("sName").getAsString();

        TrafficSignalRealtime.Junction.Builder junctionBuilder = TrafficSignalRealtime.Junction.newBuilder()
            .setId(internalToExternalIdMap.get(junctionId))
            .setOperationalStatusValue(TrafficSignalRealtime.OperationalStatus.STATUS_NORMAL_OPERATION_VALUE);
        
        JsonArray stageArray = internalData.get("alLinkedStageJSON").getAsJsonArray();
        String activeStageNumber = internalData.get("nCurrentStageNo").getAsString();

        for(int s = 0; s < stageArray.size(); s++) {
            JsonObject stageObject = stageArray.get(s).getAsJsonObject();

            String stageNo = stageObject.get("nStageno").getAsString();

            TrafficSignalRealtime.Carriageway.Builder carriagewayBuilder = TrafficSignalRealtime.Carriageway.newBuilder()
                .setId(internalToExternalIdMap.get(junctionId + stageNo))
                .setOperationalStatusValue(TrafficSignalRealtime.OperationalStatus.STATUS_NORMAL_OPERATION_VALUE);
            
            if(stageObject.get("nVehicleCount").getAsInt() > -1) {
                carriagewayBuilder.setVehicleDensity(VehicleDensity.newBuilder().setVehicleCountPerHour(
                    stageObject.get("nVehicleCount").getAsInt()
                ));
            }

            if(stageNo.equals(activeStageNumber)) {
                JsonArray trafficLaneArray = internalData.get("alLinkedPhaseJSON").getAsJsonArray();

                carriagewayBuilder.setVehicleDensity(VehicleDensity.newBuilder().setVehicleCountPerHour(
                    1999
                ));

                for(int l = 0; l < trafficLaneArray.size(); l++) {
                    String phaseNo = trafficLaneArray.get(l).getAsJsonObject().get("nPhaseNo").getAsString();

                    TrafficLane.Builder trafficLaneBuider = TrafficLane.newBuilder()
                        .setId(internalToExternalIdMap.get(junctionId + stageNo + phaseNo))
                        .setSignalStatusValue(TrafficLane.SignalStatus.SIGNAL_GO_VALUE)
                        .setSignalTiming(SignalTiming.newBuilder()
                            .setAllocatedGreenSeconds(stageObject.get("nAllocatedGreen").getAsInt())
                            .setAvailableGreenSeconds(stageObject.get("nAvailableGreen").getAsInt())
                        )
                        .setOperationalStatusValue(TrafficSignalRealtime.OperationalStatus.STATUS_NORMAL_OPERATION_VALUE);
                    
                    carriagewayBuilder.addTrafficLanes(trafficLaneBuider.build());
                }
                
            } else {
                JsonArray trafficLaneArray = junctionStageToLanes.get(junctionId + stageNo);

                for(int l = 0; l < trafficLaneArray.size(); l++) {
                    String externalLaneId = trafficLaneArray.get(l).getAsJsonObject().get("externalId").getAsString();

                    TrafficLane.Builder trafficLaneBuider = TrafficLane.newBuilder()
                        .setId(externalLaneId)
                        .setSignalStatusValue(TrafficLane.SignalStatus.SIGNAL_STOP_VALUE)
                        .setSignalTiming(SignalTiming.newBuilder()
                            .setAllocatedGreenSeconds(stageObject.get("nAllocatedGreen").getAsInt())
                            .setAvailableGreenSeconds(stageObject.get("nAvailableGreen").getAsInt())
                            .setUtilisedGreenSeconds(stageObject.get("nUtilizedGreen").getAsInt())
                        )
                        .setOperationalStatusValue(TrafficSignalRealtime.OperationalStatus.STATUS_NORMAL_OPERATION_VALUE);
                    
                    carriagewayBuilder.addTrafficLanes(trafficLaneBuider.build());
                }
            }

            junctionBuilder.addCarriageways(carriagewayBuilder.build());

        }

        return junctionBuilder.build();
    }

    private TrafficSignalRealtime.Junction createFlashingStateMessage(JsonObject internalData) {
        String junctionId = internalData.get("sName").getAsString();

        TrafficSignalRealtime.Junction.Builder junctionBuilder = TrafficSignalRealtime.Junction.newBuilder()
            .setId(internalToExternalIdMap.get(junctionId))
            .setOperationalStatusValue(TrafficSignalRealtime.OperationalStatus.STATUS_CLOSED_OR_OFF_VALUE);
        
        return junctionBuilder.build();
    }

    private TrafficSignalRealtime.Junction createOffStateMessage(JsonObject internalData) {
        String junctionId = internalData.get("sName").getAsString();

        TrafficSignalRealtime.Junction.Builder junctionBuilder = TrafficSignalRealtime.Junction.newBuilder()
            .setId(internalToExternalIdMap.get(junctionId))
            .setOperationalStatusValue(TrafficSignalRealtime.OperationalStatus.STATUS_CLOSED_OR_OFF_VALUE);

        return junctionBuilder.build();
    }

    private TrafficSignalRealtime.Junction createJunctionMessage(JsonObject internalData, String mode) {
        switch(mode) {
            case "on":
                return createOnStateMessage(internalData);
            case "flashing":
                return createFlashingStateMessage(internalData);
            case "off":
                return createOffStateMessage(internalData);
        }
        return null;
        
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



        // String mappingFileName = context.getProperty(MAPPING_FILE_PROPERTY).getValue();

        TrafficSignalRealtime.Junction junction;

        if(value.get().get("nStatus").getAsInt() == 1) {
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

        session.write(flowFile, new OutputStreamCallback(){
            @Override
            public void process(OutputStream out) throws IOException {
                String junctionJsonString = JsonFormat.printer().sortingMapKeys().print(junction);
                out.write(junctionJsonString.getBytes());
            }
        });

        

        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    }
}
