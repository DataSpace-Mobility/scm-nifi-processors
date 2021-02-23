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
import in.gov.mohua.ds.transit.HeaderOuterClass.Header;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.FeedMessage;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.Junction;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.TrafficLane;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.Carriageway.VehicleDensity;
import in.gov.mohua.ds.transit.TrafficSignalRealtime.TrafficLane.SignalTiming;

import in.gov.mohua.utils.UUID5;

@Tags({ "atcs", "json", "junctions", "merge" })
@CapabilityDescription("Merge junctions to FeedMessage.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class MergeATCSJunctionsData extends AbstractProcessor {

    TrafficSignalRealtime.FeedMessage.Builder feedMessageBuilder = null;
    Map<String, TrafficSignalRealtime.Junction> junctionDataMap = null;

    // f048d42f-ffa2-3e9e-93e4-984f9b394d8e
    UUID namespace = UUID.nameUUIDFromBytes("in.gov.mohua.scm.fscl".getBytes());

    public static final PropertyDescriptor PROVIDER_NAME_PROPERTY = new PropertyDescriptor.Builder()
            .name("PROVIDER_NAME_PROPERTY").displayName("Provider name")
            .description("Provider name").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor VERSION_PROPERTY = new PropertyDescriptor.Builder()
            .name("VERSION_PROPERTY").displayName("Version string")
            .description("Version string").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder().name("success")
            .description("standardised data").build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder().name("failure")
            .description("failed to standardise").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROVIDER_NAME_PROPERTY);
        descriptors.add(VERSION_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAILURE_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
        
        Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setIncrementality(Header.Incrementality.DIFFERENTIAL);
        headerBuilder.setTimestamp(System.currentTimeMillis() / 1000);
        headerBuilder.setProvider(
            HeaderOuterClass.Provider.newBuilder()
                .setId(namespace.toString())
        );

        feedMessageBuilder = TrafficSignalRealtime.FeedMessage.newBuilder();
        feedMessageBuilder.setHeader(headerBuilder);

        junctionDataMap = new HashMap<String, TrafficSignalRealtime.Junction>();
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

        if (descriptor == PROVIDER_NAME_PROPERTY) {
            if (newValue == null || newValue.isEmpty()) {
                return;
            }

            feedMessageBuilder.getHeaderBuilder().getProviderBuilder().setName(newValue);

        } else if (descriptor == VERSION_PROPERTY) {
            if (newValue == null || newValue.isEmpty()) {
                return;
            }

            feedMessageBuilder.getHeaderBuilder().setVersion(newValue);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // final AtomicReference<JsonObject> value = new AtomicReference<>();

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        session.read(flowFile, new InputStreamCallback(){
            @Override
            public void process(InputStream in) throws IOException {
                TrafficSignalRealtime.FeedMessage feedMessage = TrafficSignalRealtime.FeedMessage.parseFrom(in);
                
                for(TrafficSignalRealtime.Junction junction : feedMessage.getJunctionsList()) {
                    junctionDataMap.put(junction.getId(), junction);
                }
            }
        });

        TrafficSignalRealtime.FeedMessage.Builder localFeedMessageBuilder = feedMessageBuilder.clone();
        for(Map.Entry<String, TrafficSignalRealtime.Junction> entry: junctionDataMap.entrySet()) {
            localFeedMessageBuilder.addJunctions(entry.getValue());
        }

        flowFile = session.putAttribute(flowFile, "Content-Type", "application/json");
        flowFile = session.putAttribute(flowFile, "mime.type", "application/json");

        session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                localFeedMessageBuilder.getHeaderBuilder().setTimestamp(System.currentTimeMillis() / 1000);
                String feedMessageString = JsonFormat.printer().sortingMapKeys().print(localFeedMessageBuilder);
                out.write(feedMessageString.getBytes());
            }
        });

        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    }
}
