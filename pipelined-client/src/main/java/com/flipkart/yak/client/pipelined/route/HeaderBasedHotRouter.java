package com.flipkart.yak.client.pipelined.route;

import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.models.Constants;
import com.flipkart.yak.client.pipelined.models.MasterSlaveReplicaSet;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.models.Region;
import io.opentelemetry.api.baggage.Baggage;
import org.apache.commons.lang3.EnumUtils;

import java.util.Map;
import java.util.Optional;

public class HeaderBasedHotRouter implements HotRouter<MasterSlaveReplicaSet, Map<DataCenter, MasterSlaveReplicaSet>> {

    public HeaderBasedHotRouter() {}

    @Override
    public MasterSlaveReplicaSet getReplicaSet(Optional<Map<DataCenter, MasterSlaveReplicaSet>> replicaSetMapOptional) {

        if (replicaSetMapOptional.isPresent()) {
            Map<DataCenter, MasterSlaveReplicaSet> replicasetMap = replicaSetMapOptional.get();
            Baggage baggage = Baggage.current();
            String dataCenterString = baggage.getEntryValue(Constants.X_PINNED_DC);

            if(dataCenterString != null && EnumUtils.isValidEnum(Region.class, dataCenterString)
                    && replicasetMap.containsKey(Region.valueOf(dataCenterString))) {
                return replicasetMap.get(Region.valueOf(dataCenterString));
            } else {
                throw new NoSiteAvailableToHandleException("Invalid dc value in x-headers " + dataCenterString);
            }
        }
        throw new NoSiteAvailableToHandleException("Provided empty ReplicaSet config to route traffic");
    }
}

