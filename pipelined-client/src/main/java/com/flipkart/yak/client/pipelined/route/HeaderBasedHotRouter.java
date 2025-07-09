package com.flipkart.yak.client.pipelined.route;

import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.models.Constants;
import com.flipkart.yak.client.pipelined.models.MasterSlaveReplicaSet;
import com.flipkart.yak.client.pipelined.models.Region;
import io.opentelemetry.api.baggage.Baggage;

import java.util.Map;
import java.util.Optional;

public class HeaderBasedHotRouter implements HotRouter<MasterSlaveReplicaSet, Map<Region, MasterSlaveReplicaSet>> {

    public HeaderBasedHotRouter() {}

    @Override
    public MasterSlaveReplicaSet getReplicaSet(Optional<Map<Region, MasterSlaveReplicaSet>> replicaSetMapOptional) {

        if (replicaSetMapOptional.isPresent()) {
            Map<Region, MasterSlaveReplicaSet> replicasetMap = replicaSetMapOptional.get();
            Baggage baggage = Baggage.current();
            String dataCenterString = baggage.getEntryValue(Constants.X_PINNED_DC);
            Region region = Region.valueOf(dataCenterString);

            if (replicasetMap.containsKey(region)) {
                return replicasetMap.get(region);
            } else {
                throw new NoSiteAvailableToHandleException("Invalid dc value in x-headers " + dataCenterString);
            }
        }
        throw new NoSiteAvailableToHandleException("Provided empty ReplicaSet config to route traffic");
    }
}
