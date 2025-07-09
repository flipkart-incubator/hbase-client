package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.route.IntentRoute;
import com.flipkart.yak.client.pipelined.route.Route;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("java:S3740")
public class PipelinedStoreUtils {

  private PipelinedStoreUtils() {
    throw new IllegalStateException();
  }

  static List<SiteId> getSitesToWrite(Route route, MasterSlaveReplicaSet replicaSet) {
    List<SiteId> sites = new ArrayList<>();
    DataCenter currentDataCenter = route.getMyCurrentRegion();
    StoreRoute storeRoute = (StoreRoute) route;
    WriteConsistency consistency = storeRoute.getWriteConsistency();

    if (consistency.equals(WriteConsistency.LOCAL_PREFERRED)) {
      if (currentDataCenter.equals(replicaSet.getPrimarySite().dataCenter)) {
        sites.add(replicaSet.getPrimarySite());
        sites.addAll(replicaSet.getSecondarySites().stream().filter(site -> (currentDataCenter.equals(site.dataCenter)))
            .collect(Collectors.toList()));
        sites.addAll(replicaSet.getSecondarySites().stream().filter(site -> (!currentDataCenter.equals(site.dataCenter)))
            .collect(Collectors.toList()));
      } else {
        sites.addAll(replicaSet.getSecondarySites().stream().filter(site -> (currentDataCenter.equals(site.dataCenter)))
            .collect(Collectors.toList()));
        sites.add(replicaSet.getPrimarySite());
        sites.addAll(replicaSet.getSecondarySites().stream().filter(site -> (!currentDataCenter.equals(site.dataCenter)))
            .collect(Collectors.toList()));
      }
    } else if (consistency.equals(WriteConsistency.PRIMARY_PREFERRED)) {
      sites.add(replicaSet.getPrimarySite());
      sites.addAll(replicaSet.getSecondarySites());
    } else {
      sites.add(replicaSet.getPrimarySite());
    }
    return sites;
  }

  static List<SiteId> getSitesToRead(Route route, MasterSlaveReplicaSet replicaSet) {
    List<SiteId> sites = new ArrayList<>();
    DataCenter currentDataCenter = route.getMyCurrentRegion();
    StoreRoute storeRoute = (StoreRoute) route;
    ReadConsistency consistency = storeRoute.getReadConsistency();
    if (consistency.equals(ReadConsistency.LOCAL_PREFERRED)) {
      if (currentDataCenter.equals(replicaSet.getPrimarySite().dataCenter)) {
        sites.add(replicaSet.getPrimarySite());
        sites.addAll(replicaSet.getSecondarySites().stream().filter(site -> (currentDataCenter.equals(site.dataCenter)))
            .collect(Collectors.toList()));
        sites.addAll(replicaSet.getSecondarySites().stream().filter(site -> (!currentDataCenter.equals(site.dataCenter)))
            .collect(Collectors.toList()));
      } else {
        sites.addAll(replicaSet.getSecondarySites().stream().filter(site -> (currentDataCenter.equals(site.dataCenter)))
            .collect(Collectors.toList()));
        sites.add(replicaSet.getPrimarySite());
        sites.addAll(replicaSet.getSecondarySites().stream().filter(site -> (!currentDataCenter.equals(site.dataCenter)))
            .collect(Collectors.toList()));
      }
    } else if (consistency.equals(ReadConsistency.LOCAL_MANDATORY)) {
      if (currentDataCenter.equals(replicaSet.getPrimarySite().dataCenter)) {
        sites.add(replicaSet.getPrimarySite());
      }
      sites.addAll(replicaSet.getSecondarySites().stream().filter(siteId -> siteId.dataCenter.equals(currentDataCenter))
          .collect(Collectors.toList()));
    } else if (consistency.equals(ReadConsistency.PRIMARY_PREFERRED)) {
      sites.add(replicaSet.getPrimarySite());
      sites.addAll(replicaSet.getSecondarySites());
    } else {
      sites.add(replicaSet.getPrimarySite());
    }
    return sites;
  }

  @SuppressWarnings("java:S3358")
  static List<SiteId> getIntentSites(Route route, MasterSlaveReplicaSet replicaSet) {
    List<SiteId> sites = new ArrayList<>();
    DataCenter currentDataCenter = route.getMyCurrentRegion();

    IntentRoute intentRoute = (IntentRoute) route;
    IntentConsistency consistency = intentRoute.getWriteConsistency();
    replicaSet.getSecondarySites().sort((o1, o2) -> (o1.dataCenter.equals(o2.dataCenter)) ? 0 : ((o1.dataCenter.equals(currentDataCenter)) ? 1 : -1));
    if (consistency.equals(IntentConsistency.PRIMARY_MANDATORY)) {
      sites.add(replicaSet.getPrimarySite());
    }
    if (consistency.equals(IntentConsistency.PRIMARY_PREFERRED)) {
      sites.add(replicaSet.getPrimarySite());
      sites.addAll(replicaSet.getSecondarySites());
    }

    return sites;
  }
}
