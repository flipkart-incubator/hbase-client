package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.models.Constants;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.models.MasterSlaveReplicaSet;
import com.flipkart.yak.client.pipelined.models.Region;
import com.flipkart.yak.client.pipelined.route.HeaderBasedHotRouter;
import io.opentelemetry.api.baggage.Baggage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class) @PrepareForTest({ Baggage.class })
public class HeaderBasedHotRouterTest {

    HeaderBasedHotRouter headerBasedHotRouter;
    @Mock MasterSlaveReplicaSet replicaSet1;
    @Mock MasterSlaveReplicaSet replicaSet2;

    @Before
    public void setup() throws Exception {
        headerBasedHotRouter = new HeaderBasedHotRouter();
        PowerMockito.whenNew(MasterSlaveReplicaSet.class).withAnyArguments().thenReturn(replicaSet1);
    }

    @Test
    public void testEmptyRouteInfo() throws Exception {
        String errorMessage = "Provided empty ReplicaSet config to route traffic";
        try {
            headerBasedHotRouter.getReplicaSet(Optional.empty());
        } catch (Exception ex) {
            assertTrue("Expect NoSiteAvailableToHandleException to be thrown", ex != null);
            assertTrue("Expect NoSiteAvailableToHandleException to be thrown", ex instanceof
                    NoSiteAvailableToHandleException);
            assertTrue("Expect exception message to be: " + errorMessage, ex.getMessage().equals(errorMessage));
        }
    }

    @Test
    public void testEmptyDCInXheaders() throws Exception {
        Map<DataCenter, MasterSlaveReplicaSet> routeInfo = new HashMap<>();
        String errorMessage = "Invalid dc value in x-headers null";
        try {
            headerBasedHotRouter.getReplicaSet(Optional.of(routeInfo));
        } catch (Exception ex) {
            assertTrue("Expect NoSiteAvailableToHandleException to be thrown", ex != null);
            assertTrue("Expect NoSiteAvailableToHandleException to be thrown", ex instanceof
                    NoSiteAvailableToHandleException);
            assertTrue("Expect exception message to be: " + errorMessage, ex.getMessage().equals(errorMessage));
        }
    }

    @Test
    public void testInvalidDCInXheaders() throws Exception {
        String xPinnedDCValue = Region.REGION_1.name();
        Baggage baggage = Baggage.builder().put(Constants.X_PINNED_DC, xPinnedDCValue).build();
        PowerMockito.mockStatic(Baggage.class);
        PowerMockito.when(Baggage.current()).thenReturn(baggage);

        Map<DataCenter, MasterSlaveReplicaSet> routeInfo = new HashMap<>();
        String errorMessage = "Invalid dc value in x-headers " + xPinnedDCValue;
        try {
            headerBasedHotRouter.getReplicaSet(Optional.of(routeInfo));
        } catch (Exception ex) {
            assertTrue("Expect NoSiteAvailableToHandleException to be thrown", ex != null);
            assertTrue("Expect NoSiteAvailableToHandleException to be thrown", ex instanceof
                    NoSiteAvailableToHandleException);
            assertTrue("Expect exception message to be: " + errorMessage, ex.getMessage().equals(errorMessage));
        }
    }

    @Test
    public void testValidDCInXheaders() throws Exception {
        DataCenter xPinnedDCValue = Region.REGION_1;
        Baggage baggage = Baggage.builder().put(Constants.X_PINNED_DC, xPinnedDCValue.getName()).build();
        PowerMockito.mockStatic(Baggage.class);
        PowerMockito.when(Baggage.current()).thenReturn(baggage);

        Map<DataCenter, MasterSlaveReplicaSet> routeInfo = new HashMap<>();
        routeInfo.put(xPinnedDCValue, replicaSet1);
        try {
            MasterSlaveReplicaSet replicaSet = headerBasedHotRouter.getReplicaSet(Optional.of(routeInfo));
            assertTrue(replicaSet.equals(replicaSet1));
        } catch (Exception ex) {
            assertTrue("Expect NoSiteAvailableToHandleException not to be thrown", ex == null);
        }
    }

    @Test
    public void testMultipleValidDCInXheaders() throws Exception {
        Baggage baggage1 = Baggage.builder().put(Constants.X_PINNED_DC, Region.REGION_1.getName()).build();
        Baggage baggage2 = Baggage.builder().put(Constants.X_PINNED_DC, Region.REGION_2.getName()).build();
        PowerMockito.mockStatic(Baggage.class);
        PowerMockito.when(Baggage.current()).thenReturn(baggage1);

        Map<DataCenter, MasterSlaveReplicaSet> routeInfo = new HashMap<>();
        routeInfo.put(Region.REGION_1, replicaSet1);
        try {
            MasterSlaveReplicaSet replicaSet = headerBasedHotRouter.getReplicaSet(Optional.of(routeInfo));
            assertTrue(replicaSet1 + " is expected by returned " + replicaSet, replicaSet.equals(replicaSet1));
        } catch (Exception ex) {
            assertTrue("Expect NoSiteAvailableToHandleException not to be thrown", ex == null);
        }

        PowerMockito.when(Baggage.current()).thenReturn(baggage2);

        routeInfo.put(Region.REGION_2, replicaSet2);
        try {
            MasterSlaveReplicaSet replicaSet = headerBasedHotRouter.getReplicaSet(Optional.of(routeInfo));
            assertTrue(replicaSet2 + " is expected by returned " + replicaSet, replicaSet.equals(replicaSet2));
        } catch (Exception ex) {
            assertTrue("Expect NoSiteAvailableToHandleException not to be thrown", ex == null);
        }
    }
}
