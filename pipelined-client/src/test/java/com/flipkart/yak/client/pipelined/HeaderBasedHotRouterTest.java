package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.route.HeaderBasedHotRouter;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertTrue;

public class HeaderBasedHotRouterTest {

    HeaderBasedHotRouter headerBasedHotRouter;

    @Before
    public void setup() throws Exception {
        headerBasedHotRouter = new HeaderBasedHotRouter();
    }

    @Test
    public void testInvalidDCInXheaders() throws Exception {
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
}
