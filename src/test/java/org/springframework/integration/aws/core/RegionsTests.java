package org.springframework.integration.aws.core;

import com.amazonaws.regions.Regions;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * @author blake
 * @since 2/22/17
 */
public class RegionsTests {

    @Test
    public void testUsEast2RegionExists() {
        assertNotNull(Regions.US_EAST_2);
        assertEquals("us-east-2", Regions.US_EAST_2.getName());
    }
}
