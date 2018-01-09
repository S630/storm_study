
package com.storm.utils;

import org.apache.storm.utils.Time;

public class Utils
{

    public static void waitForSeconds( int seconds )
    {
        try
        {
            Thread.sleep( seconds * 1000 );
        }
        catch ( InterruptedException e )
        {
        }
    }

    public static void waitForMillis( long milliseconds )
    {
        try
        {
            Thread.sleep( milliseconds );
        }
        catch ( InterruptedException e )
        {
        }
    }

    public static void sleep( long millis )
    {
        try
        {
            Time.sleep( millis );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
