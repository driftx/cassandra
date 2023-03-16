package org.apache.cassandra.locator;

import org.apache.cassandra.utils.Pair;

import java.util.UUID;

public class InetAddressAndPortAndUUID extends Pair<InetAddressAndPort, UUID>
{
    protected InetAddressAndPortAndUUID(InetAddressAndPort inetAddressAndPort, UUID uuid)
    {
        super(inetAddressAndPort, uuid);
    }

    public InetAddressAndPort getInetAddressAndPort()
    {
        return left;
    }

    public UUID getUUID()
    {
        return right;
    }

    public static InetAddressAndPortAndUUID create(InetAddressAndPort inetAddressAndPort, UUID uuid)
    {
        return new InetAddressAndPortAndUUID(inetAddressAndPort, uuid);
    }
}
