"""One canonical billing product identity across every profile."""

def product(alias, device, resource):
    """``alias/device/resource`` lowercased: the single billing identity.

    The billing consumer matches resource keys case-insensitively and the
    durable journals already lowercase; this makes legacy HTTP paths agree
    instead of drifting per profile. It does not change quantities, prices
    or any already-written financial row.
    """
    if (not isinstance(alias, str) or not isinstance(device, str)
            or not isinstance(resource, str)
            or not alias or not device or not resource):
        raise ValueError('Invalid billing product identity')
    return (alias + '/' + device + '/' + resource).lower()
