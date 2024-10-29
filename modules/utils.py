#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ipaddress


def truncate_ip(ip, bits):
    """
    Truncate IP address to the given number of bits.
    """
    try:
        ip_obj = ipaddress.ip_network(ip, strict=False)  # Convert to ip_network
        return ip_obj.supernet(new_prefix=bits).network_address
    except ValueError:
        return None  # Handle invalid IPs
