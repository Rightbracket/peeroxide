//! What the system is asked for the sake of a path's MTU: to leave a socket's
//! packets whole, and how large a packet the link to a remote carries. C
//! libudx asks both, on every system.
//!
//! Here Linux is asked, through `rustix`, whose calls are safe. For the other
//! systems neither the standard library, tokio, socket2 nor rustix has a call,
//! and this crate contains no `unsafe` code, so there a socket's packets stay
//! as the system sends them.

use std::net::SocketAddr;

#[cfg(target_os = "linux")]
pub(crate) use linux::set_dont_fragment;

/// The MTU of the link `remote` is reached over, in bytes, where the system
/// is asked. C: `udx__get_link_mtu`.
pub(crate) fn link_mtu(remote: SocketAddr) -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        linux::link_mtu(remote)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = remote;
        None
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
    use std::os::fd::AsFd;

    use rustix::net::sockopt::{self, Ipv4PathMtuDiscovery, Ipv6PathMtuDiscovery};

    /// Forbid the network to split this socket's packets into fragments, and
    /// the system to act on what it has learnt of a path's MTU by itself.
    /// C: `udx__udp_set_dontfrag`.
    ///
    /// A stream finds the largest packet a path carries by sending larger and
    /// larger ones and seeing which arrive. A packet that may be fragmented
    /// always arrives, in pieces, so without this the search ends at the
    /// largest size whatever the path, and every full packet after it travels
    /// as two.
    ///
    /// An IPv6 socket may also carry IPv4, to mapped addresses, so the IPv4
    /// option is tried on it as well.
    pub(crate) fn set_dont_fragment(socket: &impl AsFd, ipv6: bool) -> io::Result<()> {
        if ipv6 {
            let _ = sockopt::set_ip_mtu_discover(socket, Ipv4PathMtuDiscovery::PROBE);
            sockopt::set_ipv6_mtu_discover(socket, Ipv6PathMtuDiscovery::PROBE)?;
        } else {
            sockopt::set_ip_mtu_discover(socket, Ipv4PathMtuDiscovery::PROBE)?;
        }
        Ok(())
    }

    /// The system knows the MTU for a socket connected to the address.
    /// Connecting a UDP socket sends nothing.
    pub(super) fn link_mtu(remote: SocketAddr) -> Option<usize> {
        let local: SocketAddr = match remote {
            SocketAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, 0).into(),
            SocketAddr::V6(_) => (Ipv6Addr::UNSPECIFIED, 0).into(),
        };
        let socket = UdpSocket::bind(local).ok()?;
        socket.connect(remote).ok()?;
        let mtu = if remote.is_ipv6() {
            sockopt::ipv6_mtu(&socket)
        } else {
            sockopt::ip_mtu(&socket)
        };
        usize::try_from(mtu.ok()?).ok().filter(|&mtu| mtu > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::link_mtu;

    #[test]
    fn the_link_to_this_machine_has_an_mtu_where_the_system_is_asked() {
        let mtu = link_mtu("127.0.0.1:9".parse().unwrap());
        if cfg!(target_os = "linux") {
            assert!(mtu.is_some_and(|mtu| mtu >= 1280), "got {mtu:?}");
        } else {
            assert_eq!(mtu, None);
        }
    }

    #[cfg(target_os = "linux")]
    mod on_linux {
        use std::io;
        use std::net::{SocketAddr, UdpSocket};

        use rustix::net::sockopt::{self, Ipv4PathMtuDiscovery, Ipv6PathMtuDiscovery};

        use super::super::{link_mtu, set_dont_fragment};

        #[test]
        fn a_socket_can_be_told_not_to_fragment() {
            let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            assert_ne!(
                sockopt::ip_mtu_discover(&socket).unwrap(),
                Ipv4PathMtuDiscovery::PROBE,
                "the system's default lets packets be fragmented"
            );

            set_dont_fragment(&socket, false).unwrap();
            assert_eq!(sockopt::ip_mtu_discover(&socket).unwrap(), Ipv4PathMtuDiscovery::PROBE);
        }

        #[test]
        fn an_ipv6_socket_can_be_told_not_to_fragment() {
            // Not every machine has IPv6.
            let Ok(socket) = UdpSocket::bind("[::1]:0") else {
                return;
            };

            set_dont_fragment(&socket, true).unwrap();
            assert_eq!(sockopt::ipv6_mtu_discover(&socket).unwrap(), Ipv6PathMtuDiscovery::PROBE);
        }

        /// The option at work: a datagram larger than the link to an outside
        /// address is refused instead of leaving in fragments. Nothing is
        /// sent, and a machine with no such link has nothing to show.
        #[test]
        fn a_datagram_larger_than_the_link_is_refused_instead_of_fragmented() {
            // An address reserved for documentation: routed like any outside
            // address, and never anyone's.
            let outside: SocketAddr = "192.0.2.1:9".parse().unwrap();
            let Some(mtu) = link_mtu(outside).filter(|&mtu| mtu <= 9000) else {
                return;
            };
            let Ok(socket) = UdpSocket::bind("0.0.0.0:0") else {
                return;
            };
            set_dont_fragment(&socket, false).unwrap();

            // One byte more than fits beside the IP and UDP headers.
            let too_large = vec![0u8; mtu - 28 + 1];
            let error = socket.send_to(&too_large, outside).expect_err("sent, so fragmented");
            assert_ne!(error.kind(), io::ErrorKind::WouldBlock, "{error}");
        }
    }
}
