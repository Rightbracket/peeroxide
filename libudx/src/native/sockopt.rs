//! The socket options C libudx sets that the standard library, tokio and
//! socket2 have no call for: "don't fragment", and the MTU of the link a
//! remote address is reached over.
//!
//! This is the only module of the crate that calls the system directly, and
//! so the only one that contains `unsafe`. Every call passes one integer that
//! outlives the call, and the size of that integer.

#![allow(unsafe_code)]

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};

#[cfg(unix)]
pub(crate) use std::os::fd::AsRawFd as AsRawSock;
#[cfg(windows)]
pub(crate) use std::os::windows::io::AsRawSocket as AsRawSock;

/// Forbid the network to split this socket's packets into fragments, and
/// the system to act on what it has learnt of a path's MTU by itself.
/// C: `udx__udp_set_dontfrag`.
///
/// A stream finds the largest packet a path carries by sending larger and
/// larger ones and seeing which arrive. A packet that may be fragmented
/// always arrives, in pieces, so without this the search ends at the largest
/// size whatever the path, and every full packet after it travels as two.
///
/// An IPv6 socket may also carry IPv4, to mapped addresses, so the IPv4
/// option is tried on it as well. Not every system takes it there.
pub(crate) fn set_dont_fragment(socket: &impl AsRawSock, ipv6: bool) -> io::Result<()> {
    if ipv6 {
        let _ = sys::set_dont_fragment_v4(socket);
        sys::set_dont_fragment_v6(socket)
    } else {
        sys::set_dont_fragment_v4(socket)
    }
}

/// The MTU of the link `remote` is reached over, in bytes, on the systems
/// that tell. C: `udx__get_link_mtu`.
///
/// The system knows it for a socket connected to the address. Connecting a
/// UDP socket sends nothing.
pub(crate) fn link_mtu(remote: SocketAddr) -> Option<usize> {
    let local: SocketAddr = match remote {
        SocketAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, 0).into(),
        SocketAddr::V6(_) => (Ipv6Addr::UNSPECIFIED, 0).into(),
    };
    let socket = UdpSocket::bind(local).ok()?;
    socket.connect(remote).ok()?;
    let mtu = sys::link_mtu(&socket, remote.is_ipv6()).ok()?;
    usize::try_from(mtu).ok().filter(|&mtu| mtu > 0)
}

#[cfg(unix)]
mod raw {
    use std::io;
    use std::os::fd::AsRawFd;

    use libc::{c_int, socklen_t};

    #[allow(dead_code)]
    pub(super) fn set_int(
        socket: &impl AsRawFd,
        level: c_int,
        name: c_int,
        value: c_int,
    ) -> io::Result<()> {
        // SAFETY: `value` lives across the call, and the length given is
        // its size.
        let rc = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                level,
                name,
                (&raw const value).cast(),
                size_of::<c_int>() as socklen_t,
            )
        };
        if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }

    #[allow(dead_code)]
    pub(super) fn get_int(socket: &impl AsRawFd, level: c_int, name: c_int) -> io::Result<c_int> {
        let mut value: c_int = 0;
        let mut len = size_of::<c_int>() as socklen_t;
        // SAFETY: `value` and `len` live across the call, and `len` holds
        // the size of `value`.
        let rc = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                level,
                name,
                (&raw mut value).cast(),
                &raw mut len,
            )
        };
        if rc == 0 { Ok(value) } else { Err(io::Error::last_os_error()) }
    }
}

#[cfg(windows)]
mod raw {
    use std::io;
    use std::os::windows::io::AsRawSocket;

    use windows_sys::Win32::Networking::WinSock::{SOCKET, getsockopt, setsockopt};

    pub(super) fn set_int(
        socket: &impl AsRawSocket,
        level: i32,
        name: i32,
        value: i32,
    ) -> io::Result<()> {
        // SAFETY: `value` lives across the call, and the length given is
        // its size.
        let rc = unsafe {
            setsockopt(
                socket.as_raw_socket() as SOCKET,
                level,
                name,
                (&raw const value).cast(),
                size_of::<i32>() as i32,
            )
        };
        if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }

    pub(super) fn get_int(socket: &impl AsRawSocket, level: i32, name: i32) -> io::Result<i32> {
        let mut value: i32 = 0;
        let mut len = size_of::<i32>() as i32;
        // SAFETY: `value` and `len` live across the call, and `len` holds
        // the size of `value`.
        let rc = unsafe {
            getsockopt(
                socket.as_raw_socket() as SOCKET,
                level,
                name,
                (&raw mut value).cast(),
                &raw mut len,
            )
        };
        if rc == 0 { Ok(value) } else { Err(io::Error::last_os_error()) }
    }
}

/// Linux and Android: the packets carry "don't fragment", and the system
/// neither fragments them nor holds them to the path MTU it has cached.
#[cfg(any(target_os = "linux", target_os = "android"))]
mod sys {
    use std::io;

    use super::{AsRawSock, raw};

    pub(super) fn set_dont_fragment_v4(socket: &impl AsRawSock) -> io::Result<()> {
        raw::set_int(socket, libc::IPPROTO_IP, libc::IP_MTU_DISCOVER, libc::IP_PMTUDISC_PROBE)
    }

    pub(super) fn set_dont_fragment_v6(socket: &impl AsRawSock) -> io::Result<()> {
        raw::set_int(
            socket,
            libc::IPPROTO_IPV6,
            libc::IPV6_MTU_DISCOVER,
            libc::IPV6_PMTUDISC_PROBE,
        )
    }

    pub(super) fn link_mtu(socket: &impl AsRawSock, ipv6: bool) -> io::Result<libc::c_int> {
        if ipv6 {
            raw::get_int(socket, libc::IPPROTO_IPV6, libc::IPV6_MTU)
        } else {
            raw::get_int(socket, libc::IPPROTO_IP, libc::IP_MTU)
        }
    }
}

/// Apple's systems and FreeBSD have the option under another name, and do
/// not tell a link's MTU this way.
#[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
mod sys {
    use std::io;

    use super::{AsRawSock, raw};

    pub(super) fn set_dont_fragment_v4(socket: &impl AsRawSock) -> io::Result<()> {
        raw::set_int(socket, libc::IPPROTO_IP, libc::IP_DONTFRAG, 1)
    }

    pub(super) fn set_dont_fragment_v6(socket: &impl AsRawSock) -> io::Result<()> {
        raw::set_int(socket, libc::IPPROTO_IPV6, libc::IPV6_DONTFRAG, 1)
    }

    pub(super) fn link_mtu(_socket: &impl AsRawSock, _ipv6: bool) -> io::Result<libc::c_int> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[cfg(windows)]
mod sys {
    use std::io;

    use windows_sys::Win32::Networking::WinSock::{
        IP_MTU, IP_MTU_DISCOVER, IP_PMTUDISC_PROBE, IPPROTO_IP, IPPROTO_IPV6, IPV6_MTU,
        IPV6_MTU_DISCOVER,
    };

    use super::{AsRawSock, raw};

    pub(super) fn set_dont_fragment_v4(socket: &impl AsRawSock) -> io::Result<()> {
        raw::set_int(socket, IPPROTO_IP, IP_MTU_DISCOVER, IP_PMTUDISC_PROBE)
    }

    pub(super) fn set_dont_fragment_v6(socket: &impl AsRawSock) -> io::Result<()> {
        raw::set_int(socket, IPPROTO_IPV6, IPV6_MTU_DISCOVER, IP_PMTUDISC_PROBE)
    }

    pub(super) fn link_mtu(socket: &impl AsRawSock, ipv6: bool) -> io::Result<i32> {
        if ipv6 {
            raw::get_int(socket, IPPROTO_IPV6, IPV6_MTU)
        } else {
            raw::get_int(socket, IPPROTO_IP, IP_MTU)
        }
    }
}

/// Elsewhere the packets stay as the system sends them.
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "freebsd",
    windows,
)))]
mod sys {
    use std::io;

    use super::AsRawSock;

    pub(super) fn set_dont_fragment_v4(_socket: &impl AsRawSock) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }

    pub(super) fn set_dont_fragment_v6(_socket: &impl AsRawSock) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }

    pub(super) fn link_mtu(_socket: &impl AsRawSock, _ipv6: bool) -> io::Result<i32> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn dont_fragment_is_set(socket: &UdpSocket) -> bool {
        raw::get_int(socket, libc::IPPROTO_IP, libc::IP_MTU_DISCOVER).unwrap()
            == libc::IP_PMTUDISC_PROBE
    }

    #[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
    fn dont_fragment_is_set(socket: &UdpSocket) -> bool {
        raw::get_int(socket, libc::IPPROTO_IP, libc::IP_DONTFRAG).unwrap() != 0
    }

    #[cfg(windows)]
    fn dont_fragment_is_set(socket: &UdpSocket) -> bool {
        use windows_sys::Win32::Networking::WinSock::{
            IP_MTU_DISCOVER, IP_PMTUDISC_PROBE, IPPROTO_IP,
        };
        raw::get_int(socket, IPPROTO_IP, IP_MTU_DISCOVER).unwrap() == IP_PMTUDISC_PROBE
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_vendor = "apple",
        target_os = "freebsd",
        windows,
    ))]
    #[test]
    fn a_socket_can_be_told_not_to_fragment() {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        assert!(!dont_fragment_is_set(&socket), "the system's default lets packets be fragmented");

        set_dont_fragment(&socket, false).unwrap();
        assert!(dont_fragment_is_set(&socket));
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_vendor = "apple",
        target_os = "freebsd",
        windows,
    ))]
    #[test]
    fn an_ipv6_socket_can_be_told_not_to_fragment() {
        // Not every machine has IPv6.
        let Ok(socket) = UdpSocket::bind("[::1]:0") else {
            return;
        };
        set_dont_fragment(&socket, true).unwrap();
    }

    #[test]
    fn the_link_to_this_machine_has_an_mtu_where_the_system_tells() {
        let mtu = link_mtu("127.0.0.1:9".parse().unwrap());
        if cfg!(any(target_os = "linux", target_os = "android")) {
            assert!(mtu.is_some_and(|mtu| mtu >= 1280), "got {mtu:?}");
        } else if let Some(mtu) = mtu {
            assert!(mtu >= 576, "got {mtu}");
        }
    }

    /// The option at work: a datagram larger than the link to an outside
    /// address is refused instead of leaving in fragments. Nothing is sent,
    /// and a machine with no such link has nothing to show.
    ///
    /// Linux refuses every such datagram as it is sent. Windows refuses them
    /// too, with error 10040, but has been seen to accept the first one to
    /// an address it had not sent to before, so the test is left to Linux.
    #[cfg(any(target_os = "linux", target_os = "android"))]
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
