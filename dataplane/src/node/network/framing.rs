use std::io::IoSlice;

use tokio::io::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::node::packet::{MAX_FRAMED_PACKET_SIZE, Packet, PacketBuf};

const FRAME_LEN_BYTES: usize = std::mem::size_of::<u32>();

pub async fn read_packet<R>(reader: &mut R) -> Result<Packet>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; FRAME_LEN_BYTES];
    reader.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;
    if !(20..=MAX_FRAMED_PACKET_SIZE).contains(&msg_len) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid framed packet length: {}", msg_len),
        ));
    }

    let mut buf = PacketBuf::new();
    buf.prepare_uninit(msg_len);
    reader.read_exact(buf.as_mut_slice()).await?;

    Ok(Packet::new(msg_len, buf))
}

pub async fn write_packets<W>(writer: &mut W, packets: &[Packet]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if packets.is_empty() {
        return Ok(());
    }

    let lengths: Vec<[u8; FRAME_LEN_BYTES]> = packets
        .iter()
        .map(|packet| (packet.packet_size as u32).to_be_bytes())
        .collect();
    let mut io_slices: Vec<IoSlice<'_>> = Vec::with_capacity(packets.len() * 2);
    for (len_prefix, packet) in lengths.iter().zip(packets.iter()) {
        io_slices.push(IoSlice::new(len_prefix));
        io_slices.push(IoSlice::new(packet.bytes()));
    }
    let mut slices = io_slices.as_mut_slice();

    while !slices.is_empty() {
        let written_this_call = writer.write_vectored(slices).await?;
        if written_this_call == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_vectored returned 0",
            ));
        }

        IoSlice::advance_slices(&mut slices, written_this_call);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use byteorder::{BigEndian, ByteOrder};
    use tokio::io::duplex;

    use super::*;
    use crate::node::packet::LosslessTransportMeta;

    #[tokio::test]
    async fn framing_accepts_large_lossless_packet() {
        let payload = vec![0x5Au8; 16 * 1024];
        let packet = Packet::build_ipv4_tcp_packet_with_lossless_meta(
            Ipv4Addr::new(10, 0, 0, 1),
            45000,
            Ipv4Addr::new(10, 0, 0, 2),
            46000,
            Some(LosslessTransportMeta {
                session_id: 0xA55A_A55A,
                tree_id: Some(7),
            }),
            &payload,
        );

        let (mut left, mut right) = duplex(64 * 1024);
        let expected = packet.bytes().to_vec();
        let writer = tokio::spawn(async move {
            write_packets(&mut left, &[packet])
                .await
                .expect("write should succeed");
        });

        let packet = read_packet(&mut right)
            .await
            .expect("reader should accept a large lossless packet");
        assert_eq!(packet.bytes(), expected.as_slice());
        assert_eq!(
            packet.tcp_payload().expect("packet should carry payload"),
            payload.as_slice()
        );

        writer.await.expect("writer task should finish");
    }

    #[tokio::test]
    async fn framing_preserves_packet_boundaries_even_if_inner_ipv4_length_is_wrong() {
        let first = Packet::build_ipv4_tcp_packet_with_lossless_meta(
            Ipv4Addr::new(10, 0, 0, 1),
            47000,
            Ipv4Addr::new(10, 0, 0, 2),
            48000,
            Some(LosslessTransportMeta {
                session_id: 0x1111_2222_3333_4444,
                tree_id: Some(9),
            }),
            &[0xAB; 512],
        );
        let mut first_bytes = first.bytes().to_vec();
        let shortened_len = first_bytes.len() - 40;
        BigEndian::write_u16(&mut first_bytes[2..4], shortened_len as u16);
        let first = Packet::from_vec(first_bytes.clone());

        let second = Packet::build_ipv4_tcp_packet_with_lossless_meta(
            Ipv4Addr::new(10, 0, 0, 1),
            47001,
            Ipv4Addr::new(10, 0, 0, 2),
            48001,
            Some(LosslessTransportMeta {
                session_id: 0x5555_6666_7777_8888,
                tree_id: Some(11),
            }),
            &[0xCD; 64],
        );
        let second_bytes = second.bytes().to_vec();

        let (mut left, mut right) = duplex(64 * 1024);
        let writer = tokio::spawn(async move {
            write_packets(&mut left, &[first, second])
                .await
                .expect("writer should send both packets");
        });

        let first_read = read_packet(&mut right)
            .await
            .expect("reader should return the first packet");
        assert_eq!(
            first_read.bytes(),
            first_bytes.as_slice(),
            "outer transport framing must not trust the inner IPv4 total length"
        );

        let second_read = read_packet(&mut right)
            .await
            .expect("reader should return the second packet");
        assert_eq!(second_read.bytes(), second_bytes.as_slice());

        writer.await.expect("writer task should finish");
    }
}
