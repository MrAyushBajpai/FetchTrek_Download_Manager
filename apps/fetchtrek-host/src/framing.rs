use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::HostError;

pub const MAX_MESSAGE_SIZE: u32 = 1_048_576; // 1 MiB

pub async fn read_native_message<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<String, HostError> {
    let mut len_bytes = [0u8; 4];
    let mut first_byte = [0u8; 1];

    match reader.read(&mut first_byte).await? {
        0 => return Err(HostError::Eof),
        1 => {
            len_bytes[0] = first_byte[0];
            reader.read_exact(&mut len_bytes[1..4]).await?;
        }
        _ => unreachable!(),
    }

    let length = u32::from_le_bytes(len_bytes);
    if length == 0 {
        return Err(HostError::ZeroLengthMessage);
    }
    if length > MAX_MESSAGE_SIZE {
        return Err(HostError::OversizedMessage(length));
    }

    let mut payload = vec![0u8; length as usize];
    reader.read_exact(&mut payload).await?;

    let json_str = String::from_utf8(payload)?;
    Ok(json_str)
}

pub async fn write_native_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    json_str: &str,
) -> Result<(), HostError> {
    let bytes = json_str.as_bytes();
    let length = bytes.len() as u32;

    if length == 0 {
        return Err(HostError::ZeroLengthMessage);
    }
    if length > MAX_MESSAGE_SIZE {
        return Err(HostError::OversizedMessage(length));
    }

    writer.write_all(&length.to_le_bytes()).await?;
    writer.write_all(bytes).await?;
    writer.flush().await?;
    Ok(())
}
