use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::Frame;
use crate::{MeierError, Result};

/// 프로토콜 코덱
///
/// 프레임 형식
/// [길이: 4바이트(u32, big-endian)][데이터: JSON]
#[derive(Clone)]
pub struct MeierCodec {
    pub max_frame_length: usize,
}

impl MeierCodec {
    pub fn new() -> Self {
        Self {
            max_frame_length: 10 * 1024 * 1024, // 10MB
        }
    }

    pub fn with_max_length(max_length: usize) -> Self {
        Self {
            max_frame_length: max_length,
        }
    }
}

impl Default for MeierCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for MeierCodec {
    type Item = Frame;
    type Error = MeierError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < 4 {
            return Ok(None);
        }

        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if length > self.max_frame_length {
            return Err(MeierError::Protocol(format!(
                "Frame too large: {}, bytes (max: {})",
                length, self.max_frame_length
            )));
        }

        // 전체 프레임 수신 확인
        if src.len() < 4 + length {
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // 길이 필드 제거
        src.advance(4);

        // 데이터 추출
        let data = src.split_to(length);

        // 역직렬화
        match Frame::from_bytes(&data) {
            Ok(frame) => Ok(Some(frame)),
            Err(e) => Err(e),
        }
    }
}

impl Encoder<Frame> for MeierCodec {
    type Error = MeierError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        let data = item.to_bytes()?;
        let length = data.len();

        if length > self.max_frame_length {
            return Err(MeierError::Protocol(format!(
                "Frame too large: {} bytes (max: {})",
                length, self.max_frame_length
            )));
        }

        // 버퍼 공간 확보
        dst.reserve(4 + length);

        // 길이 필드 추가(big-endian u32)
        dst.put_u32(length as u32);

        // 데이터 추가
        dst.put_slice(&data);

        Ok(())
    }
}
