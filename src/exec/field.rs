use std::io;
use std::io::Write;
use crate::ArenaStr;

pub struct FieldBigInt;
pub struct FieldFloat;
pub struct FieldDouble;
pub struct FieldChar;
pub struct FieldVarchar;

impl FieldBigInt {
    pub fn encode_sort_key<W: Write>(value: i64, wr: &mut W) -> io::Result<usize> {
        let mut bytes = value.to_be_bytes();
        bytes[0] ^= 128;
        wr.write(&bytes)
    }

    pub fn encode_inverse_sort_key<W: Write>(value: i64, wr: &mut W) -> io::Result<usize> {
        Self::encode_sort_key(-value, wr)
    }
}

impl FieldFloat {

    #[allow(overflowing_literals)]
    pub fn encode_sort_key<W: Write>(value: f32, wr: &mut W) -> io::Result<usize> {
        /* -0.0 and +0.0 compare identically, so make sure they use exactly the same bit pattern. */
        let nr = if value == 0.0 {
            0.0
        } else {
            value
        };
        /*
          Positive floats sort exactly as ints; negative floats need
          bit flipping. The bit flipping sets the upper bit to 0
          unconditionally, so put 1 in there for positive numbers
          (so they sort later for our unsigned comparison).
          NOTE: This does not sort infinities or NaN correctly.
        */
        let mut nn = i32::from_be_bytes(nr.to_be_bytes());
        nn = (nn ^ (nn >> 31)) | ((!nn) & 0x80000000);
        wr.write(&nn.to_be_bytes())
    }

    pub fn encode_inverse_sort_key<W: Write>(value: f32, wr: &mut W) -> io::Result<usize> {
        Self::encode_sort_key(-value, wr)
    }
}

impl FieldDouble {

    #[allow(overflowing_literals)]
    pub fn encode_sort_key<W: Write>(value: f64, wr: &mut W) -> io::Result<usize> {
        /* -0.0 and +0.0 compare identically, so make sure they use exactly the same bit pattern. */
        let nr = if value == 0.0 {
            0.0
        } else {
            value
        };
        let mut nn = i64::from_le_bytes(nr.to_le_bytes());
        nn = (nn ^ (nn >> 63)) | ((!nn) & 0x8000000000000000);
        wr.write(&nn.to_be_bytes())
    }

    pub fn encode_inverse_sort_key<W: Write>(value: f64, wr: &mut W) -> io::Result<usize> {
        Self::encode_sort_key(-value, wr)
    }
}

const CHAR_FILLING_BYTE: u8 = ' ' as u8;
const CHAR_FILLING_BYTES: [u8; 1] = [CHAR_FILLING_BYTE; 1];

impl FieldChar {
    pub fn encode_sort_key<W: Write>(s: &ArenaStr, n: usize, wr: &mut W) -> io::Result<usize> {
        assert!(s.len() <= n);
        let mut len = wr.write(s.as_bytes())?;
        for _ in 0..n - s.len() {
            len += wr.write(&CHAR_FILLING_BYTES)?;
        }
        Ok(len)
    }

    pub fn encode_inverse_sort_key<W: Write>(s: &ArenaStr, n: usize, wr: &mut W) -> io::Result<usize> {
        assert!(s.len() <= n);
        let buf: [u8;64] = [0;64];
        todo!()
        //Ok(n)
    }
}

impl FieldVarchar {
    const VARCHAR_SEGMENT_LEN: usize = 9;

    const VARCHAR_CMP_LESS_THAN_SPACES: u8 = 1;
    const VARCHAR_CMP_EQUAL_TO_SPACES: u8 = 2;
    const VARCHAR_CMP_GREATER_THAN_SPACES: u8 = 3;

    pub fn encode_sort_key<W: Write>(s: &ArenaStr, wr: &mut W) -> io::Result<usize> {
        let part_len = Self::VARCHAR_SEGMENT_LEN - 1;
        let n_parts = (s.len() + (part_len - 1)) / (part_len);
        let mut len = 0;
        for i in 0..n_parts {
            let part = s.bytes_part(i * part_len, (i + 1) * part_len);
            len += wr.write(part)?;
            if part.len() < part_len {
                len += Self::encode_space_filling(part_len - part.len(), CHAR_FILLING_BYTE, wr)?;
                break;
            }
            let ch = *part.last().unwrap();
            let successor = s.as_bytes()[(i + 1) * part_len];
            len += Self::encode_cmp_tag(successor, ch, wr)?;
        }
        Ok(len)
    }

    pub fn encode_inverse_sort_key<W: Write>(s: &ArenaStr, wr: &mut W) -> io::Result<usize> {
        let mut buf: [u8;Self::VARCHAR_SEGMENT_LEN] = [0;Self::VARCHAR_SEGMENT_LEN];
        let part_len = Self::VARCHAR_SEGMENT_LEN - 1;
        let n_parts = (s.len() + (part_len - 1)) / (part_len);
        let mut len = 0;
        for i in 0..n_parts {
            let part = s.bytes_part(i * part_len, (i + 1) * part_len);
            for j in 0..part.len() {
                buf[j] = 255 - part[j];
            }
            len += wr.write(&buf[..part.len()])?;
            if part.len() < part_len {
                len += Self::encode_space_filling(part_len - part.len(), 255 - CHAR_FILLING_BYTE, wr)?;
                break;
            }
            let ch = 255 - *part.last().unwrap();
            let successor = 255 - s.as_bytes()[(i + 1) * part_len];
            len += Self::encode_cmp_tag(successor, ch, wr)?;
        }
        Ok(len)
    }

    #[inline]
    fn encode_space_filling<W: Write>(n: usize, b: u8, wr: &mut W) -> io::Result<usize> {
        for _ in 0..n {
            wr.write(&[b]).unwrap();
        }
        wr.write(&[Self::VARCHAR_CMP_EQUAL_TO_SPACES])
    }

    fn encode_cmp_tag<W: Write>(successor: u8, ch: u8, wr: &mut W) -> io::Result<usize> {
        if successor > ch {
            wr.write(&[Self::VARCHAR_CMP_GREATER_THAN_SPACES])
        } else if successor < ch {
            wr.write(&[Self::VARCHAR_CMP_LESS_THAN_SPACES])
        } else {
            wr.write(&[Self::VARCHAR_CMP_EQUAL_TO_SPACES])
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Arena;
    use super::*;

    #[test]
    fn encode_varchar_key() {
        let arena = Arena::new_ref();
        let s = ArenaStr::from_arena("", &mut arena.get_mut());
        let mut buf = Vec::new();
        FieldVarchar::encode_sort_key(&s, &mut buf).unwrap();

        assert_eq!(0, buf.len());

        buf.clear();
        let s = ArenaStr::from_arena("a", &mut arena.get_mut());
        FieldVarchar::encode_sort_key(&s, &mut buf).unwrap();

        assert_eq!(9, buf.len());
        let raw: [u8; 9] = [
            97,
            32,
            32,
            32,
            32,
            32,
            32,
            32,
            2,
        ];
        assert_eq!(&raw, buf.as_slice());

        buf.clear();
        let s = ArenaStr::from_arena("中文", &mut arena.get_mut());
        FieldVarchar::encode_sort_key(&s, &mut buf).unwrap();

        assert_eq!(9, buf.len());
        let raw: [u8; 9] = [
            228,
            184,
            173,
            230,
            150,
            135,
            32,
            32,
            2,
        ];
        assert_eq!(raw, buf.as_slice());

        buf.clear();
        let s = ArenaStr::from_arena("123456789", &mut arena.get_mut());
        FieldVarchar::encode_sort_key(&s, &mut buf).unwrap();

        assert_eq!(18, buf.len());
        let raw: [u8; 18] = [
            49,
            50,
            51,
            52,
            53,
            54,
            55,
            56,
            3,
            57,
            32,
            32,
            32,
            32,
            32,
            32,
            32,
            2,
        ];
        assert_eq!(raw, buf.as_slice());
    }

    #[test]
    fn encode_sort_key() {
        let mut k1 = vec![];
        FieldBigInt::encode_sort_key(1, &mut k1).unwrap();

        let mut k2 = vec![];
        FieldBigInt::encode_sort_key(2, &mut k2).unwrap();

        assert!(k1 < k2);

        k1.clear();
        k2.clear();
        FieldBigInt::encode_sort_key(-1, &mut k1).unwrap();
        FieldBigInt::encode_sort_key(0, &mut k2).unwrap();
        assert!(k1 < k2);

        k1.clear();
        k2.clear();
        FieldFloat::encode_sort_key(-1.0, &mut k1).unwrap();
        FieldFloat::encode_sort_key(1.0, &mut k2).unwrap();
        assert!(k1 < k2);

        k1.clear();
        k2.clear();
        FieldFloat::encode_sort_key(0.01, &mut k1).unwrap();
        FieldFloat::encode_sort_key(0.001, &mut k2).unwrap();
        assert!(k1 > k2);

        k1.clear();
        k2.clear();
        FieldFloat::encode_sort_key(-0.01, &mut k1).unwrap();
        FieldFloat::encode_sort_key(-0.001, &mut k2).unwrap();
        assert!(k1 < k2);
    }

    #[test]
    fn encode_inverse_sort_key() {
        let mut k1 = vec![];
        FieldBigInt::encode_inverse_sort_key(1, &mut k1).unwrap();

        let mut k2 = vec![];
        FieldBigInt::encode_inverse_sort_key(2, &mut k2).unwrap();

        assert!(k1 > k2);

        k1.clear();
        k2.clear();

        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let aaa = ArenaStr::new("aaa", arena.get_mut());
        FieldVarchar::encode_inverse_sort_key(&aaa, &mut k1).unwrap();

        let bbb = ArenaStr::new("bbb", arena.get_mut());
        FieldVarchar::encode_inverse_sort_key(&bbb, &mut k2).unwrap();

        assert!(k1 > k2);

        k2.clear();
        let aaaa = ArenaStr::new("aaaa", arena.get_mut());
        FieldVarchar::encode_inverse_sort_key(&aaaa, &mut k2).unwrap();

        assert!(k1 > k2);

        k1.clear();
        k2.clear();
        FieldDouble::encode_inverse_sort_key(1.0, &mut k1).unwrap();
        FieldDouble::encode_inverse_sort_key(2.0, &mut k2).unwrap();

        assert!(k1 > k2);
    }
}