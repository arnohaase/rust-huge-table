use std::io::{Write};
use std::mem::size_of;
use std::convert::TryInto;
use memmap::Mmap;
use std::ops::{Deref, Index};
use std::slice::SliceIndex;


pub trait EncodePrimitives {
    fn encode_varint_u64(&mut self, value: u64) -> std::io::Result<()>;
    fn encode_varint_u32(&mut self, value: u32) -> std::io::Result<()>;
    fn encode_varint_usize(&mut self, value: usize) -> std::io::Result<()>;

    fn encode_varint_i64(&mut self, value: i64) -> std::io::Result<()> {
        if value > 0 {
            self.encode_varint_u64((value as u64) << 1)
        }
        else {
            self.encode_varint_u64(((-value as u64) << 1) + 1)
        }
    }
    fn encode_varint_i32(&mut self, value: i32) -> std::io::Result<()> {
        if value >= 0 {
            self.encode_varint_u32((value as u32) << 1)
        }
        else {
            self.encode_varint_u32(((-value as u32) << 1) + 1)
        }
    }

    fn encode_fixed_u64(&mut self, value: u64) -> std::io::Result<()>;
    fn encode_fixed_f64(&mut self, value: f64) -> std::io::Result<()>;
    fn encode_fixed_u32(&mut self, value: u32) -> std::io::Result<()>;
    fn encode_fixed_f32(&mut self, value: f32) -> std::io::Result<()>;

    fn encode_bool(&mut self, value: bool) -> std::io::Result<()>;
    fn encode_utf8(&mut self, value: &str) -> std::io::Result<()>;
}

impl <W> EncodePrimitives for W where W: Write {
    fn encode_varint_u64(&mut self, mut value: u64) -> std::io::Result<()> {
        while value >= 0x80 {
            self.write_all(&[((value & 0x7F) | 0x80) as u8])?;
            value >>= 7;
        }
        self.write_all(&[value as u8])
    }

    fn encode_varint_u32(&mut self, mut value: u32) -> std::io::Result<()> {
        while value >= 0x80 {
            self.write_all(&[((value & 0x7F) | 0x80) as u8])?;
            value >>= 7;
        }
        self.write_all(&[value as u8])
    }

    fn encode_varint_usize(&mut self, mut value: usize) -> std::io::Result<()> {
        while value >= 0x80 {
            self.write_all(&[((value & 0x7F) | 0x80) as u8])?;
            value >>= 7;
        }
        self.write_all(&[value as u8])
    }

    fn encode_fixed_u64(&mut self, value: u64) -> std::io::Result<()> {
        let value_le = u64::to_le(value);
        let ptr = &value_le as *const u64 as *const u8;
        self.write_all(unsafe { std::slice::from_raw_parts(ptr, size_of::<u64>()) })
    }

    fn encode_fixed_f64(&mut self, value: f64) -> std::io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    fn encode_fixed_u32(&mut self, value: u32) -> std::io::Result<()> {
        let value_le = u32::to_le(value);
        let ptr = &value_le as *const u32 as *const u8;
        self.write_all(unsafe { std::slice::from_raw_parts(ptr, size_of::<u32>()) })
    }

    fn encode_fixed_f32(&mut self, value: f32) -> std::io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }

    fn encode_bool(&mut self, value: bool) -> std::io::Result<()> {
        self.encode_varint_u32(if value {1} else {0})
    }

    fn encode_utf8(&mut self, value: &str) -> std::io::Result<()> {
        let bytes = value.as_bytes();
        self.encode_varint_usize(bytes.len())?;
        self.write_all(bytes.as_ref())
    }
}


pub trait DecodePrimitives {
    fn decode_varint_u64(&self, offs: &mut usize) -> u64;
    fn decode_varint_u32(&self, offs: &mut usize) -> u32;
    fn decode_varint_usize(&self, offs: &mut usize) -> usize;

    fn decode_varint_i64(&self, offs: &mut usize) -> i64 {
        let raw = self.decode_varint_u64(offs);
        if (raw&1) == 0 {
            (raw >> 1) as i64
        }
        else {
            -((raw >> 1) as i64)
        }
    }

    fn decode_varint_i32(&self, offs: &mut usize) -> i32 {
        let raw = self.decode_varint_u32(offs);
        if (raw&1) == 0 {
            (raw >> 1) as i32
        }
        else {
            -((raw >> 1) as i32)
        }
    }

    fn decode_fixed_u64(&self, offs: &mut usize) -> u64;
    fn decode_fixed_f64(&self, offs: &mut usize) -> f64;
    fn decode_fixed_u32(&self, offs: &mut usize) -> u32;
    fn decode_fixed_f32(&self, offs: &mut usize) -> f32;

    fn decode_bool(&self, offs: &mut usize) -> bool;
    fn decode_utf8(&self, offs: &mut usize) -> &str;
}


impl <D> DecodePrimitives for D where D: Deref<Target=[u8]> {
    //TODO fn check_capacity(&self, )

    fn decode_varint_u64(&self, offs: &mut usize) -> u64 {
        let mut result = 0u64;
        let mut shift = 0u64;

        loop {
            let next = self[*offs] as u64;
            *offs += 1;

            result += (next & 0x7F) << shift;
            shift += 7;
            //TODO check for overflow

            if next & 0x80 == 0 {
                break;
            }
        }

        result
    }

    fn decode_varint_u32(&self, offs: &mut usize) -> u32 {
        let mut result = 0u32;
        let mut shift = 0u32;

        loop {
            let next = self[*offs] as u32;
            *offs += 1;

            result += (next & 0x7F) << shift;
            shift += 7;
            //TODO check for overflow

            if next & 0x80 == 0 {
                break;
            }
        }

        result
    }

    fn decode_varint_usize(&self, offs: &mut usize) -> usize {
        let mut result = 0usize;
        let mut shift = 0usize;

        loop {
            let next = self[*offs] as usize;
            *offs += 1;

            result += (next & 0x7F) << shift;
            shift += 7;
            //TODO check for overflow

            if next & 0x80 == 0 {
                break;
            }
        }

        result
    }

    fn decode_fixed_u64(&self, offs: &mut usize) -> u64 {
        let (buf, _) = self[*offs..].split_at(size_of::<u64>());
        *offs += size_of::<u64>();
        u64::from_le_bytes(buf.try_into().unwrap())
    }

    fn decode_fixed_f64(&self, offs: &mut usize) -> f64 {
        let (buf, _) = self[*offs..].split_at(size_of::<f64>());
        *offs += size_of::<f64>();
        f64::from_le_bytes(buf.try_into().unwrap())
    }

    fn decode_fixed_u32(&self, offs: &mut usize) -> u32 {
        let (buf, _) = self[*offs..].split_at(size_of::<u32>());
        *offs += size_of::<u32>();
        u32::from_le_bytes(buf.try_into().unwrap())
    }

    fn decode_fixed_f32(&self, offs: &mut usize) -> f32 {
        let (buf, _) = self[*offs..].split_at(size_of::<f32>());
        *offs += size_of::<f32>();
        f32::from_le_bytes(buf.try_into().unwrap())
    }

    fn decode_bool(&self, offs: &mut usize) -> bool {
        let result = self[*offs] != 0;
        *offs += 1;
        result
    }

    fn decode_utf8(&self, offs: &mut usize) -> &str {
        let len = self.decode_varint_usize(offs);
        let str_buf = &self[*offs .. *offs+len];
        *offs += len;

        //TODO unchecked: unsafe { std::str::from_utf8_unchecked(str_buf) }
        std::str::from_utf8(str_buf).expect("invalid UTF-8 string")
    }
}

#[cfg(test)]
mod test {
    use crate::primitives::{EncodePrimitives, DecodePrimitives};

    #[test]
    pub fn test_bool() {
        let mut v = Vec::new();

        v.encode_bool(true).unwrap();
        v.encode_bool(false).unwrap();
        v.encode_bool(true).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(true, v.decode_bool(&mut offs));
        assert_eq!(false, v.decode_bool(&mut offs));
        assert_eq!(true, v.decode_bool(&mut offs));
    }

    #[test]
    pub fn test_utf8() {
        let mut v = Vec::new();

        v.encode_utf8("abc").unwrap();
        v.encode_utf8("abcäöü-yo").unwrap();
        v.encode_utf8("").unwrap();
        v.encode_utf8("hey").unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!("abc", v.decode_utf8(&mut offs));
        assert_eq!("abcäöü-yo", v.decode_utf8(&mut offs));
        assert_eq!("", v.decode_utf8(&mut offs));
        assert_eq!("hey", v.decode_utf8(&mut offs));
    }
    
    #[test]
    pub fn test_fixed_u32() {
        let mut v = Vec::new();

        v.encode_fixed_u32(0).unwrap();
        v.encode_fixed_u32(1).unwrap();
        v.encode_fixed_u32(127).unwrap();
        v.encode_fixed_u32(128).unwrap();
        v.encode_fixed_u32(9988).unwrap();
        v.encode_fixed_u32(1234567890).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(0, v.decode_fixed_u32(&mut offs));
        assert_eq!(1, v.decode_fixed_u32(&mut offs));
        assert_eq!(127, v.decode_fixed_u32(&mut offs));
        assert_eq!(128, v.decode_fixed_u32(&mut offs));
        assert_eq!(9988, v.decode_fixed_u32(&mut offs));
        assert_eq!(1234567890, v.decode_fixed_u32(&mut offs));
    }

    #[test]
    pub fn test_fixed_u64() {
        let mut v = Vec::new();

        v.encode_fixed_u64(0).unwrap();
        v.encode_fixed_u64(1).unwrap();
        v.encode_fixed_u64(127).unwrap();
        v.encode_fixed_u64(128).unwrap();
        v.encode_fixed_u64(9988).unwrap();
        v.encode_fixed_u64(1234567890).unwrap();
        v.encode_fixed_u64(0x1234565432101234).unwrap();
        v.encode_fixed_u64(0xffffffffffffffff).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(0, v.decode_fixed_u64(&mut offs));
        assert_eq!(1, v.decode_fixed_u64(&mut offs));
        assert_eq!(127, v.decode_fixed_u64(&mut offs));
        assert_eq!(128, v.decode_fixed_u64(&mut offs));
        assert_eq!(9988, v.decode_fixed_u64(&mut offs));
        assert_eq!(1234567890, v.decode_fixed_u64(&mut offs));
        assert_eq!(0x1234565432101234, v.decode_fixed_u64(&mut offs));
        assert_eq!(0xffffffffffffffff, v.decode_fixed_u64(&mut offs));
    }

    #[test]
    pub fn test_fixed_f32() {
        let mut v = Vec::new();

        v.encode_fixed_f32(0.).unwrap();
        v.encode_fixed_f32(1.).unwrap();
        v.encode_fixed_f32(-2.34).unwrap();
        v.encode_fixed_f32(987.654e29).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(0., v.decode_fixed_f32(&mut offs));
        assert_eq!(1., v.decode_fixed_f32(&mut offs));
        assert_eq!(-2.34, v.decode_fixed_f32(&mut offs));
        assert_eq!(987.654e29, v.decode_fixed_f32(&mut offs));
    }

    #[test]
    pub fn test_fixed_f64() {
        let mut v = Vec::new();

        v.encode_fixed_f64(0.).unwrap();
        v.encode_fixed_f64(1.).unwrap();
        v.encode_fixed_f64(-2.34).unwrap();
        v.encode_fixed_f64(987.654e29).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(0., v.decode_fixed_f64(&mut offs));
        assert_eq!(1., v.decode_fixed_f64(&mut offs));
        assert_eq!(-2.34, v.decode_fixed_f64(&mut offs));
        assert_eq!(987.654e29, v.decode_fixed_f64(&mut offs));
    }

    #[test]
    pub fn test_varint_u32() {
        let mut v = Vec::new();

        v.encode_varint_u32(0).unwrap();
        v.encode_varint_u32(1).unwrap();
        v.encode_varint_u32(127).unwrap();
        v.encode_varint_u32(128).unwrap();
        v.encode_varint_u32(9988).unwrap();
        v.encode_varint_u32(1234567890).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(0, v.decode_varint_u32(&mut offs));
        assert_eq!(1, v.decode_varint_u32(&mut offs));
        assert_eq!(127, v.decode_varint_u32(&mut offs));
        assert_eq!(128, v.decode_varint_u32(&mut offs));
        assert_eq!(9988, v.decode_varint_u32(&mut offs));
        assert_eq!(1234567890, v.decode_varint_u32(&mut offs));
    }

    #[test]
    pub fn test_varint_u64() {
        let mut v = Vec::new();

        v.encode_varint_u64(0).unwrap();
        v.encode_varint_u64(1).unwrap();
        v.encode_varint_u64(127).unwrap();
        v.encode_varint_u64(128).unwrap();
        v.encode_varint_u64(9988).unwrap();
        v.encode_varint_u64(1234567890).unwrap();
        v.encode_varint_u64(0x1234565432101234).unwrap();
        v.encode_varint_u64(0xffffffffffffffff).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(0, v.decode_varint_u64(&mut offs));
        assert_eq!(1, v.decode_varint_u64(&mut offs));
        assert_eq!(127, v.decode_varint_u64(&mut offs));
        assert_eq!(128, v.decode_varint_u64(&mut offs));
        assert_eq!(9988, v.decode_varint_u64(&mut offs));
        assert_eq!(1234567890, v.decode_varint_u64(&mut offs));
        assert_eq!(0x1234565432101234, v.decode_varint_u64(&mut offs));
        assert_eq!(0xffffffffffffffff, v.decode_varint_u64(&mut offs));
    }

    #[test]
    pub fn test_varint_usize() {
        let mut v = Vec::new();

        v.encode_varint_usize(0).unwrap();
        v.encode_varint_usize(1).unwrap();
        v.encode_varint_usize(127).unwrap();
        v.encode_varint_usize(128).unwrap();
        v.encode_varint_usize(9988).unwrap();
        v.encode_varint_usize(1234567890).unwrap();
        v.encode_varint_usize(0x1234565432101234).unwrap();
        v.encode_varint_usize(0xffffffffffffffff).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(0, v.decode_varint_usize(&mut offs));
        assert_eq!(1, v.decode_varint_usize(&mut offs));
        assert_eq!(127, v.decode_varint_usize(&mut offs));
        assert_eq!(128, v.decode_varint_usize(&mut offs));
        assert_eq!(9988, v.decode_varint_usize(&mut offs));
        assert_eq!(1234567890, v.decode_varint_usize(&mut offs));
        assert_eq!(0x1234565432101234, v.decode_varint_usize(&mut offs));
        assert_eq!(0xffffffffffffffff, v.decode_varint_usize(&mut offs));
    }

    #[test]
    pub fn test_varint_i32() {
        let mut v = Vec::new();

        v.encode_varint_i32(0).unwrap();
        v.encode_varint_i32(1).unwrap();
        v.encode_varint_i32(-1).unwrap();
        v.encode_varint_i32(9988).unwrap();
        v.encode_varint_i32(1234567890).unwrap();
        v.encode_varint_i32(-1234567890).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(0, v.decode_varint_i32(&mut offs));
        assert_eq!(1, v.decode_varint_i32(&mut offs));
        assert_eq!(-1, v.decode_varint_i32(&mut offs));
        assert_eq!(9988, v.decode_varint_i32(&mut offs));
        assert_eq!(1234567890, v.decode_varint_i32(&mut offs));
        assert_eq!(-1234567890, v.decode_varint_i32(&mut offs));
    }

    #[test]
    pub fn test_varint_i64() {
        let mut v = Vec::new();

        v.encode_varint_i64(0).unwrap();
        v.encode_varint_i64(1).unwrap();
        v.encode_varint_i64(-1).unwrap();
        v.encode_varint_i64(9988).unwrap();
        v.encode_varint_i64(1234567890).unwrap();
        v.encode_varint_i64(-1234567890).unwrap();
        v.encode_varint_i64(0x7fffffffffffffff).unwrap();
        v.encode_varint_i64(-0x7fffffffffffffff).unwrap();

        let v = v;
        let mut offs = 0usize;

        assert_eq!(0, v.decode_varint_i64(&mut offs));
        assert_eq!(1, v.decode_varint_i64(&mut offs));
        assert_eq!(-1, v.decode_varint_i64(&mut offs));
        assert_eq!(9988, v.decode_varint_i64(&mut offs));
        assert_eq!(1234567890, v.decode_varint_i64(&mut offs));
        assert_eq!(-1234567890, v.decode_varint_i64(&mut offs));
        assert_eq!(0x7fffffffffffffff, v.decode_varint_i64(&mut offs));
        assert_eq!(-0x7fffffffffffffff, v.decode_varint_i64(&mut offs));
    }
}

