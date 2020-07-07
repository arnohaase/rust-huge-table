use std::io::{Write, Read};
use std::char::decode_utf16;
use std::mem::size_of;
use std::convert::TryInto;

pub trait PersistentKey: Eq {
    fn raw_size() -> usize; //TODO variable size
    fn as_raw(&self) -> Vec<u8>;
}

pub trait PersistentValue: Copy { //TODO is Copy necessary?
    fn from_raw(raw: &[u8]) -> Self;
    fn as_raw(&self) -> Vec<u8>; //TODO 'write' is sufficient here
}

pub trait EncodePrimitives {
    fn encode_varint_u64(&mut self, value: u64);
    fn encode_varint_u32(&mut self, value: u32);
    fn encode_varint_usize(&mut self, value: usize);

    fn encode_varint_i64(&mut self, value: i64) {
        if value > 0 {
            self.encode_varint_u64((value as u64) << 1);
        }
        else {
            self.encode_varint_u64(((-value as u64) << 1) + 1);
        }
    }
    fn encode_varint_i32(&mut self, value: i32) {
        if value >= 0 {
            self.encode_varint_u32((value as u32) << 1);
        }
        else {
            self.encode_varint_u32(((-value as u32) << 1) + 1);
        }
    }

    fn encode_fixed_u64(&mut self, value: u64);
    fn encode_fixed_f64(&mut self, value: f64);
    fn encode_fixed_u32(&mut self, value: u32);
    fn encode_fixed_f32(&mut self, value: f32);

    fn encode_bool(&mut self, value: bool);
    fn encode_utf8(&mut self, value: &str);
}

impl EncodePrimitives for Vec<u8> {
    fn encode_varint_u64(&mut self, mut value: u64) {
        while (value & !0x7f) > 0 {
            self.push(((value & 0x7F) | 80) as u8);
            value >>= 7;
        }
        self.push(value as u8);
    }

    fn encode_varint_u32(&mut self, mut value: u32) {
        while (value & !0x7f) > 0 {
            self.push(((value & 0x7F) | 80) as u8);
            value >>= 7;
        }
        self.push(value as u8);
    }

    fn encode_varint_usize(&mut self, mut value: usize) {
        while (value & !0x7f) > 0 {
            self.push(((value & 0x7F) | 80) as u8);
            value >>= 7;
        }
        self.push(value as u8);
    }

    fn encode_fixed_u64(&mut self, value: u64) {
        let value_le = u64::to_le(value);
        let ptr = &value_le as *const u64 as *const u8;
        self.extend_from_slice(unsafe { std::slice::from_raw_parts(ptr, size_of::<u64>()) });
    }

    fn encode_fixed_f64(&mut self, value: f64) {
        self.extend_from_slice(&value.to_le_bytes());
    }

    fn encode_fixed_u32(&mut self, value: u32) {
        let value_le = u32::to_le(value);
        let ptr = &value_le as *const u32 as *const u8;
        self.extend_from_slice(unsafe { std::slice::from_raw_parts(ptr, size_of::<u32>()) });
    }

    fn encode_fixed_f32(&mut self, value: f32) {
        self.extend_from_slice(&value.to_le_bytes());
    }

    fn encode_bool(&mut self, value: bool) {
        if value {
            self.push(1);
        }
        else {
            self.push(0);
        }
    }

    fn encode_utf8(&mut self, value: &str) {
        let bytes = value.as_bytes();
        self.encode_varint_usize(bytes.len());
        self.extend_from_slice(bytes);
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
    fn decode_utf8(&self, offs: &mut usize) -> String;
}

impl DecodePrimitives for &[u8] {
    fn decode_varint_u64(&self, offs: &mut usize) -> u64 {
        let mut result = 0u64;

        loop {
            let next = self[*offs] as u64;
            *offs += 1;

            result <<= 7;
            result += next & 0x7F;
            //TODO check for overflow

            if next & 0x80 == 0 {
                break;
            }
        }

        result
    }

    fn decode_varint_u32(&self, offs: &mut usize) -> u32 {
        let mut result = 0u32;

        loop {
            let next = self[*offs] as u32;
            *offs += 1;

            result <<= 7;
            result += next & 0x7F;
            //TODO check for overflow

            if next & 0x80 == 0 {
                break;
            }
        }

        result
    }

    fn decode_varint_usize(&self, offs: &mut usize) -> usize {
        let mut result = 0usize;

        loop {
            let next = self[*offs] as usize;
            *offs += 1;

            result <<= 7;
            result += next & 0x7F;
            //TODO check for overflow

            if next & 0x80 == 0 {
                break;
            }
        }

        result
    }

    fn decode_fixed_u64(&self, offs: &mut usize) -> u64 {
        let (buf, _) = self[*offs..].split_at(size_of::<u64>());
        u64::from_le_bytes(buf.try_into().unwrap())
    }

    fn decode_fixed_f64(&self, offs: &mut usize) -> f64 {
        let (buf, _) = self[*offs..].split_at(size_of::<f64>());
        f64::from_le_bytes(buf.try_into().unwrap())
    }

    fn decode_fixed_u32(&self, offs: &mut usize) -> u32 {
        let (buf, _) = self[*offs..].split_at(size_of::<u32>());
        u32::from_le_bytes(buf.try_into().unwrap())
    }

    fn decode_fixed_f32(&self, offs: &mut usize) -> f32 {
        let (buf, _) = self[*offs..].split_at(size_of::<f32>());
        f32::from_le_bytes(buf.try_into().unwrap())
    }

    fn decode_bool(&self, offs: &mut usize) -> bool {
        let result = self[*offs] != 0;
        *offs += 1;
        result
    }

    fn decode_utf8(&self, offs: &mut usize) -> String {
        let len = self.decode_varint_usize(offs);
        let str_buf = &self[*offs .. *offs+len];

        //TODO unchecked?
        core::str::from_utf8(str_buf).unwrap().to_string() //TODO error handling, reduce copying (?)
    }
}


