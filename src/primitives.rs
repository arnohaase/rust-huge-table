use std::io::{Write};
use std::mem::size_of;
use std::convert::TryInto;
use memmap::Mmap;


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
        while (value & !0x7f) > 0 {
            self.write_all(&[((value & 0x7F) | 80) as u8])?;
            value >>= 7;
        }
        self.write_all(&[value as u8])
    }

    fn encode_varint_u32(&mut self, mut value: u32) -> std::io::Result<()> {
        while (value & !0x7f) > 0 {
            self.write_all(&[((value & 0x7F) | 80) as u8])?;
            value >>= 7;
        }
        self.write_all(&[value as u8])
    }

    fn encode_varint_usize(&mut self, mut value: usize) -> std::io::Result<()> {
        while (value & !0x7f) > 0 {
            self.write_all(&[((value & 0x7F) | 80) as u8])?;
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

impl DecodePrimitives for Mmap {
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

    fn decode_utf8(&self, offs: &mut usize) -> &str {
        let len = self.decode_varint_usize(offs);
        let str_buf = &self[*offs .. *offs+len];
        *offs += len;

        //TODO unchecked: unsafe { std::str::from_utf8_unchecked(str_buf) }
        std::str::from_utf8(str_buf).expect("invalid UTF-8 string")
    }
}

