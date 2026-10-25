//! Pure-Rust stdlib RFC 3174 SHA-1 digest engine (zero external dependencies).

/// Streaming SHA-1 context for incremental hashing.
#[derive(Debug, Clone)]
pub struct Sha1 {
    h: [u32; 5],
    total_bytes: u64,
    buffer: [u8; 64],
    buffer_len: usize,
}

impl Default for Sha1 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha1 {
    /// Creates a new SHA-1 hasher initialized to standard IV constants.
    pub fn new() -> Self {
        Self {
            h: [
                0x6745_2301,
                0xEFCD_AB89,
                0x98BA_DCFE,
                0x1032_5476,
                0xC3D2_E1F0,
            ],
            total_bytes: 0,
            buffer: [0u8; 64],
            buffer_len: 0,
        }
    }

    /// Feeds data bytes into the hasher.
    pub fn update(&mut self, mut data: &[u8]) {
        self.total_bytes = self.total_bytes.wrapping_add(data.len() as u64);

        if self.buffer_len > 0 {
            let to_fill = 64 - self.buffer_len;
            if data.len() >= to_fill {
                self.buffer[self.buffer_len..64].copy_from_slice(&data[..to_fill]);
                Self::process_block(&mut self.h, &self.buffer);
                self.buffer_len = 0;
                data = &data[to_fill..];
            } else {
                self.buffer[self.buffer_len..self.buffer_len + data.len()].copy_from_slice(data);
                self.buffer_len += data.len();
                return;
            }
        }

        while data.len() >= 64 {
            let block: &[u8; 64] = data[..64].try_into().unwrap();
            Self::process_block(&mut self.h, block);
            data = &data[64..];
        }

        if !data.is_empty() {
            self.buffer[..data.len()].copy_from_slice(data);
            self.buffer_len = data.len();
        }
    }

    /// Finalizes the hash calculation and returns the 20-byte digest.
    pub fn finalize(mut self) -> [u8; 20] {
        let bit_len = self.total_bytes.wrapping_mul(8);

        // Append 0x80 byte
        self.buffer[self.buffer_len] = 0x80;
        self.buffer_len += 1;

        if self.buffer_len > 56 {
            self.buffer[self.buffer_len..64].fill(0);
            Self::process_block(&mut self.h, &self.buffer);
            self.buffer.fill(0);
        } else {
            self.buffer[self.buffer_len..56].fill(0);
        }

        self.buffer[56..64].copy_from_slice(&bit_len.to_be_bytes());
        Self::process_block(&mut self.h, &self.buffer);

        let mut digest = [0u8; 20];
        for (i, word) in self.h.iter().enumerate() {
            digest[4 * i..4 * i + 4].copy_from_slice(&word.to_be_bytes());
        }
        digest
    }

    #[inline]
    fn process_block(h: &mut [u32; 5], block: &[u8; 64]) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[4 * i],
                block[4 * i + 1],
                block[4 * i + 2],
                block[4 * i + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];

        for (i, &w_i) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };

            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w_i);

            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
}

/// Computes the 20-byte SHA-1 digest of `data`.
pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hasher.finalize()
}
