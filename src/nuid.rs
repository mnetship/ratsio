// Copyright 2018 Ivan Porto Carrero
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copied here due to lazy_static version collision.
extern crate rand;

use rand::thread_rng;
use rand::rngs::{OsRng};
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::sync::Mutex;

const BASE: usize = 62;
const ALPHABET: [u8; BASE as usize] = [b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9',
                                       b'A', b'B', b'C', b'D', b'E', b'F', b'G', b'H', b'I', b'J',
                                       b'K', b'L', b'M', b'N', b'O', b'P', b'Q', b'R', b'S', b'T',
                                       b'U', b'V', b'W', b'X', b'Y', b'Z', b'a', b'b', b'c', b'd',
                                       b'e', b'f', b'g', b'h', b'i', b'j', b'k', b'l', b'm', b'n',
                                       b'o', b'p', b'q', b'r', b's', b't', b'u', b'v', b'w', b'x',
                                       b'y', b'z'];

const PRE_LEN: usize = 12;
const MAX_SEQ: u64 = 839299365868340224; // (BASE ^ remaining bytes 22 - 12) == 62^10
const MIN_INC: u64 = 33;
const MAX_INC: u64 = 333;
const TOTAL_LEN: usize = 22;

lazy_static! {
    static ref GLOBAL_NUID: Mutex<NUID> = Mutex::new(NUID::new());
}

/// Generate the next `NUID` string from the global locked `NUID` instance.
pub fn next() -> String {
    GLOBAL_NUID.lock().unwrap().next()
}

/// NUID needs to be very fast to generate and truly unique, all while being entropy pool friendly.
/// We will use 12 bytes of crypto generated data (entropy draining), and 10 bytes of sequential data
/// that is started at a pseudo random number and increments with a pseudo-random increment.
/// Total is 22 bytes of base 62 ascii text :)
pub struct NUID {
    pre: [u8; PRE_LEN],
    seq: u64,
    inc: u64,

}



impl NUID {
    /// generate a new `NUID` and properly initialize the prefix, sequential start, and sequential increment.
    pub fn new() -> NUID {
        let mut rng = thread_rng();
        let seq = Rng::gen_range::<u64, u64, _>(&mut rng, 0, MAX_SEQ);
        let inc = MIN_INC + Rng::gen_range::<u64, u64, _>(&mut rng, 0, MAX_INC+MIN_INC);
        let mut n = NUID {
            pre: [0; PRE_LEN], seq, inc,
        };
        n.randomize_prefix();
        n
    }

    pub fn randomize_prefix(&mut self) {
        let mut rng = OsRng::new().expect("failed to get crypto random number generator");
        for (i, n) in rng.sample_iter(&Alphanumeric).take(PRE_LEN).enumerate() {
            self.pre[i] = ALPHABET[n as usize % BASE];
        }
    }

    /// Generate the next `NUID` string.
    pub fn next(&mut self) -> String {
        self.seq += self.inc;
        if self.seq >= MAX_SEQ {
            self.randomize_prefix();
            self.reset_sequential();
        }
        let seq: usize = self.seq as usize;

        let mut b: [u8; TOTAL_LEN] = [0; TOTAL_LEN];
        for (i, n) in self.pre.iter().enumerate() {
            b[i] = *n;
        }

        let mut l = seq;
        for i in (PRE_LEN..TOTAL_LEN).rev() {
            b[i] = ALPHABET[l%BASE];
            l = l/BASE;
        }

        // data is base62 encoded so this can't fail
        String::from_utf8(b.to_vec()).unwrap()
    }

    fn reset_sequential(&mut self) {
        let mut rng = thread_rng();
        self.seq = Rng::gen_range::<u64, _, _>(&mut rng, 0, MAX_SEQ);
        self.inc = MIN_INC + Rng::gen_range::<u64, _, _>(&mut rng, 0, MIN_INC+MAX_INC);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn alphabet_size() {
        assert_eq!(ALPHABET.len(), BASE);
    }

    #[test]
    fn global_nuid_init() {
        assert_eq!(GLOBAL_NUID.lock().unwrap().pre.len(), PRE_LEN);
        assert_ne!(GLOBAL_NUID.lock().unwrap().seq, 0);
    }

    #[test]
    fn nuid_rollover() {
        let mut n = NUID::new();
        n.seq = MAX_SEQ;
        let old = n.pre.to_vec().clone();
        n.next();
        assert_ne!(n.pre.to_vec(), old);

        let mut n = NUID::new();
        n.seq = 1;
        let old = n.pre.to_vec().clone();
        n.next();
        assert_eq!(n.pre.to_vec(), old);
    }

    #[test]
    fn nuid_len() {
        let id = next();
        assert_eq!(id.len(), TOTAL_LEN);
    }

    #[test]
    fn proper_prefix() {
        let mut min: u8 = 255;
        let mut max: u8 = 0;

        for nn in ALPHABET.iter() {
            let n = *nn;
            if n < min {
                min = n;
            }
            if n > max {
                max = n;
            }
        }

        for _ in 0..100_000 {
            let nuid = NUID::new();
            for j in 0..PRE_LEN {
                assert!(nuid.pre[j] >= min || nuid.pre[j] <= max);
            }
        }
    }

    #[test]
    fn unique() {
        let mut set = HashSet::new();
        for _ in 0..10_000_000 {
            assert_eq!(set.insert(next()), true);
        }

    }
}