// Shopping Sessions pipeline: mmap + hand-written CSV parsing + hash partitioning.
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hashbrown::HashMap;
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use rayon::prelude::*;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type S = &'static [u8];

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const NB: usize = 256; // partitions

#[derive(Default, Clone, Copy)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0 = xxh3_64(bytes);
    }
    // Keys are already xxh3 hashes, but partitions group keys by their top 8 bits, and
    // hashbrown takes its 7-bit tag from the top bits: rotate so tags stay distinct.
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = v.rotate_right(8);
    }
    #[inline]
    fn write_u128(&mut self, v: u128) {
        let mut h = (v as u64) ^ ((v >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= h >> 29;
        h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        self.0 = h ^ (h >> 32);
    }
}
type BH = BuildHasherDefault<IdHasher>;

macro_rules! log {
    ($t0:expr, $($arg:tt)*) => {
        eprintln!("[{:>7.3}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*));
    };
}

// ---------------------------------------------------------------- profiling (feature "prof")

const PROF: bool = cfg!(feature = "prof");
/// read, scan overhead, eid+ingest, validation, user+push, closure calls (lines)
static PROF_CY: [std::sync::atomic::AtomicU64; 6] = [const { std::sync::atomic::AtomicU64::new(0) }; 6];
const PROF_NAMES: [&str; 6] = ["pread", "scanner", "eid+ingest", "validate", "user+store", "lines"];

#[inline(always)]
fn tsc() -> u64 {
    if PROF {
        unsafe { core::arch::x86_64::_rdtsc() }
    } else {
        0
    }
}

fn prof_add(i: usize, v: u64) {
    if PROF {
        PROF_CY[i].fetch_add(v, Relaxed);
    }
}

fn prof_report() {
    if PROF {
        let v: Vec<u64> = PROF_CY.iter().map(|a| a.load(Relaxed)).collect();
        let lines = v[5].max(1);
        for i in 0..5 {
            eprintln!("prof {:<11} {:>8.1} Gcycles  {:>6.1} cycles/line", PROF_NAMES[i], v[i] as f64 / 1e9, v[i] as f64 / lines as f64);
        }
        eprintln!("prof lines {}", v[5]);
    }
}

// ---------------------------------------------------------------- timestamps

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some((a as i64) * 10 + c as i64)
    } else {
        None
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline]
fn parse_iso(b: &[u8]) -> Option<i64> {
    let n = b.len();
    let a: &[u8; 20] = b[..20].try_into().ok()?;
    // all 14 digit positions and 5 separators checked without early branches
    let g = |i: usize| a[i].wrapping_sub(b'0');
    let digits = [g(0), g(1), g(2), g(3), g(5), g(6), g(8), g(9), g(11), g(12), g(14), g(15), g(17), g(18)];
    let mut bad = (a[4] != b'-') | (a[7] != b'-') | (a[10] != b'T') | (a[13] != b':') | (a[16] != b':');
    for &x in &digits {
        bad |= x > 9;
    }
    if bad {
        return None;
    }
    let v = |i: usize| (digits[i] as i64) * 10 + digits[i + 1] as i64;
    let y = v(0) * 100 + v(2);
    let mo = v(4);
    let d = v(6);
    let h = v(8);
    let mi = v(10);
    let s = v(12);
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let dim = match mo {
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let off = if n == 20 {
        if b[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match b[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om)
    };
    let secs = days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s - off * 60;
    Some(secs * 1000)
}

/// Reference timestamp parser (byte at a time). Used by the self-test to check `parse_ts`.
fn parse_ts_ref(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if (n == 20 || n == 25) && b[4] == b'-' {
        return parse_iso(b);
    }
    let mut v: i64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as i64)?;
    }
    Some(v)
}

const fn rep8(x: u8) -> u64 {
    0x0101_0101_0101_0101u64 * x as u64
}

#[inline(always)]
fn ld8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

/// True iff every byte selected by `m` (0xFF lanes) of `w` is an ASCII digit.
/// A byte >= 0xFA can carry into its neighbour, but it already fails its own check.
#[inline(always)]
fn digits_ok(w: u64, m: u64) -> bool {
    let x = w & m;
    let hi = rep8(0xF0) & m;
    let three = rep8(0x30) & m;
    (x & hi) == three && (x.wrapping_add(rep8(0x06) & m) & hi) == three
}

/// Value of 8 ASCII digits (already validated); the first byte is the most significant.
#[inline(always)]
fn swar8(w: u64) -> u64 {
    let x = w.wrapping_sub(rep8(b'0'));
    let x = x.wrapping_mul(10).wrapping_add(x >> 8);
    let x = (x & 0x00FF_00FF_00FF_00FF).wrapping_mul(100).wrapping_add((x >> 16) & 0x00FF_00FF_00FF_00FF);
    let x = (x & 0x0000_FFFF_0000_FFFF).wrapping_mul(10000).wrapping_add((x >> 32) & 0x0000_FFFF_0000_FFFF);
    x & 0xFFFF_FFFF
}

const CUM_DAYS: [i64; 13] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365];

/// F1 / F2 with word-sized checks. Same rules as `parse_iso`.
#[inline]
fn parse_iso_fast(b: &[u8]) -> Option<i64> {
    let n = b.len();
    // "YYYY-MM-" "DDTHH:MM" ":SS" ; little-endian lanes
    let w0 = ld8(b, 0);
    let w1 = ld8(b, 8);
    let w2 = u32::from_le_bytes(b[16..20].try_into().unwrap()) as u64;
    const MD0: u64 = 0x00FF_FF00_FFFF_FFFF; // digits at 0-3, 5, 6
    const MS0: u64 = 0xFF00_00FF_0000_0000; // '-' at 4 and 7
    const T0: u64 = ((b'-' as u64) << 32) | ((b'-' as u64) << 56);
    const MD1: u64 = 0xFFFF_00FF_FF00_FFFF; // digits at 8, 9, 11, 12, 14, 15
    const MS1: u64 = 0x0000_FF00_00FF_0000; // 'T' at 10, ':' at 13
    const T1: u64 = ((b'T' as u64) << 16) | ((b':' as u64) << 40);
    const MD2: u64 = 0x00FF_FF00; // digits at 17, 18
    let ok = (w0 & MS0) == T0
        && (w1 & MS1) == T1
        && (w2 & 0xFF) == b':' as u64
        && digits_ok(w0, MD0)
        && digits_ok(w1, MD1)
        && digits_ok(w2, MD2);
    if !ok {
        return None;
    }
    // separators -> '0' so the subtraction cannot borrow across bytes
    let v0 = ((w0 & MD0) | (rep8(b'0') & !MD0)).wrapping_sub(rep8(b'0'));
    let v1 = ((w1 & MD1) | (rep8(b'0') & !MD1)).wrapping_sub(rep8(b'0'));
    let v2 = ((w2 & MD2) | (0x3030_3030 & !MD2)).wrapping_sub(0x3030_3030);
    let by = |w: u64, k: u32| ((w >> (8 * k)) & 0xFF) as i64;
    let y = by(v0, 0) * 1000 + by(v0, 1) * 100 + by(v0, 2) * 10 + by(v0, 3);
    let mo = by(v0, 5) * 10 + by(v0, 6);
    let d = by(v1, 0) * 10 + by(v1, 1);
    let h = by(v1, 3) * 10 + by(v1, 4);
    let mi = by(v1, 6) * 10 + by(v1, 7);
    let s = by(v2, 1) * 10 + by(v2, 2);
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let leap = is_leap(y);
    let dim = match mo {
        2 => 28 + leap as i64,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let off = if n == 20 {
        if b[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match b[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om)
    };
    let days = if (1970..=2099).contains(&y) {
        // leap years in 1970..2099 are exactly the multiples of 4
        (y - 1970) * 365 + (y - 1969) / 4 + CUM_DAYS[(mo - 1) as usize] + (mo > 2 && leap) as i64 + d - 1
    } else {
        days_from_civil(y, mo, d)
    };
    Some((days * 86400 + h * 3600 + mi * 60 + s - off * 60) * 1000)
}

#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if (n == 20 || n == 25) && b[4] == b'-' {
        return parse_iso_fast(b);
    }
    if (8..=18).contains(&n) {
        // F3 that cannot overflow: last 8 digits by SWAR, the rest in a short loop
        let w = ld8(b, n - 8);
        if !digits_ok(w, !0) {
            return None;
        }
        let mut hi: i64 = 0;
        let mut bad = false;
        for &c in &b[..n - 8] {
            let d = c.wrapping_sub(b'0');
            bad |= d > 9;
            hi = hi * 10 + d as i64;
        }
        if bad {
            return None;
        }
        return Some(hi * 100_000_000 + swar8(w) as i64);
    }
    parse_ts_ref(b)
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

// ---------------------------------------------------------------- CSV

#[inline]
fn split<const N: usize>(line: S, out: &mut [S; N]) -> bool {
    let mut k = 0;
    let mut start = 0;
    let mut quote = false;
    for (i, &c) in line.iter().enumerate() {
        if c == b',' {
            if k == N - 1 {
                return false;
            }
            out[k] = &line[start..i];
            k += 1;
            start = i + 1;
        } else if c == b'"' {
            quote = true;
        }
    }
    if k != N - 1 {
        return false;
    }
    out[k] = &line[start..];
    if quote {
        for f in out.iter_mut() {
            let b = *f;
            if memchr(b'"', b).is_some() {
                let n = b.len();
                if n < 2 || b[0] != b'"' || b[n - 1] != b'"' || memchr(b'"', &b[1..n - 1]).is_some() {
                    return false;
                }
                *f = &b[1..n - 1];
            }
        }
    }
    true
}

/// Bit masks of ',', '\n' and '"' for 64 bytes at `p`.
#[inline(always)]
fn masks64(p: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let m = |v: __m256i, c: u8| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c as i8))) as u32 as u64;
        (m(a, b',') | (m(b, b',') << 32), m(a, b'\n') | (m(b, b'\n') << 32), m(a, b'"') | (m(b, b'"') << 32))
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for (i, &x) in p.iter().enumerate() {
            c |= ((x == b',') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
            q |= ((x == b'"') as u64) << i;
        }
        (c, n, q)
    }
}

/// Bits a..b (b exclusive) of a 64-bit mask.
#[inline(always)]
fn bit_range(a: u32, b: u32) -> u64 {
    let hi = if b >= 64 { !0u64 } else { (1u64 << b) - 1 };
    let lo = if a >= 64 { !0u64 } else { (1u64 << a) - 1 };
    hi & !lo
}

/// Vectorized equivalent of `for_lines` + `split::<N>`: calls `f` with the unquoted fields
/// of every well-formed line. Quote rule by counting: a line is valid iff its quote count
/// equals 2 x the number of fields that start and end with '"' (length >= 2).
#[inline]
fn scan<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = data.len();
    let mut starts = [0usize; N];
    let mut ends = [0usize; N];
    let mut k = 0usize; // fields closed so far in this line; N means too many
    let mut ls = 0usize; // line start
    let mut q = 0u32; // quotes in this line
    let mut emit = |ls: usize, starts: &[usize; N], ends: &[usize; N], q: u32| {
        let _ = ls;
        let mut out: [&'a [u8]; N] = [&[]; N];
        for i in 0..N {
            debug_assert!(starts[i] <= ends[i] && ends[i] <= data.len());
            // SAFETY: starts[i] is one past a separator and ends[i] the next separator (or
            // the end of data), both found inside `data` in increasing order.
            out[i] = unsafe { data.get_unchecked(starts[i]..ends[i]) };
        }
        if q != 0 {
            let mut good = 0u32;
            for o in out.iter_mut() {
                let b = *o;
                let n = b.len();
                if n >= 2 && b[0] == b'"' && b[n - 1] == b'"' {
                    *o = &b[1..n - 1];
                    good += 1;
                }
            }
            if 2 * good != q {
                return;
            }
        }
        f(&out);
    };
    let mut blk = 0usize;
    let mut tail = [0u8; 64];
    while blk < len {
        let block: &[u8; 64] = if blk + 64 <= len {
            data[blk..blk + 64].try_into().unwrap()
        } else {
            tail = [0u8; 64];
            tail[..len - blk].copy_from_slice(&data[blk..]);
            &tail
        };
        let (cm, nl, qm) = masks64(block);
        let mut sep = cm | nl;
        let mut seg = 0u32;
        while sep != 0 {
            let t = sep.trailing_zeros();
            sep &= sep - 1;
            let pos = blk + t as usize;
            if (nl >> t) & 1 == 0 {
                if k < N - 1 {
                    ends[k] = pos;
                    k += 1;
                    starts[k] = pos + 1;
                } else {
                    k = N;
                }
            } else {
                q += (qm & bit_range(seg, t)).count_ones();
                if k == N - 1 {
                    ends[k] = pos;
                    emit(ls, &starts, &ends, q);
                }
                ls = pos + 1;
                k = 0;
                starts[0] = ls;
                q = 0;
                seg = t + 1;
            }
        }
        q += (qm & bit_range(seg, 64)).count_ones();
        blk += 64;
    }
    if ls < len && k == N - 1 {
        ends[k] = len;
        emit(ls, &starts, &ends, q);
    }
}

#[inline]
fn for_lines(data: S, mut f: impl FnMut(S)) {
    let mut pos = 0;
    for nl in memchr_iter(b'\n', data) {
        f(&data[pos..nl]);
        pos = nl + 1;
    }
    if pos < data.len() {
        f(&data[pos..]);
    }
}

fn map_file(p: &Path, populate: bool) -> S {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {p:?}: {e}"));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let mut o = memmap2::MmapOptions::new();
    if populate {
        o.populate();
    }
    let m = unsafe { o.map(&f) }.expect("mmap");
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::Sequential);
    }
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// An input CSV opened for positional reads.
struct FileRef {
    file: File,
    len: usize,
}

fn open_csvs(dir: &Path) -> Vec<FileRef> {
    list_csv(dir)
        .into_iter()
        .map(|p| {
            let file = File::open(&p).unwrap_or_else(|e| panic!("open {p:?}: {e}"));
            let len = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
            FileRef { file, len }
        })
        .filter(|f| f.len > 0)
        .collect()
}

const PIECE: usize = 1 << 20;
const SLACK: usize = 16 << 10;

/// Piece size; PIECE_BYTES overrides it for boundary tests only.
fn piece_size() -> usize {
    static P: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *P.get_or_init(|| std::env::var("PIECE_BYTES").ok().and_then(|v| v.parse().ok()).filter(|&v| v > 0).unwrap_or(PIECE))
}

/// Reads every file in pieces of PIECE bytes, in parallel, into a reused per-thread buffer,
/// and calls `f` on the complete lines that *start* in each piece (the header line of each
/// file excluded). A piece reads 1 byte before it (to see whether a line starts at its first
/// byte) and up to the end of its last line, growing the read for very long lines.
fn for_each_piece<T: Send>(files: &[FileRef], f: impl Fn(usize, &[u8]) -> T + Sync) -> Vec<T> {
    for_each_piece_sized(files, piece_size(), f)
}

/// Each file adds at most one partial piece; reserve room for that in the piece count.
fn files_slack(files: &[FileRef]) -> usize {
    files.len().min((1 << 13) - 1)
}

/// `for_each_piece` with an explicit piece size.
fn for_each_piece_sized<T: Send>(files: &[FileRef], piece: usize, f: impl Fn(usize, &[u8]) -> T + Sync) -> Vec<T> {
    use std::os::unix::fs::FileExt;
    let slack = SLACK.min(piece);
    let tasks: Vec<(usize, usize)> = files
        .iter()
        .enumerate()
        .flat_map(|(fi, fr)| (0..fr.len.div_ceil(piece)).map(move |k| (fi, k * piece)))
        .collect();
    tasks
        .par_iter()
        .enumerate()
        .map_init(Vec::<u8>::new, |buf, (ti, &(fi, off))| {
            let fr = &files[fi];
            let rs = off.saturating_sub(1); // read start
            let piece_end = (off + piece).min(fr.len); // first byte of the next piece
            let mut want = (piece_end + slack).min(fr.len) - rs;
            loop {
                if buf.len() < want {
                    buf.resize(want, 0);
                }
                let tr = tsc();
                fr.file.read_exact_at(&mut buf[..want], rs as u64).expect("read");
                prof_add(0, tsc().wrapping_sub(tr));
                let b = &buf[..want];
                // first line starting in [off, piece_end): after the header for off == 0,
                // else after the first newline at or after byte off - 1
                let start = match memchr(b'\n', b) {
                    Some(i) => i + 1,
                    None => return f(ti, &[]),
                };
                let pe = piece_end - rs; // piece end in buffer coordinates
                if start >= pe {
                    return f(ti, &[]);
                }
                // end of the last line starting before pe: the first newline at or after pe - 1
                let end = if pe >= want && rs + want == fr.len {
                    want
                } else {
                    match memchr(b'\n', &b[pe - 1..]) {
                        Some(i) => pe - 1 + i + 1,
                        None if rs + want == fr.len => want,
                        None => {
                            want = (want * 2).min(fr.len - rs);
                            continue;
                        }
                    }
                };
                return f(ti, &b[start..end]);
            }
        })
        .collect()
}

/// Split files (header removed) into chunks at line boundaries.
fn chunks(files: &[S], target: usize) -> Vec<S> {
    let mut out = vec![];
    for &f in files {
        let mut start = match memchr(b'\n', f) {
            Some(i) => i + 1,
            None => continue,
        };
        while start < f.len() {
            let mut end = start.saturating_add(target).min(f.len());
            if end < f.len() {
                end = match memchr(b'\n', &f[end..]) {
                    Some(i) => end + i + 1,
                    None => f.len(),
                };
            }
            out.push(&f[start..end]);
            start = end;
        }
    }
    out
}

fn read_small(p: &Path) -> Vec<[S; 2]> {
    let data = map_file(p, false);
    let mut out = vec![];
    for c in chunks(&[data], usize::MAX) {
        for_lines(c, |l| {
            let mut f: [S; 2] = [&[]; 2];
            if split(l, &mut f) {
                out.push(f);
            }
        });
    }
    out
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let i = text.find(&format!("\"{key}\"")).expect("manifest key") + key.len() + 2;
    let rest = &text[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    let v = rest[a..b].as_bytes();
    if let Some(t) = parse_ts(v) {
        return t;
    }
    // Tolerate fractional seconds (".123") in the manifest only.
    let dot = v.iter().position(|&c| c == b'.').expect("manifest ts");
    let end = dot + 1 + v[dot + 1..].iter().take_while(|c| c.is_ascii_digit()).count();
    let mut frac: Vec<u8> = v[dot + 1..end].to_vec();
    frac.resize(3, b'0');
    let ms: i64 = std::str::from_utf8(&frac[..3]).unwrap().parse().unwrap();
    let mut whole = v[..dot].to_vec();
    whole.extend_from_slice(&v[end..]);
    parse_ts(&whole).expect("manifest ts") + ms
}

// ---------------------------------------------------------------- exact IDs

/// An ID string packed exactly into 64 bits, when it is "canonical": 1 to 10 bytes, any
/// first byte, the rest from [0-9a-z]. The first byte goes in bits 47..55; the rest are
/// base-37 digits (1..=36, 0 pads shorter strings) in bits 0..47 (37^9 < 2^47).
/// Comparing two canonical codes is the byte-wise comparison of the strings.
/// Anything else (uppercase, spaces, '\r', longer IDs, ...) gets bit 63 plus a 63-bit
/// hash and goes into a side table; comparisons involving it use the bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
struct Id(u64);

const FALLBACK: u64 = 1 << 63;

impl std::hash::Hash for Id {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(mix(*self));
    }
}

#[inline]
fn mix(a: Id) -> u64 {
    let mut h = a.0.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^= h >> 29;
    h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h ^= h >> 32;
    h
}

const fn b37_table() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    let mut j = 0;
    while j < 26 {
        t[b'a' as usize + j] = 11 + j as u8;
        j += 1;
    }
    t
}
static B37: [u8; 256] = b37_table();
const B37_CHARS: &[u8; 37] = b"\x000123456789abcdefghijklmnopqrstuvwxyz";

const SHARDS: usize = 64;
static FALLBACK_IDS: std::sync::OnceLock<Vec<Mutex<HashMap<u64, S>>>> = std::sync::OnceLock::new();

fn fallback_table() -> &'static Vec<Mutex<HashMap<u64, S>>> {
    FALLBACK_IDS.get_or_init(|| (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect())
}

#[inline]
fn enc(s: &[u8]) -> Id {
    let n = s.len();
    if (1..=10).contains(&n) {
        let mut v: u64 = 0;
        let mut ok = true;
        for i in 1..10 {
            let d = if i < n { B37[s[i] as usize] } else { 0 };
            ok &= d != 0 || i >= n;
            v = v * 37 + d as u64;
        }
        if ok {
            return Id(((s[0] as u64) << 47) | v);
        }
    }
    enc_fallback(s)
}

#[cold]
fn enc_fallback(s: &[u8]) -> Id {
    let code = FALLBACK | (xxh3_64(s) >> 1);
    let code = if code == u64::MAX { code - 1 } else { code };
    // input buffers are reused, so the table keeps its own copy of the string
    fallback_table()[(code % SHARDS as u64) as usize]
        .lock()
        .unwrap()
        .entry(code)
        .or_insert_with(|| &*Box::leak(s.to_vec().into_boxed_slice()));
    Id(code)
}

#[inline]
fn is_long(a: Id) -> bool {
    a.0 & FALLBACK != 0
}

struct IdStr {
    b: [u8; 16],
    n: usize,
    long: Option<S>,
}
impl IdStr {
    #[inline]
    fn bytes(&self) -> &[u8] {
        match self.long {
            Some(s) => s,
            None => &self.b[..self.n],
        }
    }
}

#[inline]
fn decode(a: Id) -> IdStr {
    if is_long(a) {
        let s = *fallback_table()[(a.0 % SHARDS as u64) as usize].lock().unwrap().get(&a.0).unwrap();
        return IdStr { b: [0; 16], n: 0, long: Some(s) };
    }
    let mut b = [0u8; 16];
    b[0] = (a.0 >> 47) as u8;
    let mut rest = a.0 & ((1u64 << 47) - 1);
    let mut digits = [0u8; 9];
    for i in (0..9).rev() {
        digits[i] = (rest % 37) as u8;
        rest /= 37;
    }
    let mut n = 1;
    for &d in &digits {
        if d == 0 {
            break;
        }
        b[n] = B37_CHARS[d as usize];
        n += 1;
    }
    IdStr { b, n, long: None }
}

/// Byte-wise order of the original strings.
#[inline]
fn id_cmp(a: Id, b: Id) -> std::cmp::Ordering {
    if !is_long(a) && !is_long(b) {
        a.cmp(&b)
    } else {
        decode(a).bytes().cmp(decode(b).bytes())
    }
}

/// Exact 128-bit key for lookup-only strings (products, campaigns): up to 15 bytes plus
/// length; longer strings hash to 120 bits with 0xFF as the length byte.
#[inline]
fn key128(s: &[u8]) -> u128 {
    let n = s.len();
    if n <= 15 {
        let mut b = [0u8; 16];
        b[..n].copy_from_slice(s);
        b[15] = n as u8;
        u128::from_be_bytes(b)
    } else {
        xxh3_128(s) | 0xFF
    }
}

// ---------------------------------------------------------------- records

/// One parsed copy of an event. `ts == INVALID` marks an invalid (or late) copy.
#[derive(Clone, Copy)]
struct EvRaw {
    eid: Id,
    user: Id,
    ingest: u32, // seconds since epoch
    ts: u32,     // ms since window_start
    pc: u32,     // price | cat << 24 (price 0 for non-purchases)
}
const INVALID: u32 = u32::MAX;


struct URow {
    name: Id,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: Option<u8>,
}

#[derive(Clone, Copy, Default)]
struct CAcc {
    sessions: u64,
    purchasing: u64,
    revenue: u64,
    assisted: u64,
}

#[inline]
fn bucket(a: Id) -> usize {
    (mix(a) >> 56) as usize
}

type Parts<T> = Vec<Vec<T>>; // NB partitions

/// Per-bucket capacity for `lines` records spread over NB buckets (+25% headroom).
/// No fixed minimum, so many tiny pieces don't reserve memory they never use.
fn part_cap(lines: usize) -> usize {
    let avg = lines / NB;
    avg + avg / 4 + if lines >= NB * 8 { 8 } else { 0 }
}

fn new_parts<T>(cap: usize) -> Parts<T> {
    (0..NB).map(|_| Vec::with_capacity(cap)).collect()
}

/// [producer][bucket] -> [bucket][producer]
fn transpose<T>(v: Vec<Parts<T>>) -> Vec<Vec<Vec<T>>> {
    let mut out: Vec<Vec<Vec<T>>> = (0..NB).map(|_| Vec::with_capacity(v.len())).collect();
    for parts in v {
        for (b, p) in parts.into_iter().enumerate() {
            if !p.is_empty() {
                out[b].push(p);
            }
        }
    }
    out
}

// ---------------------------------------------------------------- links

/// Per bucket of id: id -> the person's name (their smallest id).
type LinkMap = HashMap<Id, Id, BH>;

fn uf_find(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = p[x as usize].load(Relaxed);
        if px == x {
            return x;
        }
        let gp = p[px as usize].load(Relaxed);
        if gp != px {
            let _ = p[x as usize].compare_exchange_weak(px, gp, Relaxed, Relaxed);
        }
        x = gp;
    }
}

fn uf_union(p: &[AtomicU32], a: u32, b: u32) {
    let (mut a, mut b) = (a, b);
    loop {
        a = uf_find(p, a);
        b = uf_find(p, b);
        if a == b {
            return;
        }
        if a < b {
            std::mem::swap(&mut a, &mut b);
        }
        // link the larger root under the smaller one: parent[x] <= x always holds
        if p[a as usize].compare_exchange(a, b, AcqRel, Relaxed).is_ok() {
            return;
        }
    }
}

fn build_links(files: Vec<FileRef>) -> Vec<LinkMap> {
    let parsed: Vec<(Vec<(Id, Id)>, Parts<Id>)> = for_each_piece(&files, |_, c| {
            let mut edges = vec![];
            let mut ids: Parts<Id> = new_parts(0);
            scan::<2>(c, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    let a = enc(f[0]);
                    let b = enc(f[1]);
                    if a != b {
                        edges.push((a, b));
                    }
                    ids[bucket(a)].push(a);
                    ids[bucket(b)].push(b);
                }
            });
            (edges, ids)
        });
    let (edges, idparts): (Vec<Vec<(Id, Id)>>, Vec<Parts<Id>>) = parsed.into_iter().unzip();
    let idparts = transpose(idparts);
    // per bucket: unique ids; the map value temporarily holds the local index in `lo`
    let mut maps: Vec<(LinkMap, Vec<Id>)> = idparts
        .into_par_iter()
        .map(|parts| {
            let n: usize = parts.iter().map(|p| p.len()).sum();
            let mut m: LinkMap = HashMap::with_capacity_and_hasher(n, BH::default());
            let mut list: Vec<Id> = Vec::with_capacity(n);
            for p in parts {
                for a in p {
                    m.entry(a).or_insert_with(|| {
                        list.push(a);
                        Id((list.len() - 1) as u64)
                    });
                }
            }
            (m, list)
        })
        .collect();
    let mut off = vec![0u32; NB + 1];
    for b in 0..NB {
        off[b + 1] = off[b] + maps[b].1.len() as u32;
    }
    let n = off[NB] as usize;
    let gidx = |a: Id| -> u32 {
        let b = bucket(a);
        off[b] + maps[b].0[&a].0 as u32
    };
    let parent: Vec<AtomicU32> = (0..n as u32).map(AtomicU32::new).collect();
    edges.par_iter().for_each(|es| {
        for &(a, b) in es {
            uf_union(&parent, gidx(a), gidx(b));
        }
    });
    let gid: Vec<Id> = maps.iter().flat_map(|(_, l)| l.iter().copied()).collect();
    // smallest id per component
    let best: Vec<AtomicU32> = (0..n as u32).map(AtomicU32::new).collect();
    (0..n as u32).into_par_iter().for_each(|i| {
        let r = uf_find(&parent, i) as usize;
        let mine = gid[i as usize];
        let mut cur = best[r].load(Relaxed);
        while id_cmp(mine, gid[cur as usize]).is_lt() {
            match best[r].compare_exchange_weak(cur, i, AcqRel, Relaxed) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    });
    maps.par_iter_mut().enumerate().for_each(|(b, (m, _))| {
        let o = off[b];
        for v in m.values_mut() {
            let r = uf_find(&parent, o + v.0 as u32) as usize;
            *v = gid[best[r].load(Relaxed) as usize];
        }
    });
    maps.into_iter().map(|(m, _)| m).collect()
}


// ---------------------------------------------------------------- global tables (iteration 8)

/// Open-addressing hash table of (key, value) u64 pairs with linear probing, lock-free.
/// Keys are stored as `id + 1` so that 0 means empty (zeroed memory = empty table, so
/// allocation costs only lazy zero pages). Callers never use u64::MAX as an id.
struct Table {
    slots: Vec<u64>,
    mask: usize,
}

const MAX_PROBES: usize = 1 << 16;

impl Table {
    fn new(entries: usize) -> Table {
        let n = (entries + entries / 2).next_power_of_two().max(1 << 16);
        Table { slots: vec![0u64; 2 * n], mask: n - 1 }
    }
    #[inline(always)]
    fn a(&self) -> &[AtomicU64] {
        // SAFETY: AtomicU64 has the same layout as u64; all access goes through atomics.
        unsafe { &*(self.slots.as_slice() as *const [u64] as *const [AtomicU64]) }
    }
    #[inline(always)]
    fn home(&self, id: Id) -> usize {
        (mix(id) as usize) & self.mask
    }
    #[inline(always)]
    fn prefetch(&self, id: Id) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(2 * self.home(id)) as *const i8, _MM_HINT_T0);
        }
    }
    /// Dedup: keep the minimum of `v` per id (stored as !v with fetch_max, as 0 = empty).
    #[inline]
    fn insert_min(&self, id: Id, v: u64) {
        let (a, key) = (self.a(), id.0 + 1);
        let mut i = self.home(id);
        for _ in 0..MAX_PROBES {
            let k = a[2 * i].load(Relaxed);
            if k == key {
                a[2 * i + 1].fetch_max(!v, Relaxed);
                return;
            }
            if k == 0 {
                match a[2 * i].compare_exchange(0, key, Relaxed, Relaxed) {
                    Ok(_) => {
                        a[2 * i + 1].fetch_max(!v, Relaxed);
                        return;
                    }
                    Err(x) if x == key => {
                        a[2 * i + 1].fetch_max(!v, Relaxed);
                        return;
                    }
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
        panic!("dedup table full");
    }
    /// The minimum value stored for an id that is known to be present.
    #[inline]
    fn get_min(&self, id: Id) -> u64 {
        let (a, key) = (self.a(), id.0 + 1);
        let mut i = self.home(id);
        loop {
            let k = a[2 * i].load(Relaxed);
            if k == key {
                return !a[2 * i + 1].load(Relaxed);
            }
            debug_assert!(k != 0);
            i = (i + 1) & self.mask;
        }
    }
    /// Users: dense person index of an id, inserting a new person (named by the id itself)
    /// if the id is not in the table. Values are stored as index + 1 (0 = being written).
    #[inline]
    fn person(&self, id: Id, next: &AtomicU32, names: &[AtomicU64]) -> u32 {
        let (a, key) = (self.a(), id.0 + 1);
        let mut i = self.home(id);
        for _ in 0..MAX_PROBES {
            let k = a[2 * i].load(Acquire);
            if k == 0 {
                match a[2 * i].compare_exchange(0, key, AcqRel, Acquire) {
                    Ok(_) => {
                        let idx = next.fetch_add(1, Relaxed);
                        names[idx as usize].store(id.0, Relaxed);
                        a[2 * i + 1].store(idx as u64 + 1, Release);
                        return idx;
                    }
                    Err(x) if x != key => {
                        i = (i + 1) & self.mask;
                        continue;
                    }
                    Err(_) => {} // same key inserted concurrently: wait for its value below
                }
            } else if k != key {
                i = (i + 1) & self.mask;
                continue;
            }
            loop {
                let v = a[2 * i + 1].load(Acquire);
                if v != 0 {
                    return (v - 1) as u32;
                }
                std::hint::spin_loop();
            }
        }
        panic!("user table full");
    }
    /// Users: insert a linked id with a given person index (before any lookups).
    fn put(&self, id: Id, idx: u32) {
        let (a, key) = (self.a(), id.0 + 1);
        let mut i = self.home(id);
        for _ in 0..MAX_PROBES {
            let k = a[2 * i].load(Relaxed);
            if k == key {
                return;
            }
            if k == 0 && a[2 * i].compare_exchange(0, key, Relaxed, Relaxed).is_ok() {
                a[2 * i + 1].store(idx as u64 + 1, Relaxed);
                return;
            }
            if a[2 * i].load(Relaxed) == key {
                return;
            }
            i = (i + 1) & self.mask;
        }
        panic!("user table full");
    }
    /// Users: person index of an id, or None if the id never had a valid event copy.
    #[inline]
    fn get(&self, id: Id) -> Option<u32> {
        let (a, key) = (self.a(), id.0 + 1);
        let mut i = self.home(id);
        loop {
            let k = a[2 * i].load(Relaxed);
            if k == key {
                return Some((a[2 * i + 1].load(Relaxed) - 1) as u32);
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// A valid event copy in its person's bucket. It survives dedup iff the dedup table's
/// minimum for `eid` is (its ingest_ts, its copy number), i.e. `win`.
#[derive(Clone, Copy)]
struct PEv {
    pidx: u32,
    ts: u32,
    pc: u32,
    copy: u32,
    eid: Id,
}

#[derive(Clone, Copy)]
struct PClick {
    pidx: u32,
    ts: u32,
    cid: Id,
    camp: u16,
}

#[derive(Clone, Copy)]
struct TmpEv {
    eid: Id,
    user: Id,
    v: u64, // ingest_ts << 32 | copy
    ts: u32,
    pc: u32,
    valid: bool,
}

const LOOKAHEAD: usize = 16;

struct Globals<'a> {
    dedup: &'a Table,
    users: &'a Table,
    next: &'a AtomicU32,
    names: &'a [AtomicU64],
}

#[inline]
fn person_bucket(pidx: u32) -> usize {
    pidx as usize & (NB - 1)
}

/// Events: decode the piece into `tmp`, then do the table work with prefetching:
/// every copy goes into the dedup table; valid copies get their person and are stored.
fn parse_events_g(piece: usize, c: &[u8], ws: i64, we: i64, prod_map: &HashMap<u128, u8, BH>, unknown: u8, g: &Globals) -> Parts<PEv> {
    thread_local! {
        static TMP: std::cell::RefCell<Vec<TmpEv>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    TMP.with(|cell| {
        let mut tmp = cell.borrow_mut();
        tmp.clear();
        let mut k: u32 = 0;
        let pbase = (piece as u32) << 18;
        scan::<7>(c, |f| {
            if f[0].is_empty() {
                return;
            }
            let eid = enc(f[0]);
            let ingest_ms = parse_ts(f[3]).unwrap_or(i64::MAX / 2);
            let ingest = (ingest_ms.div_euclid(1000)).clamp(0, u32::MAX as i64) as u64;
            let copy = pbase | k;
            k += 1;
            let mut r = TmpEv { eid, user: Id::default(), v: (ingest << 32) | copy as u64, ts: 0, pc: 0, valid: false };
            'v: {
                if f[1].is_empty() || f[5].is_empty() {
                    break 'v;
                }
                let ts = match parse_ts(f[2]) {
                    Some(t) if t >= ws && t < we => t,
                    _ => break 'v,
                };
                match f[4] {
                    b"view" | b"cart" => {}
                    b"purchase" => match parse_price(f[6]) {
                        Some(p) => {
                            let cat = *prod_map.get(&key128(f[5])).unwrap_or(&unknown) as u32;
                            r.pc = p | (cat << 24);
                        }
                        None => break 'v,
                    },
                    _ => break 'v,
                }
                if ingest_ms - ts > DAY {
                    break 'v;
                }
                r.ts = (ts - ws) as u32;
                r.user = enc(f[1]);
                r.valid = true;
            }
            tmp.push(r);
        });
        assert!(k < (1 << 18), "too many lines in one piece");
        let n = tmp.len();
        let mut parts: Parts<PEv> = new_parts(part_cap(n));
        for j in 0..n.min(LOOKAHEAD) {
            g.dedup.prefetch(tmp[j].eid);
            if tmp[j].valid {
                g.users.prefetch(tmp[j].user);
            }
        }
        for j in 0..n {
            if j + LOOKAHEAD < n {
                let t = &tmp[j + LOOKAHEAD];
                g.dedup.prefetch(t.eid);
                if t.valid {
                    g.users.prefetch(t.user);
                }
            }
            let r = tmp[j];
            g.dedup.insert_min(r.eid, r.v);
            if r.valid {
                let pidx = g.users.person(r.user, g.next, g.names);
                parts[person_bucket(pidx)].push(PEv { pidx, ts: r.ts, pc: r.pc, copy: r.v as u32, eid: r.eid });
            }
        }
        parts
    })
}

/// Clicks: decode, then look up each user's person (clicks of users that never had a valid
/// event copy are irrelevant and dropped).
fn parse_clicks_g(c: &[u8], cws: i64, we: i64, camp_map: &HashMap<u128, u16, BH>, g: &Globals) -> Parts<PClick> {
    let mut tmp: Vec<(Id, PClick)> = Vec::with_capacity(c.len() / 45 + 1);
    scan::<4>(c, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let ts = match parse_ts(f[3]) {
            Some(t) if t >= cws && t < we => t,
            _ => return,
        };
        let camp = match camp_map.get(&key128(f[2])) {
            Some(&c) => c,
            None => return,
        };
        tmp.push((enc(f[1]), PClick { pidx: 0, ts: (ts - cws) as u32, cid: enc(f[0]), camp }));
    });
    let n = tmp.len();
    let mut parts: Parts<PClick> = new_parts(part_cap(n));
    for j in 0..n.min(LOOKAHEAD) {
        g.users.prefetch(tmp[j].0);
    }
    for j in 0..n {
        if j + LOOKAHEAD < n {
            g.users.prefetch(tmp[j + LOOKAHEAD].0);
        }
        let (u, mut ck) = tmp[j];
        if let Some(pidx) = g.users.get(u) {
            ck.pidx = pidx;
            parts[person_bucket(pidx)].push(ck);
        }
    }
    parts
}

// ---------------------------------------------------------------- main

fn main() {
    // Freed memory is reused by later stages; returning it to the OS only costs time
    // (mostly at exit). mimalloc reads this at start-up, so set it and re-exec once.
    // The Dockerfile sets it too, so in the container no re-exec happens.
    if std::env::var_os("MIMALLOC_PURGE_DELAY").is_none() {
        use std::os::unix::process::CommandExt;
        if let Ok(exe) = std::env::current_exe() {
            let err = std::process::Command::new(exe).args(std::env::args_os().skip(1)).env("MIMALLOC_PURGE_DELAY", "-1").exec();
            eprintln!("re-exec failed ({err}); continuing");
        }
    }
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let threads = if ncpu > 0 { ncpu } else { std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8) };
    rayon::ThreadPoolBuilder::new().num_threads(threads).build_global().unwrap();
    if std::env::var("SELFTEST").is_ok() {
        id_selftest();
    }
    fallback_table();

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");
    let cws = ws - WEEK;
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    assert!(we - ws < (u32::MAX as i64) - WEEK - 1, "window too long");

    // products / categories
    let prods = read_small(&data.join("products.csv"));
    let mut cats: Vec<S> = prods.iter().map(|p| p[1]).collect();
    cats.push(b"unknown");
    cats.sort();
    cats.dedup();
    assert!(cats.len() <= 256, "too many categories");
    let unknown = cats.binary_search(&(&b"unknown"[..])).unwrap() as u8;
    let mut prod_map: HashMap<u128, u8, BH> = HashMap::with_capacity_and_hasher(prods.len() * 2, BH::default());
    for p in &prods {
        if !p[0].is_empty() {
            prod_map.entry(key128(p[0])).or_insert(cats.binary_search(&p[1]).unwrap() as u8);
        }
    }
    let ncat = cats.len();
    // campaigns
    let camps_raw = read_small(&data.join("campaigns.csv"));
    let mut camp_map: HashMap<u128, u16, BH> = HashMap::with_capacity_and_hasher(camps_raw.len() * 2, BH::default());
    let mut camps: Vec<[S; 2]> = vec![];
    for c in &camps_raw {
        if c[0].is_empty() {
            continue;
        }
        let k = key128(c[0]);
        if !camp_map.contains_key(&k) {
            camp_map.insert(k, camps.len() as u16);
            camps.push(*c);
        }
    }
    let ncamp = camps.len();
    assert!(ncamp < 65535);
    let organic = ncamp;
    if std::env::var_os("BENCH").is_some() {
        parser_bench(&data, ws, we, &prod_map, unknown);
        unsafe { libc::_exit(0) };
    }

    let (ev_files, ck_files, ln_files) = (open_csvs(&data.join("events")), open_csvs(&data.join("clicks")), open_csvs(&data.join("links")));
    log!(t0, "setup: {} event files, {} click files, {} link files, ndays {ndays}", ev_files.len(), ck_files.len(), ln_files.len());
    let ev_bytes: usize = ev_files.iter().map(|f| f.len).sum();

    // ---- links first: every linked id gets its person's dense index
    let links = build_links(ln_files);
    let counts: Vec<usize> = links.par_iter().map(|m| m.iter().filter(|(k, v)| k == v).count()).collect();
    let mut off = vec![0u32; NB + 1];
    for b in 0..NB {
        off[b + 1] = off[b] + counts[b] as u32;
    }
    let ncomp = off[NB];
    let name_idx: Vec<HashMap<Id, u32, BH>> = links
        .par_iter()
        .enumerate()
        .map(|(b, m)| {
            let mut h: HashMap<Id, u32, BH> = HashMap::with_capacity_and_hasher(counts[b], BH::default());
            let mut r = off[b];
            for (k, v) in m.iter() {
                if k == v {
                    h.insert(*k, r);
                    r += 1;
                }
            }
            h
        })
        .collect();
    let nlinked: usize = links.iter().map(|m| m.len()).sum();
    // sizes: copies ~ bytes/80 (table for bytes/56 keys); distinct users ~ events/15 (sized for bytes/400)
    let dedup = Table::new(ev_bytes / 56 + 1024);
    let users = Table::new(nlinked + ev_bytes / 400 + 1024);
    let names_store = vec![0u64; users.mask + 1 + ncomp as usize];
    // SAFETY: same layout; all access through atomics
    let names: &[AtomicU64] = unsafe { &*(names_store.as_slice() as *const [u64] as *const [AtomicU64]) };
    links.par_iter().for_each(|m| {
        for (id, name) in m.iter() {
            let idx = name_idx[bucket(*name)][name];
            users.put(*id, idx);
            if id == name {
                names[idx as usize].store(name.0, Relaxed);
            }
        }
    });
    drop(name_idx);
    drop(links);
    let next = AtomicU32::new(ncomp);
    let g = Globals { dedup: &dedup, users: &users, next: &next, names };
    log!(t0, "links built: {nlinked} linked ids, {ncomp} linked persons");

    // ---- events: dedup table + person resolution + store into person buckets, one pass
    // copy number = piece index (14 bits) << 18 | line in piece: keep the piece count under
    // 2^14 by growing pieces for very large inputs (a valid line is >= 8 bytes, so a piece of
    // up to 2 MB has < 2^18 lines; inputs up to ~32 GB fit).
    let ev_piece = piece_size().max(ev_bytes.div_ceil((1 << 14) - files_slack(&ev_files)) + 1);
    let ev_parts: Vec<Parts<PEv>> = for_each_piece_sized(&ev_files, ev_piece, |piece, c| parse_events_g(piece, c, ws, we, &prod_map, unknown, &g));
    assert!(ev_parts.len() < (1 << 14), "too many event pieces");
    log!(t0, "events parsed");
    let ck_parts: Vec<Parts<PClick>> = for_each_piece(&ck_files, |_, c| parse_clicks_g(c, cws, we, &camp_map, &g));
    log!(t0, "clicks parsed");
    let ev_b = transpose(ev_parts);
    let ck_b = transpose(ck_parts);

    // ---- sessions per person bucket
    let nthreads = rayon::current_num_threads();
    let accs: Vec<Mutex<Vec<CAcc>>> = (0..nthreads).map(|_| Mutex::new(vec![CAcc::default(); (ncamp + 1) * ndays])).collect();
    let user_rows: Vec<Vec<URow>> = ev_b
        .into_par_iter()
        .zip(ck_b.into_par_iter())
        .enumerate()
        .map(|(b, (eparts, cparts))| {
            let mut evs: Vec<PEv> = Vec::with_capacity(eparts.iter().map(|p| p.len()).sum());
            for p in eparts {
                evs.extend_from_slice(&p);
            }
            // dedup: keep the copy holding the table's minimum (ingest_ts, copy number)
            let n = evs.len();
            let mut w = 0usize;
            for j in 0..n.min(LOOKAHEAD) {
                dedup.prefetch(evs[j].eid);
            }
            for j in 0..n {
                if j + LOOKAHEAD < n {
                    dedup.prefetch(evs[j + LOOKAHEAD].eid);
                }
                let e = evs[j];
                if dedup.get_min(e.eid) as u32 == e.copy {
                    evs[w] = e;
                    w += 1;
                }
            }
            evs.truncate(w);
            // dense local person index li = pidx >> 8 (pidx & 255 == b): counting sort, no hashing
            let np = evs.iter().map(|e| (e.pidx >> 8) as usize + 1).max().unwrap_or(0);
            let mut eoff = vec![0u32; np + 1];
            for e in &evs {
                eoff[(e.pidx >> 8) as usize + 1] += 1;
            }
            for p in 0..np {
                eoff[p + 1] += eoff[p];
            }
            let mut sorted: Vec<PEv> = evs.clone();
            {
                let mut cur = eoff.clone();
                for e in &evs {
                    let li = (e.pidx >> 8) as usize;
                    sorted[cur[li] as usize] = *e;
                    cur[li] += 1;
                }
            }
            drop(evs);
            let mut cls: Vec<PClick> = Vec::with_capacity(cparts.iter().map(|p| p.len()).sum());
            for p in cparts {
                for c in p {
                    let li = (c.pidx >> 8) as usize;
                    if li < np && eoff[li + 1] > eoff[li] {
                        cls.push(c);
                    }
                }
            }
            let mut coff = vec![0u32; np + 1];
            for c in &cls {
                coff[(c.pidx >> 8) as usize + 1] += 1;
            }
            for p in 0..np {
                coff[p + 1] += coff[p];
            }
            let mut csorted: Vec<PClick> = cls.clone();
            {
                let mut cur = coff.clone();
                for c in &cls {
                    let li = (c.pidx >> 8) as usize;
                    csorted[cur[li] as usize] = *c;
                    cur[li] += 1;
                }
            }
            drop(cls);
            let tid = rayon::current_thread_index().unwrap_or(0);
            let mut acc = accs[tid].lock().unwrap();
            let mut rows: Vec<URow> = Vec::new();
            let mut counts = vec![0u32; ncamp];
            let mut active: Vec<u16> = Vec::new();
            let mut pos = vec![0u32; ncamp];
            let mut catrev = vec![0u64; ncat];
            // Click times are relative to ws - WEEK, session times to ws.
            let wk = WEEK as u32;
            for p in 0..np {
                if eoff[p + 1] == eoff[p] {
                    continue;
                }
                let pe = &mut sorted[eoff[p] as usize..eoff[p + 1] as usize];
                if pe.len() > 1 {
                    pe.sort_unstable_by_key(|e| e.ts);
                }
                let pe = &*pe;
                let pc = &mut csorted[coff[p] as usize..coff[p + 1] as usize];
                if pc.len() > 1 {
                    pc.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| id_cmp(a.cid, b.cid)));
                }
                let pc = &*pc;
                let name = Id(names[((p as u32) << 8 | b as u32) as usize].load(Relaxed));
                let (mut lo, mut hi) = (0usize, 0usize);
                let mut cur_day: i64 = i64::MIN;
                let mut row = URow { name, day: 0, sessions: 0, events: 0, revenue: 0, top: None };
                let mut k = 0usize;
                while k < pe.len() {
                    let s = pe[k].ts;
                    let mut m = k + 1;
                    while m < pe.len() && (pe[m].ts - pe[m - 1].ts) as i64 <= GAP {
                        m += 1;
                    }
                    let mut rev = 0u64;
                    let day = (s as i64 + ws).div_euclid(DAY);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            row.top = top_cat(&mut catrev, row.revenue);
                            rows.push(row);
                        }
                        row = URow { name, day: day as i32, sessions: 0, events: 0, revenue: 0, top: None };
                        cur_day = day;
                    }
                    for e in &pe[k..m] {
                        let p = (e.pc & 0x00ff_ffff) as u64;
                        rev += p;
                        catrev[(e.pc >> 24) as usize] += p;
                    }
                    row.sessions += 1;
                    row.events += (m - k) as u32;
                    row.revenue += rev;
                    let di = (day - day0) as usize;
                    // Window [s - WEEK, s] in session time is [s, s + WEEK] in click time.
                    let sc = s + wk;
                    while hi < pc.len() && pc[hi].ts <= sc {
                        let c = pc[hi].camp as usize;
                        if counts[c] == 0 {
                            pos[c] = active.len() as u32;
                            active.push(c as u16);
                        }
                        counts[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && pc[lo].ts < s {
                        let c = pc[lo].camp as usize;
                        counts[c] -= 1;
                        if counts[c] == 0 {
                            let p = pos[c] as usize;
                            let last = *active.last().unwrap();
                            active[p] = last;
                            pos[last as usize] = p as u32;
                            active.pop();
                        }
                        lo += 1;
                    }
                    let credited = if lo < hi {
                        let mut q = hi - 1;
                        let t = pc[q].ts;
                        while q > lo && pc[q - 1].ts == t {
                            q -= 1;
                        }
                        pc[q].camp as usize
                    } else {
                        organic
                    };
                    let a = &mut acc[credited * ndays + di];
                    a.sessions += 1;
                    a.revenue += rev;
                    if rev > 0 {
                        a.purchasing += 1;
                    }
                    for &c in &active {
                        acc[c as usize * ndays + di].assisted += 1;
                    }
                    k = m;
                }
                if cur_day != i64::MIN {
                    row.top = top_cat(&mut catrev, row.revenue);
                    rows.push(row);
                }
                for &c in &active {
                    counts[c as usize] = 0;
                }
                active.clear();
            }
            rows
        })
        .collect();
    log!(t0, "sessions built: {} user rows", user_rows.iter().map(|r| r.len()).sum::<usize>());

    // campaign rows
    let mut total = vec![CAcc::default(); (ncamp + 1) * ndays];
    for a in &accs {
        let a = a.lock().unwrap();
        for (t, x) in total.iter_mut().zip(a.iter()) {
            t.sessions += x.sessions;
            t.purchasing += x.purchasing;
            t.revenue += x.revenue;
            t.assisted += x.assisted;
        }
    }

    write_outputs(&out, user_rows, &total, &camps, ncamp, ndays, day0, &cats);
    log!(t0, "done");
    prof_report();
    // Output files are complete (closed by the writers). Skip destructors and exit handlers:
    // mimalloc's exit-time cleanup of the retained memory occasionally stalled ~1.4 s.
    unsafe { libc::_exit(0) };
}

fn parse_events(c: &[u8], ws: i64, we: i64, prod_map: &HashMap<u128, u8, BH>, unknown: u8) -> Parts<EvRaw> {
    let lines = c.len() / 70 + 1; // estimate: event lines are ~80 bytes
    let mut parts: Parts<EvRaw> = new_parts(part_cap(lines));
    let t_all = tsc();
    let mut cy = [0u64; 4]; // eid+ingest, validate, user+store, lines
    scan::<7>(c, |f| {
        let ta = tsc();
        let eid = f[0];
        if eid.is_empty() {
            return;
        }
        let eid = enc(eid);
        let ingest_ms = parse_ts(f[3]).unwrap_or(i64::MAX / 2);
        let ingest = (ingest_ms.div_euclid(1000)).clamp(0, u32::MAX as i64) as u32;
        let mut r = EvRaw { eid, user: Id::default(), ingest, ts: INVALID, pc: 0 };
        let tb = tsc();
        let mut tc = 0u64;
        'v: {
            if f[1].is_empty() || f[5].is_empty() {
                break 'v;
            }
            let ts = match parse_ts(f[2]) {
                Some(t) if t >= ws && t < we => t,
                _ => break 'v,
            };
            match f[4] {
                b"view" | b"cart" => {}
                b"purchase" => match parse_price(f[6]) {
                    Some(p) => {
                        let cat = *prod_map.get(&key128(f[5])).unwrap_or(&unknown) as u32;
                        r.pc = p | (cat << 24);
                    }
                    None => break 'v,
                },
                _ => break 'v,
            }
            if ingest_ms - ts > DAY {
                break 'v;
            }
            r.ts = (ts - ws) as u32;
            tc = tsc();
            r.user = enc(f[1]);
        }
        if tc == 0 {
            tc = tsc();
        }
        parts[bucket(eid)].push(r);
        if PROF {
            let td = tsc();
            cy[0] += tb - ta;
            cy[1] += tc - tb;
            cy[2] += td - tc;
            cy[3] += 1;
        }
    });
    if PROF {
        let total = tsc() - t_all;
        prof_add(1, total - cy[0] - cy[1] - cy[2]);
        prof_add(2, cy[0]);
        prof_add(3, cy[1]);
        prof_add(4, cy[2]);
        prof_add(5, cy[3]);
    }
    parts
}


#[inline]
fn top_cat(catrev: &mut [u64], revenue: u64) -> Option<u8> {
    if revenue == 0 {
        return None;
    }
    let mut best = 0usize;
    let mut bv = 0u64;
    for (i, v) in catrev.iter_mut().enumerate() {
        if *v > bv {
            bv = *v;
            best = i;
        }
        *v = 0;
    }
    Some(best as u8)
}

fn utf8(s: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(s)
}

#[allow(clippy::too_many_arguments)]
fn write_outputs(out: &Path, user_rows: Vec<Vec<URow>>, total: &[CAcc], camps: &[[S; 2]], ncamp: usize, ndays: usize, day0: i64, cats: &[S]) {
    use arrow_array::builder::{Date32Builder, Int64Builder, StringBuilder};
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    // Uncompressed, no statistics; dictionaries only where values repeat.
    let props = || {
        use parquet::file::properties::EnabledStatistics;
        use parquet::schema::types::ColumnPath;
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_dictionary_enabled(false)
            .set_column_dictionary_enabled(ColumnPath::from("top_category"), true)
            .set_column_dictionary_enabled(ColumnPath::from("campaign_id"), true)
            .set_column_dictionary_enabled(ColumnPath::from("channel"), true)
            .build()
    };

    let uschema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let nfiles = 16usize;
    let per = user_rows.len().div_ceil(nfiles).max(1);
    let groups: Vec<&[Vec<URow>]> = user_rows.chunks(per).collect();
    groups.par_iter().enumerate().for_each(|(gi, g)| {
        let n: usize = g.iter().map(|r| r.len()).sum();
        let mut b_user = StringBuilder::with_capacity(n, n * 12);
        let mut b_day = Date32Builder::with_capacity(n);
        let mut b_s = Int64Builder::with_capacity(n);
        let mut b_e = Int64Builder::with_capacity(n);
        let mut b_r = Int64Builder::with_capacity(n);
        let mut b_t = StringBuilder::with_capacity(n, n * 8);
        for rows in g.iter() {
            for r in rows {
                b_user.append_value(utf8(decode(r.name).bytes()));
                b_day.append_value(r.day);
                b_s.append_value(r.sessions as i64);
                b_e.append_value(r.events as i64);
                b_r.append_value(r.revenue as i64);
                match r.top {
                    Some(c) => b_t.append_value(utf8(cats[c as usize])),
                    None => b_t.append_null(),
                }
            }
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(b_user.finish()),
            Arc::new(b_day.finish()),
            Arc::new(b_s.finish()),
            Arc::new(b_e.finish()),
            Arc::new(b_r.finish()),
            Arc::new(b_t.finish()),
        ];
        let batch = RecordBatch::try_new(uschema.clone(), cols).unwrap();
        let f = File::create(ud.join(format!("part-{gi:05}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(f, uschema.clone(), Some(props())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    });

    let cschema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let mut b_c = StringBuilder::new();
    let mut b_ch = StringBuilder::new();
    let mut b_day = Date32Builder::new();
    let mut b_s = Int64Builder::new();
    let mut b_p = Int64Builder::new();
    let mut b_r = Int64Builder::new();
    let mut b_a = Int64Builder::new();
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = &total[c * ndays + d];
            if a.sessions == 0 && a.assisted == 0 {
                continue;
            }
            if c == ncamp {
                b_c.append_value("organic");
                b_ch.append_value("organic");
            } else {
                b_c.append_value(utf8(camps[c][0]));
                b_ch.append_value(utf8(camps[c][1]));
            }
            b_day.append_value((day0 + d as i64) as i32);
            b_s.append_value(a.sessions as i64);
            b_p.append_value(a.purchasing as i64);
            b_r.append_value(a.revenue as i64);
            b_a.append_value(if c == ncamp { 0 } else { a.assisted as i64 });
        }
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(b_c.finish()),
        Arc::new(b_ch.finish()),
        Arc::new(b_day.finish()),
        Arc::new(b_s.finish()),
        Arc::new(b_p.finish()),
        Arc::new(b_r.finish()),
        Arc::new(b_a.finish()),
    ];
    let batch = RecordBatch::try_new(cschema.clone(), cols).unwrap();
    let f = File::create(cd.join("part-00000.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, cschema.clone(), Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Self-test of the ID encoding (SELFTEST=1): round trip and order on random strings.
fn id_selftest() {
    let mut seed = 0x1234_5678_9abc_def0u64;
    let mut rnd = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let alpha: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let odd: &[u8] = b"ABZ _\r\t\"-\x00\xc3";
    let mut strs: Vec<Vec<u8>> = Vec::new();
    for i in 0..20000 {
        let len = match i % 7 { 0 => 1, 1 => 9, 2 => 10, 3 => 11, 4 => 16, _ => 1 + (rnd() % 12) as usize };
        let mut s = Vec::with_capacity(len);
        for j in 0..len {
            let r = rnd();
            let c = if j == 0 && r % 3 == 0 { (r >> 8) as u8 } else if r % 50 == 0 { odd[(r >> 8) as usize % odd.len()] } else { alpha[(r >> 8) as usize % alpha.len()] };
            s.push(c);
        }
        strs.push(s);
    }
    strs.push(b"u".to_vec());
    strs.push(b"u0".to_vec());
    strs.push(b"u00".to_vec());
    strs.push(b"uzzzzzzzzz".to_vec());
    let leaked: Vec<S> = strs.into_iter().map(|s| &*Box::leak(s.into_boxed_slice())).collect();
    let ids: Vec<Id> = leaked.iter().map(|s| enc(s)).collect();
    let mut fails = 0;
    for (s, id) in leaked.iter().zip(&ids) {
        if decode(*id).bytes() != *s {
            fails += 1;
        }
    }
    let n = leaked.len();
    for _ in 0..2_000_000 {
        let a = (rnd() % n as u64) as usize;
        let b = (rnd() % n as u64) as usize;
        if id_cmp(ids[a], ids[b]) != leaked[a].cmp(leaked[b]) || (ids[a] == ids[b]) != (leaked[a] == leaked[b]) {
            fails += 1;
        }
    }
    let canon = ids.iter().filter(|i| !is_long(**i)).count();
    eprintln!("id selftest: {n} strings ({canon} canonical), failures: {fails}");
    // timestamps: fast parser vs byte-at-a-time reference
    let mut tfails = 0u64;
    let mut tvalid = 0u64;
    let junk: &[u8] = b"0123456789-+:TZtz .x\r\"";
    for i in 0..2_000_000u64 {
        let mut s: Vec<u8> = match i % 3 {
            0 | 1 => {
                let y = rnd() % 10000;
                let mo = rnd() % 14;
                let d = rnd() % 33;
                let h = rnd() % 26;
                let mi = rnd() % 61;
                let se = rnd() % 61;
                let mut t = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}");
                if i % 3 == 0 {
                    t.push('Z');
                } else {
                    let sign = if rnd() % 2 == 0 { '+' } else { '-' };
                    t.push_str(&format!("{sign}{:02}:{:02}", rnd() % 26, rnd() % 62));
                }
                t.into_bytes()
            }
            _ => {
                let len = 1 + (rnd() % 25) as usize;
                let zeros = (rnd() % 4) as usize;
                let mut t: Vec<u8> = vec![b'0'; zeros.min(len)];
                while t.len() < len {
                    t.push(b'0' + (rnd() % 10) as u8);
                }
                t
            }
        };
        // corrupt one byte in a quarter of the cases
        if rnd() % 4 == 0 && !s.is_empty() {
            let k = (rnd() % s.len() as u64) as usize;
            s[k] = junk[(rnd() % junk.len() as u64) as usize];
        }
        let (a, b) = (parse_ts(&s), parse_ts_ref(&s));
        tvalid += a.is_some() as u64;
        if a != b {
            tfails += 1;
            if tfails <= 5 {
                eprintln!("  ts mismatch {:?}: fast {a:?} ref {b:?}", String::from_utf8_lossy(&s));
            }
        }
    }
    eprintln!("ts selftest: 2000000 strings ({tvalid} valid), failures: {tfails}");
    let fails = fails + tfails as usize;
    std::process::exit(if fails == 0 { 0 } else { 1 });
}

// ---------------------------------------------------------------- parser bench (BENCH=1)

/// Loads up to BENCH_MB (default 200) of event data and times parser layers on ONE thread,
/// best of 5.
fn parser_bench(data: &Path, ws: i64, we: i64, prod_map: &HashMap<u128, u8, BH>, unknown: u8) {
    use std::hint::black_box;
    let limit: usize = std::env::var("BENCH_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(200) << 20;
    let mut bufs: Vec<Vec<u8>> = Vec::new();
    let mut total = 0;
    for p in list_csv(&data.join("events")) {
        if total >= limit {
            break;
        }
        let b = std::fs::read(&p).unwrap();
        total += b.len();
        bufs.push(b);
    }
    // 1 MB pieces at line boundaries, header removed (like the real reader)
    let mut pieces: Vec<&[u8]> = Vec::new();
    for b in &bufs {
        let mut start = match memchr(b'\n', b) {
            Some(i) => i + 1,
            None => continue,
        };
        while start < b.len() {
            let mut end = (start + PIECE).min(b.len());
            if end < b.len() {
                end = memchr(b'\n', &b[end..]).map(|i| end + i + 1).unwrap_or(b.len());
            }
            pieces.push(&b[start..end]);
            start = end;
        }
    }
    let lines: usize = pieces.iter().map(|p| memchr_iter(b'\n', p).count()).sum();
    eprintln!("bench: {} MB, {} pieces, {} lines (1 thread, best of 5)", total >> 20, pieces.len(), lines);
    let time = |name: &str, f: &dyn Fn(&[u8]) -> u64| {
        let mut best = f64::MAX;
        let mut chk = 0u64;
        for _ in 0..5 {
            let t = Instant::now();
            chk = 0;
            for p in &pieces {
                chk = chk.wrapping_add(f(p));
            }
            best = best.min(t.elapsed().as_secs_f64());
        }
        eprintln!("  {:<34} {:>7.1} ns/line   {:>6.0} MB/s   ({chk})", name, best * 1e9 / lines as f64, total as f64 / best / 1e6);
    };
    time("L0 newlines (memchr)", &|p| memchr_iter(b'\n', p).count() as u64);
    time("L1 block masks", &|p| {
        let mut acc = 0u64;
        let mut i = 0;
        while i + 64 <= p.len() {
            let (c, n, q) = masks64(p[i..i + 64].try_into().unwrap());
            acc += (c ^ n ^ q).count_ones() as u64;
            i += 64;
        }
        acc
    });
    time("L2 scan, empty callback", &|p| {
        let mut n = 0u64;
        scan::<7>(p, |_| n += 1);
        n
    });
    time("L3a scan + 2 enc", &|p| {
        let mut acc = 0u64;
        scan::<7>(p, |f| acc = acc.wrapping_add(enc(f[0]).0 ^ enc(f[1]).0));
        acc
    });
    time("L3b scan + 2 parse_ts", &|p| {
        let mut acc = 0u64;
        scan::<7>(p, |f| acc = acc.wrapping_add(parse_ts(f[2]).unwrap_or(0) as u64 ^ parse_ts(f[3]).unwrap_or(0) as u64));
        acc
    });
    time("L3 scan + 2 enc + 2 parse_ts", &|p| {
        let mut acc = 0u64;
        scan::<7>(p, |f| {
            acc = acc.wrapping_add(enc(f[0]).0 ^ enc(f[1]).0);
            acc = acc.wrapping_add(parse_ts(f[2]).unwrap_or(0) as u64 ^ parse_ts(f[3]).unwrap_or(0) as u64);
        });
        acc
    });
    time("L4 parse_events (full, stores)", &|p| {
        let parts = parse_events(p, ws, we, prod_map, unknown);
        black_box(parts.iter().map(|v| v.len() as u64).sum())
    });
}


