// Shopping Sessions pipeline: mmap + hand-written CSV parsing + hash partitioning.
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{AcqRel, Relaxed};
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
const CHUNK: usize = 4 << 20;

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

#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
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
fn scan<const N: usize>(data: S, mut f: impl FnMut(&[S; N])) {
    let len = data.len();
    let mut starts = [0usize; N];
    let mut ends = [0usize; N];
    let mut k = 0usize; // fields closed so far in this line; N means too many
    let mut ls = 0usize; // line start
    let mut q = 0u32; // quotes in this line
    let mut emit = |ls: usize, starts: &[usize; N], ends: &[usize; N], q: u32| {
        let _ = ls;
        let mut out: [S; N] = [&[]; N];
        for i in 0..N {
            out[i] = &data[starts[i]..ends[i]];
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
fn enc(s: S) -> Id {
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
fn enc_fallback(s: S) -> Id {
    let code = FALLBACK | (xxh3_64(s) >> 1);
    fallback_table()[(code % SHARDS as u64) as usize].lock().unwrap().entry(code).or_insert(s);
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

/// A surviving event. `key` is the user id until resolved, then the person (their name).
#[derive(Clone, Copy)]
struct SessEv {
    key: Id,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy)]
struct ClickRec {
    key: Id,
    cid: Id,
    ts: u32, // ms since window_start - 7 days
    camp: u16,
}

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

fn build_links(files: Vec<S>) -> Vec<LinkMap> {
    let ch = chunks(&files, CHUNK);
    let parsed: Vec<(Vec<(Id, Id)>, Parts<Id>)> = ch
        .par_iter()
        .map(|&c| {
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
        })
        .collect();
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

#[inline]
fn resolve(maps: &[LinkMap], b: usize, u: Id) -> Id {
    match maps[b].get(&u) {
        Some(v) => *v,
        None => u,
    }
}

// ---------------------------------------------------------------- main

fn main() {
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

    let paths: Vec<(u8, PathBuf)> = [("events", 0u8), ("clicks", 1), ("links", 2)]
        .iter()
        .flat_map(|(d, k)| list_csv(&data.join(d)).into_iter().map(move |p| (*k, p)))
        .collect();
    let mapped: Vec<(u8, S)> = paths.par_iter().map(|(k, p)| (*k, map_file(p, true))).collect();
    let pick = |k: u8| -> Vec<S> { mapped.iter().filter(|m| m.0 == k).map(|m| m.1).collect() };
    let (ev_files, ck_files, ln_files) = (pick(0), pick(1), pick(2));
    log!(t0, "setup: {} event files, {} click files, {} link files, ndays {ndays}", ev_files.len(), ck_files.len(), ln_files.len());
    if std::env::var("EXP").is_ok() {
        parse_experiment(&ev_files, ws, we, &prod_map, unknown);
        return;
    }

    // ---- stage 1: links || (events, clicks) parsing
    let (links, (stage1, clicks1)) = rayon::join(
        || {
            let l = build_links(ln_files);
            log!(t0, "links built");
            l
        },
        || {
            let ev_chunks = chunks(&ev_files, CHUNK);
            let s1: Vec<Parts<EvRaw>> = ev_chunks.par_iter().map(|&c| parse_events(c, ws, we, &prod_map, unknown)).collect();
            log!(t0, "events parsed");
            let ck_chunks = chunks(&ck_files, CHUNK);
            let c1: Vec<Parts<ClickRec>> = ck_chunks.par_iter().map(|&c| parse_clicks(c, cws, we, &camp_map)).collect();
            log!(t0, "clicks parsed");
            (transpose(s1), transpose(c1))
        },
    );

    // ---- stage 2: dedup events on event_id (smallest ingest), keep valid ones, by user bucket
    let stage2: Vec<Parts<SessEv>> = stage1
        .into_par_iter()
        .map(|parts| {
            let total: usize = parts.iter().map(|p| p.len()).sum();
            let mut map: HashMap<Id, u64, BH> = HashMap::with_capacity_and_hasher(total, BH::default());
            for (pi, p) in parts.iter().enumerate() {
                for (i, r) in p.iter().enumerate() {
                    let v = ((pi as u64) << 32) | i as u64;
                    match map.entry(r.eid) {
                        hashbrown::hash_map::Entry::Occupied(mut o) => {
                            let cur = *o.get();
                            let c = &parts[(cur >> 32) as usize][(cur & 0xffff_ffff) as usize];
                            if r.ingest < c.ingest {
                                *o.get_mut() = v;
                            }
                        }
                        hashbrown::hash_map::Entry::Vacant(e) => {
                            e.insert(v);
                        }
                    }
                }
            }
            let cap = total / NB + total / NB / 4 + 16;
            let mut out: Parts<SessEv> = new_parts(cap);
            for &v in map.values() {
                let r = &parts[(v >> 32) as usize][(v & 0xffff_ffff) as usize];
                if r.ts == INVALID {
                    continue;
                }
                out[bucket(r.user)].push(SessEv { key: r.user, ts: r.ts, pc: r.pc });
            }
            out
        })
        .collect();
    let stage2 = transpose(stage2);
    log!(t0, "events deduped");

    // ---- resolve user id -> person, re-bucket by person
    let resolved: Vec<(Parts<SessEv>, Parts<ClickRec>)> = stage2
        .into_par_iter()
        .zip(clicks1.into_par_iter())
        .enumerate()
        .map(|(b, (eparts, cparts))| {
            let ne: usize = eparts.iter().map(|p| p.len()).sum();
            let nc: usize = cparts.iter().map(|p| p.len()).sum();
            let mut eo: Parts<SessEv> = new_parts(ne / NB + ne / NB / 4 + 16);
            let mut co: Parts<ClickRec> = new_parts(nc / NB + nc / NB / 4 + 16);
            for p in eparts {
                for mut e in p {
                    e.key = resolve(&links, b, e.key);
                    eo[bucket(e.key)].push(e);
                }
            }
            for p in cparts {
                for mut c in p {
                    c.key = resolve(&links, b, c.key);
                    co[bucket(c.key)].push(c);
                }
            }
            (eo, co)
        })
        .collect();
    let (ev_r, ck_r): (Vec<Parts<SessEv>>, Vec<Parts<ClickRec>>) = resolved.into_iter().unzip();
    let ev_r = transpose(ev_r);
    let ck_r = transpose(ck_r);
    log!(t0, "persons resolved");

    // ---- stage 3: sessions per person
    let nthreads = rayon::current_num_threads();
    let accs: Vec<Mutex<Vec<CAcc>>> = (0..nthreads).map(|_| Mutex::new(vec![CAcc::default(); (ncamp + 1) * ndays])).collect();
    let user_rows: Vec<Vec<URow>> = ev_r
        .into_par_iter()
        .zip(ck_r.into_par_iter())
        .map(|(eparts, cparts)| {
            let mut evs: Vec<SessEv> = Vec::with_capacity(eparts.iter().map(|p| p.len()).sum());
            for p in eparts {
                evs.extend_from_slice(&p);
            }
            let mut cls: Vec<ClickRec> = Vec::with_capacity(cparts.iter().map(|p| p.len()).sum());
            for p in cparts {
                cls.extend_from_slice(&p);
            }
            // Group by person without comparing 128-bit keys: dense index per person,
            // counting sort into person order, then sort each person's short list by ts.
            let mut pidx: HashMap<Id, u32, BH> = HashMap::with_capacity_and_hasher(evs.len() / 4 + 16, BH::default());
            let mut persons: Vec<Id> = Vec::new();
            let eix: Vec<u32> = evs
                .iter()
                .map(|e| {
                    let n = persons.len() as u32;
                    *pidx.entry(e.key).or_insert_with(|| {
                        persons.push(e.key);
                        n
                    })
                })
                .collect();
            let np = persons.len();
            let mut eoff = vec![0u32; np + 1];
            for &x in &eix {
                eoff[x as usize + 1] += 1;
            }
            for p in 0..np {
                eoff[p + 1] += eoff[p];
            }
            let mut sorted: Vec<SessEv> = evs.clone();
            {
                let mut cur = eoff.clone();
                for (e, &x) in evs.iter().zip(&eix) {
                    sorted[cur[x as usize] as usize] = *e;
                    cur[x as usize] += 1;
                }
            }
            drop(evs);
            drop(eix);
            // clicks of persons without events are irrelevant
            let cix: Vec<u32> = cls.iter().map(|c| pidx.get(&c.key).copied().unwrap_or(u32::MAX)).collect();
            let mut coff = vec![0u32; np + 1];
            for &x in &cix {
                if x != u32::MAX {
                    coff[x as usize + 1] += 1;
                }
            }
            for p in 0..np {
                coff[p + 1] += coff[p];
            }
            let mut csorted: Vec<ClickRec> = Vec::with_capacity(coff[np] as usize);
            unsafe { csorted.set_len(coff[np] as usize) };
            {
                let mut cur = coff.clone();
                for (c, &x) in cls.iter().zip(&cix) {
                    if x != u32::MAX {
                        csorted[cur[x as usize] as usize] = *c;
                        cur[x as usize] += 1;
                    }
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
                let name = persons[p];
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
}

fn parse_events(c: S, ws: i64, we: i64, prod_map: &HashMap<u128, u8, BH>, unknown: u8) -> Parts<EvRaw> {
    let lines = c.len() / 70 + 1; // estimate: event lines are ~80 bytes
    let mut parts: Parts<EvRaw> = new_parts(lines / NB + lines / NB / 4 + 16);
    scan::<7>(c, |f| {
        let eid = f[0];
        if eid.is_empty() {
            return;
        }
        let eid = enc(eid);
        let ingest_ms = parse_ts(f[3]).unwrap_or(i64::MAX / 2);
        let ingest = (ingest_ms.div_euclid(1000)).clamp(0, u32::MAX as i64) as u32;
        let mut r = EvRaw { eid, user: Id::default(), ingest, ts: INVALID, pc: 0 };
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
        }
        parts[bucket(eid)].push(r);
    });
    parts
}

fn parse_clicks(c: S, cws: i64, we: i64, camp_map: &HashMap<u128, u16, BH>) -> Parts<ClickRec> {
    let lines = c.len() / 45 + 1; // estimate: click lines are ~50 bytes
    let mut parts: Parts<ClickRec> = new_parts(lines / NB + lines / NB / 4 + 16);
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
        let u = enc(f[1]);
        parts[bucket(u)].push(ClickRec { key: u, cid: enc(f[0]), ts: (ts - cws) as u32, camp });
    });
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

/// Temporary: time the event parse in layers (EXP=1).
fn parse_experiment(files: &[S], ws: i64, we: i64, prod_map: &HashMap<u128, u8, BH>, unknown: u8) {
    use std::hint::black_box;
    let ch = chunks(files, CHUNK);
    let mut best = [f64::MAX; 6];
    for _round in 0..5 {
        for level in 0..6 {
            let t = Instant::now();
            let n: u64 = ch
                .par_iter()
                .map(|&c| {
                    let mut acc = 0u64;
                    match level {
                        0 => for_lines(c, |l| acc += l.len() as u64),
                        1 => scan::<7>(c, |f| acc += f[2].len() as u64),
                        2 => scan::<7>(c, |f| {
                            let e = enc(f[0]);
                            let u = enc(f[1]);
                            acc ^= e.0 ^ u.0;
                        }),
                        3 => scan::<7>(c, |f| {
                            let a = parse_ts(f[2]).unwrap_or(0);
                            let b = parse_ts(f[3]).unwrap_or(0);
                            acc = acc.wrapping_add((a ^ b) as u64);
                        }),
                        4 => {
                            let p = parse_events(c, ws, we, prod_map, unknown);
                            acc += p.iter().map(|v| v.len() as u64).sum::<u64>();
                            black_box(&p);
                        }
                        _ => {
                            // full parse but drop each chunk's output immediately (allocation reuse by mimalloc)
                            let p = parse_events(c, ws, we, prod_map, unknown);
                            acc += p[0].len() as u64;
                            drop(p);
                        }
                    }
                    black_box(acc)
                })
                .sum();
            let names = ["lines only", "+ scan (simd)", "scan + 2x enc", "scan + 2x parse_ts", "full parse (kept)", "full parse (dropped)"];
            best[level] = best[level].min(t.elapsed().as_secs_f64());
            black_box(n);
            let _ = names;
        }
    }
    let names = ["lines only", "+ scan (simd)", "scan + 2x enc", "scan + 2x parse_ts", "full parse (kept)", "full parse (dropped)"];
    for l in 0..6 {
        eprintln!("best of 5: level {l} {:<22} {:>7.3}s", names[l], best[l]);
    }
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
    std::process::exit(if fails == 0 { 0 } else { 1 });
}
