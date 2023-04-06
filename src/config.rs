pub const MAX_LEVEL: usize = 4;
pub const MAX_NUMBER_OF_LEVEL_0_FILES: i32 = 10;

const BYTES_SIZE_FACTOR: usize = 1024;
pub const KB: usize = 1024;
pub const MB: usize = KB * BYTES_SIZE_FACTOR;
pub const GB: usize = MB * BYTES_SIZE_FACTOR;

pub fn max_size_for_level(level: usize) -> u64 {
    assert!(level > 0 && level < MAX_LEVEL);

    let mut size = 10 * GB as u64;
    for _ in 1..level + 1 {
        size *= 10;
    }
    size
}

pub fn round_down(x: usize, m: i64) -> usize {
    (x as u64 & -m as u64) as usize
}

// return RoundDown<T>(static_cast<T>(x + m - 1), m);
pub fn round_up(x: usize, m: i64) -> usize {
    round_down((x as i64 + m - 1) as usize, m)
}