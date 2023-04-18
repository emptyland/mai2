pub const MAX_LEVEL: usize = 4;
pub const MAX_NUMBER_OF_LEVEL_0_FILES: i32 = 10;
pub const LIMIT_MIN_NUMBER_OF_SLOTS: usize = 17;

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

pub fn compute_number_of_slots(level: usize, number_of_entries: usize, limit_min_number_of_slots: usize) -> usize {
    if number_of_entries == 0 {
        return 0;
    }
    assert!(limit_min_number_of_slots > 0);

    let adjust_factor = 0.978 - (level as f32 * 0.212);
    let mut rv = (number_of_entries as f32 * adjust_factor) as usize;
    if rv < limit_min_number_of_slots {
        rv = limit_min_number_of_slots;
    }
    while rv % 2 == 0 || rv % 3 == 0 || rv % 5 == 0 || rv % 7 == 0 || rv % 11 == 0 {
        rv += 1;
    }
    rv
}

pub fn round_down(x: usize, m: i64) -> usize {
    (x as u64 & -m as u64) as usize
}

// return RoundDown<T>(static_cast<T>(x + m - 1), m);
pub fn round_up(x: usize, m: i64) -> usize {
    round_down((x as i64 + m - 1) as usize, m)
}