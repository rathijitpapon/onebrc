/*
* Input value ranges are as follows:
1. Station name: non null UTF-8 string of min length 1 character and max length 100 bytes,
    containing neither ; nor \n characters. (i.e. this could be 100 one-byte characters,
    or 50 two-byte characters, etc.)
2. Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive),
    always with one fractional digit
* There is a maximum of 10,000 unique station names
* Line endings in the file are \n characters on all platforms
* Implementations must not rely on specifics of a given data set,
    e.g. any valid station name as per the constraints above and
    any data distribution (number of measurements per station) must be supported
* The rounding of output values must be done using the semantics of
    IEEE 754 rounding-direction "roundTowardPositive"
*/

use ahash::AHashMap;
use std::str;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

#[derive(Debug, Clone, Copy)]
struct WeatherData {
    total_temperature: f32,
    min_temperature: f32,
    max_temperature: f32,
    count: u32,
    mean_temperature: f32,
}

impl WeatherData {
    #[inline(always)]
    fn merge(&mut self, other: &WeatherData) {
        self.total_temperature += other.total_temperature;
        self.count += other.count;
        self.min_temperature = self.min_temperature.min(other.min_temperature);
        self.max_temperature = self.max_temperature.max(other.max_temperature);
    }

    #[inline(always)]
    fn add_temperature(&mut self, temperature: f32) {
        self.min_temperature = self.min_temperature.min(temperature);
        self.max_temperature = self.max_temperature.max(temperature);
        self.total_temperature += temperature;
        self.count += 1;
    }

    #[inline(always)]
    fn update_mean(&mut self) {
        self.mean_temperature = self.total_temperature / self.count as f32;
    }

    #[inline(always)]
    fn round(&mut self) {
        self.mean_temperature = (self.mean_temperature * 10.0).round() / 10.0;
        self.min_temperature = (self.min_temperature * 10.0).round() / 10.0;
        self.max_temperature = (self.max_temperature * 10.0).round() / 10.0;
    }
}

const KEY_SIZE: usize = 16;
type Key = [u8; KEY_SIZE];
type StationTemperatures = AHashMap<Key, WeatherData>;

fn process_weather_line(line: &str) -> (Key, WeatherData) {
    let parts: Vec<&str> = line.split(';').collect();
    if parts.len() != 2 || line.is_empty() {
        panic!("Invalid line");
    }

    let mut key = [0u8; KEY_SIZE];
    let name = parts[0].as_bytes();
    let station_length = name.len().min(KEY_SIZE);
    key[..station_length].copy_from_slice(&name[..station_length]);
    let temperature = parts[1].parse::<f32>().unwrap();

    let weather_data = WeatherData {
        total_temperature: temperature,
        count: 1,
        min_temperature: temperature,
        max_temperature: temperature,
        mean_temperature: 0.0,
    };

    (key, weather_data)
}

#[inline(always)]
fn process_buffer(buf: &[u8]) -> (StationTemperatures, u32) {
    let mut station_temperatures: StationTemperatures = AHashMap::with_capacity(1000);
    let mut station_name = [0u8; KEY_SIZE];
    let mut temperature = 0.0;
    let mut lines_count = 0;
    let mut negative_multiplier = 1;
    let mut state = 0;
    let mut station_index = 0;

    buf.iter().enumerate().for_each(|(index, &byte)| {
        if byte == b';' {
            state = 1;
        } else if state == 0 && station_index < KEY_SIZE {
            station_name[station_index] = byte;
            station_index += 1;
        } else if byte == b'.' {
            temperature = temperature + (u8::from(buf[index + 1]) - 48) as f32 * 0.1;
            temperature = temperature * negative_multiplier as f32;
            state = 2;
        } else if byte == b'-' {
            negative_multiplier = -1;
        } else if state == 1 {
            temperature = temperature * 10.0 + (u8::from(byte) - 48) as f32;
        } else if byte == b'\n' {
            if let Some(data) = station_temperatures.get_mut(&station_name) {
                data.add_temperature(temperature);
            } else {
                station_temperatures.insert(
                    station_name,
                    WeatherData {
                        total_temperature: temperature,
                        count: 1,
                        min_temperature: temperature,
                        max_temperature: temperature,
                        mean_temperature: 0.0,
                    },
                );
            }

            lines_count += 1;
            station_name.fill(0);
            temperature = 0.0;
            negative_multiplier = 1;
            state = 0;
            station_index = 0;
        }
    });

    return (station_temperatures, lines_count);
}

fn process_thread(buf: &[u8], extra_buffer_size: usize) -> (StationTemperatures, u32) {
    let start_index = buf
        .iter()
        .position(|&b| b == b'\n')
        .map(|i| i + 1)
        .unwrap_or(0);

    let buf_default_pos = buf.len() - extra_buffer_size;
    let end_index = buf[buf_default_pos..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|i| i + buf_default_pos + 1)
        .unwrap_or(buf_default_pos);

    process_buffer(&buf[start_index..end_index])
}

const TOTAL_LINES: usize = 1_000_000_000;
const AVG_ROW_SIZE: usize = 14;
const THREAD_COUNT: usize = 250;
const BUFFER_SIZE: usize = 2_000_000;
const STAGE_COUNT: usize = (TOTAL_LINES * AVG_ROW_SIZE).div_ceil(THREAD_COUNT * BUFFER_SIZE);
const SINGLE_ROW_SIZE: usize = 64;

fn main() {
    let start_time = time::Instant::now();

    println!("buffer size: {:?}", BUFFER_SIZE);

    // let cores: usize = std::thread::available_parallelism().unwrap().into();
    // println!("{}", cores);

    let file_path = "measurements.txt";

    let mut station_temperatures: StationTemperatures = AHashMap::with_capacity(500);

    // Process first line
    let mut file = File::open(file_path).expect("Unable to open file");
    let mut buf = [0; KEY_SIZE + 5];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read(&mut buf).unwrap();
    let first_line = str::from_utf8(&buf)
        .unwrap()
        .split('\n')
        .collect::<Vec<&str>>()[0];
    let (key, value) = process_weather_line(first_line);
    station_temperatures.insert(key, value);

    let total_lines = Arc::new(AtomicU32::new(1));
    let station_temperatures_list: Arc<Mutex<Vec<StationTemperatures>>> =
        Arc::new(Mutex::new(Vec::with_capacity(THREAD_COUNT)));

    (0..STAGE_COUNT).for_each(|stage_index| {
        let mut file_reader_threads = Vec::with_capacity(THREAD_COUNT);

        (0..THREAD_COUNT).for_each(|thread_index| {
            let mut buf = [0; BUFFER_SIZE + SINGLE_ROW_SIZE];
            let start = stage_index * BUFFER_SIZE * THREAD_COUNT + thread_index * BUFFER_SIZE;

            let station_temperatures_list = Arc::clone(&station_temperatures_list);
            let total_lines = Arc::clone(&total_lines);

            let mut file = File::open(file_path).expect("Unable to open file");

            let file_reader_thread = thread::spawn(move || {
                file.seek(SeekFrom::Start(start as u64)).unwrap();
                file.read(&mut buf).unwrap();
                let (station_temperatures, lines_count) = process_thread(&buf, SINGLE_ROW_SIZE);

                total_lines.fetch_add(lines_count, std::sync::atomic::Ordering::SeqCst);

                let mut station_temperatures_list = station_temperatures_list.lock().unwrap();
                station_temperatures_list.push(station_temperatures);
            });

            file_reader_threads.push(file_reader_thread);
        });

        file_reader_threads
            .into_iter()
            .for_each(|thread| thread.join().unwrap());

        println!("Stage: {:?} completed", stage_index);
    });

    let station_temperatures_list = station_temperatures_list.lock().unwrap();
    station_temperatures_list.iter().for_each(|st| {
        st.iter().for_each(|(station_name, data)| {
            if let Some(parent_data) = station_temperatures.get_mut(station_name) {
                parent_data.merge(&data);
            } else {
                station_temperatures.insert(*station_name, *data);
            }
        });
    });

    station_temperatures.values_mut().for_each(|data| {
        data.update_mean();
        data.round();
    });

    let end_time = start_time.elapsed();

    let mut station_temperatures: Vec<_> = station_temperatures.iter().collect();
    station_temperatures.sort_by(|a, b| a.0.cmp(b.0));

    for (station_name, data) in station_temperatures.iter() {
        println!(
            "Station: {:?}, Min: {}, Mean: {}, Max: {}",
            str::from_utf8(station_name.as_slice())
                .unwrap()
                .replace("\0", ""),
            data.min_temperature,
            data.mean_temperature,
            data.max_temperature
        );
        // println!(
        //     "{}={}/{}/{}",
        //     station_name, data.min_temperature, data.mean_temperature, data.max_temperature
        // );
    }

    println!(
        "Total lines: {:?}",
        total_lines.load(std::sync::atomic::Ordering::SeqCst)
    );
    println!("Total stations: {:?}", station_temperatures.len());
    println!("Elapsed time: {:?}", end_time);
}
