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

use std::collections::BTreeMap;
use std::str;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

#[derive(Debug, Clone)]
struct WeatherData {
    total_temperature: f64,
    min_temperature: f64,
    max_temperature: f64,
    count: u32,
    mean_temperature: f64,
}

impl WeatherData {
    fn merge(&mut self, other: &WeatherData) {
        self.total_temperature += other.total_temperature;
        self.count += other.count;
        self.min_temperature = self.min_temperature.min(other.min_temperature);
        self.max_temperature = self.max_temperature.max(other.max_temperature);
    }

    fn update_mean(&mut self) {
        self.mean_temperature = self.total_temperature / self.count as f64;
    }

    fn round(&mut self) {
        self.mean_temperature = (self.mean_temperature * 10.0).round() / 10.0;
        self.min_temperature = (self.min_temperature * 10.0).round() / 10.0;
        self.max_temperature = (self.max_temperature * 10.0).round() / 10.0;
    }
}

fn process_weather_line(line: &str) -> Result<(&str, WeatherData), &'static str> {
    let parts: Vec<&str> = line.split(';').collect();
    if parts.len() != 2 || line.is_empty() {
        return Err("Invalid line");
    }

    let station_name = parts[0];
    let temperature = parts[1].parse::<f64>().unwrap();

    let weather_data = WeatherData {
        total_temperature: temperature,
        count: 1,
        min_temperature: temperature,
        max_temperature: temperature,
        mean_temperature: 0.0,
    };

    Ok((station_name, weather_data))
}

fn process_buffer_bytes(
    buf: &[u8],
    extra_buffer_size: usize,
    total_lines: Arc<Mutex<u32>>,
) -> BTreeMap<String, WeatherData> {
    let mut start_index = 0;
    let mut end_index = buf.len() - extra_buffer_size;

    for (index, &byte) in buf.iter().enumerate() {
        if byte == b'\n' {
            start_index = index + 1;
            break;
        }
    }

    for (index, &byte) in buf[buf.len() - extra_buffer_size..].iter().enumerate() {
        if byte == b'\n' {
            end_index = index + buf.len() - extra_buffer_size;
            break;
        }
    }

    let line_str = str::from_utf8(&buf[start_index..end_index]).unwrap();
    let lines: Vec<&str> = line_str.split('\n').collect();

    let mut station_temperatures: BTreeMap<String, WeatherData> = BTreeMap::new();
    let mut lines_count = 0;

    for line in lines {
        if let Ok((station_name, weather_data)) = process_weather_line(line) {
            if let Some(data) = station_temperatures.get_mut(station_name) {
                data.merge(&weather_data);
            } else {
                station_temperatures.insert(station_name.to_string(), weather_data);
            }
            lines_count += 1;
        }
    }

    let mut total_lines = total_lines.lock().unwrap();
    *total_lines += lines_count;
    drop(total_lines);

    return station_temperatures;
}

fn main() {
    let start_time = time::Instant::now();

    // let cores: usize = std::thread::available_parallelism().unwrap().into();
    // println!("{}", cores);

    // let file_path = "weather_stations.csv";
    let file_path = "measurements.txt";

    let stage_count = 30;
    let max_threads = 250;
    let buffer_size = 2000000;
    let single_row_size = 100;

    let mut station_temperatures: BTreeMap<String, WeatherData> = BTreeMap::new();

    // Process first line
    let mut file = File::open(file_path).expect("Unable to open file");
    let mut buf = vec![0; single_row_size];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read(&mut buf).unwrap();
    let first_line = str::from_utf8(&buf)
        .unwrap()
        .split('\n')
        .collect::<Vec<&str>>()[0];
    let first_line_data = process_weather_line(first_line).unwrap();
    station_temperatures.insert(first_line_data.0.to_string(), first_line_data.1);

    let total_lines = Arc::new(Mutex::new(1));
    let station_temperatures_list: Arc<Mutex<Vec<BTreeMap<String, WeatherData>>>> =
        Arc::new(Mutex::new(Vec::new()));

    for stage_index in 0..stage_count {
        let mut file_reader_threads = Vec::new();

        for thread_index in 0..max_threads {
            let mut file = File::open(file_path).expect("Unable to open file");
            let mut buf = vec![0; buffer_size + single_row_size];
            let start = stage_index * buffer_size * max_threads + thread_index * buffer_size;
            let total_lines = Arc::clone(&total_lines);
            let station_temperatures_list = Arc::clone(&station_temperatures_list);

            let file_reader_thread = thread::spawn(move || {
                file.seek(SeekFrom::Start(start as u64)).unwrap();
                file.read(&mut buf).unwrap();
                let station_temperatures = process_buffer_bytes(&buf, single_row_size, total_lines);

                let mut station_temperatures_list = station_temperatures_list.lock().unwrap();
                station_temperatures_list.push(station_temperatures);
            });

            file_reader_threads.push(file_reader_thread);
        }

        for file_reader_thread in file_reader_threads {
            file_reader_thread.join().unwrap();
        }

        println!("Stage: {:?} completed", stage_index);
    }

    let station_temperatures_list = station_temperatures_list.lock().unwrap();

    for station_temperatures_data in station_temperatures_list.iter() {
        for (station_name, data) in station_temperatures_data {
            if let Some(parent_data) = station_temperatures.get_mut(station_name) {
                parent_data.merge(&data);
            } else {
                station_temperatures.insert(station_name.to_string(), data.clone());
            }
        }
    }

    for (_, data) in station_temperatures.iter_mut() {
        data.update_mean();
        data.round();
    }

    let mut station_temperatures: Vec<_> = station_temperatures.iter().collect();
    station_temperatures.sort_by(|a, b| a.0.cmp(b.0));

    for (station_name, data) in station_temperatures.iter() {
        println!(
            "Station: {}, Min: {}, Mean: {}, Max: {}",
            station_name, data.min_temperature, data.mean_temperature, data.max_temperature
        );
        // println!(
        //     "{}={}/{}/{}",
        //     station_name, data.min_temperature, data.mean_temperature, data.max_temperature
        // );
    }

    println!("Total lines: {:?}", *total_lines.lock().unwrap());
    println!("Total stations: {:?}", station_temperatures.len());
    println!("Elapsed time: {:?}", start_time.elapsed());
}
