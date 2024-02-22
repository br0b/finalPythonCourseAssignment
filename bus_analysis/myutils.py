from datetime import datetime, timedelta
import geopy.distance
import logging
import os
import shutil
from pandas import DataFrame
import csv
from pathlib import Path

VehicleNumberType = str
LineType = str
BrigadeType = str


class Departure:
    def __init__(self,
                 bus_stop_id: str,
                 bus_stop_number: str,
                 line: str,
                 brigade: str,
                 time: datetime):
        self._bus_stop_id = bus_stop_id
        self._bus_stop_number = bus_stop_number
        self._line = line
        self._brigade = brigade
        self._time = time

    def get_bus_stop_id(self) -> str:
        return self._bus_stop_id

    def get_bus_stop_number(self) -> str:
        return self._bus_stop_number

    def get_line(self) -> str:
        return self._line

    def get_brigade(self) -> str:
        return self._brigade

    def get_time(self) -> datetime:
        return self._time

    def serialize(self) -> list:
        return [self._bus_stop_id,
                self._bus_stop_number,
                self._line,
                self._brigade,
                self._time]

    def __str__(self) -> str:
        return f'({self._bus_stop_id}, {self._bus_stop_number}, ' \
               f'{self._line}, {self._brigade}, {self._time})'


class Position:
    def __init__(self, lat: float, lon: float):
        self._lat = lat
        self._lon = lon

    def get_lat(self) -> float:
        return self._lat

    def get_lon(self) -> float:
        return self._lon

    def serialize(self) -> list[float]:
        return [self._lat, self._lon]

    def distance_to(self, other: 'Position') -> float:
        return geopy.distance.great_circle(self.serialize(),
                                           other.serialize()).m

    def __str__(self) -> str:
        return f'({self._lat}, {self._lon})'

    def __eq__(self, other: 'Position') -> bool:
        return self._lat == other._lat and self._lon == other._lon


class BusRecord:
    def __init__(self,
                 vehicle_number: VehicleNumberType,
                 line: LineType,
                 brigade: BrigadeType,
                 time: datetime,
                 pos: Position):
        self._vehicle_number = vehicle_number
        self._line = line
        self._brigade = brigade
        self._time = time
        self._pos = pos

    def get_vehicle_number(self) -> VehicleNumberType:
        return self._vehicle_number

    def get_line(self) -> LineType:
        return self._line

    def get_brigade(self) -> BrigadeType:
        return self._brigade

    def get_time(self) -> datetime:
        return self._time

    def get_position(self) -> Position:
        """Returns the bus's location as a tuple (lon, lat)."""
        return self._pos

    def get_num(self) -> VehicleNumberType:
        return self._vehicle_number

    def serialize(self) -> list:
        """Returns a list of the bus record's attributes."""
        return ([self._vehicle_number,
                 self._line,
                 self._brigade,
                 self._time]
                + self._pos.serialize())

    def __eq__(self, other: 'BusRecord') -> bool:
        return (self._vehicle_number == other._vehicle_number
                and self._time == other._time)

    def __hash__(self) -> int:
        return hash((self._vehicle_number, self._time))

    def __str__(self) -> str:
        return f'({self._vehicle_number}, {self._line}, ' \
               f'{self._brigade}, {self._time}, {self._pos})'


class ProjectConfig:
    def __init__(self, config_json: dict[str, str | int]):
        self._bus_url: str = config_json['bus info URL']
        self._bus_stops_url: str = config_json['bus stops URL']
        self._bus_stop_lines_url: str = config_json['bus stop lines URL']
        self._schedule_url: str = config_json['schedule URL']
        self._api_key: str = config_json['API key']
        self._time_format: str = config_json['API time format']
        self._download_delay = timedelta(
            seconds=int(config_json['download delay (s)']))
        self._download_duration = timedelta(
            seconds=int(config_json['download duration (s)']))
        self._bus_data_file = config_json['bus data file']
        self._bus_stops_file = config_json['bus stops file']
        self._departures_folder = config_json['departures folder']
        self._bus_recording_stats_file = config_json[
            'bus recording statistics file']

    def get_bus_info_url(self) -> str:
        return self._bus_url

    def get_bus_stops_url(self) -> str:
        return self._bus_stops_url

    def get_bus_stop_lines_url(self) -> str:
        return self._bus_stop_lines_url

    def get_schedule_url(self) -> str:
        return self._schedule_url

    def get_api_key(self) -> str:
        return self._api_key

    def get_time_format(self) -> str:
        return self._time_format

    def get_download_delay(self) -> timedelta:
        return self._download_delay

    def get_download_duration(self) -> timedelta:
        return self._download_duration

    def get_bus_data_file(self) -> str:
        return self._bus_data_file

    def get_bus_stops_file(self) -> str:
        return self._bus_stops_file

    def get_departures_folder(self) -> str:
        return self._departures_folder

    def get_bus_recording_stats_file(self) -> str:
        return self._bus_recording_stats_file


class TimeParser:
    def __init__(self, time_format: str):
        self._time_format = time_format

    def parse_to_datetime(self, time: str) -> datetime:
        return datetime.strptime(time, self._time_format)

    def parse_to_str(self, time: datetime) -> str:
        return time.strftime(self._time_format)

    def get_date(self, time: datetime) -> str:
        return self.parse_to_str(time).split(' ')[0]


class BusRecordingStatistics:
    def __init__(self,
                 n_requests: int = 0,
                 n_successful_requests: int = 0,
                 n_records: int = 0,
                 processing_start_time: datetime = datetime.now(),
                 processing_end_time: datetime = datetime.now()):
        self._n_requests = n_requests
        self._n_successful_requests = n_successful_requests
        self._n_records = n_records
        self._processing_start_time = processing_start_time
        self._processing_end_time = processing_end_time

    def log_stats(self, time_parser: TimeParser) -> None:
        for key, val in self.to_json(time_parser).items():
            logging.info(f'{key}: {val}')

    def to_json(self, time_parser: TimeParser) \
            -> dict[str, int | timedelta]:
        """Saves the recording statistics to a json file."""
        start_time_str = time_parser.parse_to_str(self._processing_start_time)
        end_time_str = time_parser.parse_to_str(self._processing_end_time)
        return {
            'Number of requests': self._n_requests,
            'Number of successful requests': self._n_successful_requests,
            'Number of records': self._n_records,
            'Processing start time': start_time_str,
            'Processing end time': end_time_str
        }

    def add_requests_and_records_info(self,
                                      other: 'BusRecordingStatistics') \
            -> 'BusRecordingStatistics':
        return BusRecordingStatistics(
            self._n_requests + other._n_requests,
            self._n_successful_requests + other._n_successful_requests,
            self._n_records + other._n_records,
            self._processing_start_time,
            other._processing_end_time
        )

    def get_n_requests(self) ->int:
        return self._n_requests

    def get_n_successful_requests(self) -> int:
        return self._n_successful_requests

    def get_processing_start_time(self) -> datetime:
        return self._processing_start_time

    def get_processing_end_time(self) -> datetime:
        return self._processing_end_time


def save_to_csv(data: list[list], file_name: str) -> None:
    import csv
    with open(file_name, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)


def serialize_list(objs: list) -> list[list]:
    return [obj.serialize() for obj in objs]


def get_config_filename() -> str:
    return 'project_config.json'


def get_project_config() -> ProjectConfig:
    import json
    with open(get_config_filename()) as file:
        return ProjectConfig(json.load(file))


def get_bus_recording_stats(config: ProjectConfig,
                            time_parser: TimeParser) \
        -> BusRecordingStatistics:
    """Get the statistics from config.get_bus_recording_stats_file()"""
    import json
    with open(config.get_bus_recording_stats_file()) as file:
        stats = json.load(file)
        return BusRecordingStatistics(
            stats['Number of requests'],
            stats['Number of successful requests'],
            stats['Number of records'],
            time_parser.parse_to_datetime(stats['Processing start time']),
            time_parser.parse_to_datetime(stats['Processing end time'])
        )


def rmdir(directory: str) -> None:
    """Remove the directory if it exists and create a new one."""
    if os.path.exists(directory):
        shutil.rmtree(directory)


def setup_logging(level: int) -> None:
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s')


def get_max_valid_speed():
    """Get the maximum valid speed in m/s."""
    return 90 / 3.6


def get_max_legal_speed():
    """Get the maximum legal speed in m/s."""
    return 50 / 3.6


def get_speed(rec1: BusRecord, rec2: BusRecord):
    """Get the speed of a bus between two records in m/s."""
    dist = rec1.get_position().distance_to(rec2.get_position())
    t_delta = ((rec2.get_time() - rec1.get_time())
               .total_seconds())
    return dist / t_delta


def print_bus_speed_info(bus_speeds: list[tuple[BusRecord, float]],
                         speed_type: str,
                         min_speed: float,
                         max_speed: float | None = None) -> None:
    """Print information about buses with a certain speed type."""
    if not bus_speeds:
        print(f'There are no {speed_type} buses!')
        return

    speeding_bus_vehicle_nums = set()

    for _rec, speed in bus_speeds:
        if (_rec.get_vehicle_number() not in speeding_bus_vehicle_nums
                and speed is not None
                and min_speed < speed
                and (max_speed is None or speed < max_speed)):
            speeding_bus_vehicle_nums.add(_rec.get_vehicle_number())

    _n_speeding_buses = len(speeding_bus_vehicle_nums)
    n_buses = len(set(rec.get_vehicle_number() for rec, _ in bus_speeds))

    print(f'There are {_n_speeding_buses} '
          f'({"%.2f" % (_n_speeding_buses / n_buses * 100)} %) '
          f'buses that breached the speed of {min_speed * 3.6} km/h.\n')


def get_speeds(_bus_recs: list[BusRecord]) -> list[float]:
    """Get speeds of the bus records."""
    _speeds: list[float | None] = [None]

    for _i in range(1, len(_bus_recs)):
        # Calculate time delta.
        prev = _bus_recs[_i - 1]
        rec = _bus_recs[_i]
        if (prev.get_vehicle_number()
                == rec.get_vehicle_number()):
            _speeds.append(get_speed(prev, rec))
        else:
            _speeds.append(None)

    return _speeds


def get_positions_of_interest(bus_speeds: list[tuple[BusRecord, float]]) \
        -> list[tuple[Position, int]]:
    """Get positions where many speeding buses were detected.
    Integer threshold is the number of buses that is considered numerous.
    """
    pos_speeds = []
    visited: set[BusRecord] = set()
    n_processed = 0

    for rec, speed in bus_speeds:
        if rec in visited:
            continue
        visited.add(rec)
        n_buses_in_vicinity = 1
        for rec2, speed2 in bus_speeds:
            if (rec2 not in visited and
                    rec.get_position()
                            .distance_to(rec2.get_position()) < 100):
                visited.add(rec2)
                n_buses_in_vicinity += 1
        pos_speeds.append((rec.get_position(), n_buses_in_vicinity))
        n_processed += 1
        print('%.2f' % (n_processed / len(bus_speeds) * 100),
              '%',
              end='\r')

    return pos_speeds


def get_departure_file_path(_config: ProjectConfig, _file_id: int) -> Path:
    """Get the path to the departure file."""
    return Path(f'{_config.get_departures_folder()}/{str(_file_id)}.csv')


def get_departures_from_csv(_path: Path,
                            _time_parser: TimeParser) -> list[Departure]:
    """Get departures from a csv file."""
    _departures = []
    with open(_path, newline='') as _csvfile:
        reader = csv.reader(_csvfile)
        for row in reader:
            _departures.append(Departure(row[0],
                                         row[1],
                                         row[2],
                                         row[3],
                                         _time_parser.parse_to_datetime(
                                             row[4])))
    return _departures


def get_bus_recs_of_brigade(_busesdf: DataFrame,
                            line: str,
                            brigade: str,
                            _time_parser: TimeParser) -> list[BusRecord]:
    """Get bus records of a certain brigade."""
    return dataframe_to_bus_records(
        _busesdf.loc[(_busesdf['Line'] == line)
                     & (_busesdf['Brigade'] == brigade)],
        _time_parser)


def dataframe_to_bus_records(_busesdf: DataFrame,
                             _time_parser: TimeParser) -> list[BusRecord]:
    """Convert a dataframe to a list of bus records."""
    return [BusRecord(row[1],
                      row[2],
                      row[3],
                      _time_parser.parse_to_datetime(row[4]),
                      Position(row[5], row[6]))
            for row in _busesdf.itertuples()]
