from datetime import timedelta, datetime
import json
import logging
from time import sleep
from typing import Any
import urllib.request

import myutils
from myutils import (BusRecord,
                     BusRecordingStatistics,
                     Departure,
                     Position,
                     ProjectConfig,
                     TimeParser,
                     VehicleNumberType)

LineType = str
BrigadeType = str
BusRecordJsonType = dict[str, str | float]
BusStopJsonType = dict[str, list[dict[str, str]]]
BusStopLineJsonType = dict[str, list[dict[str, str]]]
DepartureJsonType = dict[str, list[dict[str, str]]]
BusLineType = str


class BusStop:
    def __init__(self,
                 bus_stop_id: str,
                 bus_stop_number: str,
                 pos: Position):
        self._bus_stop_id = bus_stop_id
        self._bus_stop_number = bus_stop_number
        self._pos = pos

    def get_bus_stop_id(self) -> str:
        return self._bus_stop_id

    def get_bus_stop_number(self) -> str:
        return self._bus_stop_number

    def get_position(self) -> Position:
        return self._pos

    def serialize(self) -> list:
        return ([self._bus_stop_id,
                self._bus_stop_number]
                + self._pos.serialize())


def get_max_delay() -> timedelta:
    return timedelta(minutes=5)


def get_departure_time_window(config: ProjectConfig) \
        -> tuple[datetime, datetime]:
    """Returns a time window of departures to download."""
    return (datetime.now() - get_max_delay(),
            datetime.now()
            + 2 * config.get_download_duration()
            + get_max_delay())


def get_bus_record_from_json(bus_record: BusRecordJsonType,
                             time_parser: TimeParser) -> BusRecord:
    return BusRecord(bus_record['VehicleNumber'],
                     bus_record['Lines'],
                     bus_record['Brigade'],
                     time_parser.parse_to_datetime(bus_record['Time']),
                     Position(bus_record['Lat'],
                              bus_record['Lon']))


def get_bus_stop_from_json(bus_stop: BusStopJsonType) -> BusStop:
    arr = bus_stop['values']
    return BusStop(arr[0]['value'],
                   arr[1]['value'],
                   Position(float(arr[4]['value']),
                            float(arr[5]['value'])))


def get_bus_stop_line_from_json(bus_stop_line: BusStopLineJsonType) \
        -> BusLineType:
    return bus_stop_line['values'][0]['value']


def get_departure_from_json(bus_stop_id: str,
                            bus_stop_number: str,
                            line: str,
                            departure: DepartureJsonType,
                            time_parser: TimeParser) -> Departure:
    arr = departure['values']
    time = arr[5]['value']
    if int(time[:2]) < 24:
        time = time_parser.get_date(datetime.now()) + ' ' + time
    else:
        time = (time_parser.get_date(datetime.now() + timedelta(days=1))
                + ' '
                + str(int(time[:2]) - 24).zfill(2) + time[2:])
    return Departure(
        bus_stop_id,
        bus_stop_number,
        line,
        arr[2]['value'],
        time_parser.parse_to_datetime(time))


def is_response_json_valid(response: dict[str, Any]) -> bool:
    return 'result' in response and isinstance(response['result'], list)


def log_api_error(response: dict[str, Any]) -> None:
    logging.warning(f'API returned an error message: '
                    f'\'{response["result"]}\'.')


def save_departures_to_file(departures: list[Departure],
                            config: ProjectConfig,
                            file_id: int) -> None:
    myutils.save_to_csv(
        myutils.serialize_list(departures),
        f'{config.get_departures_folder()}/{file_id}.csv')


class DataDownloader:
    def __init__(self, config: ProjectConfig, time_parser: TimeParser):
        self._config = config
        self._time_parser = time_parser

    def download_bus_stops(self) -> list[BusStop]:
        """Downloads bus stops from the API and returns them as a list."""
        bus_stop_url = self._add_api_key(self._config.get_bus_stops_url())
        logging.info('Requesting bus stops.')
        bus_stops_json: dict[str, list[BusStopJsonType]] \
            = self._download_valid_json(bus_stop_url,
                                        delay=timedelta(0))
        bus_stops = [get_bus_stop_from_json(bus_stop_json)
                     for bus_stop_json in bus_stops_json['result']]
        logging.info(f'{len(bus_stops)} bus stops received.')
        return bus_stops

    def download_departures(
            self,
            bus_stops: list[BusStop]) \
            -> None:
        departures = []
        file_id = 1
        n_departures = 0
        for bus_stop in bus_stops:
            bus_stop_id = bus_stop.get_bus_stop_id()
            bus_stop_num = bus_stop.get_bus_stop_number()
            logging.info(f'Downloading departures for bus stop '
                         f'({bus_stop_id}, {bus_stop_num})')
            lines = self._download_bus_lines(bus_stop)
            for line in lines:
                logging.debug(f'Downloading departures for line {line}.')
                cur_departures = self._download_departures_for_line(
                    bus_stop_id,
                    bus_stop_num,
                    line)
                departures += cur_departures
                n_departures += len(cur_departures)
                # If many departures are received, save them to a file.
                if len(departures) > 10000:
                    save_departures_to_file(departures, self._config, file_id)
                    file_id += 1
                    departures.clear()
            logging.info(f'{n_departures} departures received.')
        if departures:
            save_departures_to_file(departures, self._config, file_id)

    def record_buses(self) -> tuple[list[BusRecord], BusRecordingStatistics]:
        url = self._add_api_key(self._config.get_bus_info_url())
        time_of_start = datetime.now()
        time_of_end = time_of_start + self._config.get_download_duration()
        download_duration = self._config.get_download_duration() \
            .total_seconds()
        self._log_start_of_recording(time_of_end, download_duration)
        bus_recs, stats = self._record_buses_loop(url, time_of_start)
        stats = BusRecordingStatistics(
            stats.get_n_requests(),
            stats.get_n_successful_requests(),
            len(bus_recs),
            time_of_start,
            datetime.now())
        return bus_recs, stats

    def _download_departures_for_line(
            self,
            bus_stop_id: str,
            bus_stop_number: str,
            line: BusLineType) \
            -> list[Departure]:
        url = self._get_departures_url(bus_stop_id, bus_stop_number, line)
        departures_json = self._download_valid_json(url, delay=timedelta(0))
        departures = [get_departure_from_json(bus_stop_id,
                                              bus_stop_number,
                                              line,
                                              departure_json,
                                              self._time_parser)
                      for departure_json in departures_json['result']]
        return departures

    def _download_bus_lines(self,
                            bus_stop: BusStop) -> list[BusLineType]:
        bus_stop_id = bus_stop.get_bus_stop_id()
        bus_stop_num = bus_stop.get_bus_stop_number()
        url = self._get_bus_stop_lines_url(bus_stop_id,
                                           bus_stop_num)
        bus_lines_json = self._download_valid_json(url,
                                                   delay=timedelta(0))
        return [get_bus_stop_line_from_json(line_json)
                for line_json in bus_lines_json['result']]

    def _record_buses_loop(self, url: str, time_of_start: datetime) \
            -> tuple[list[BusRecord], BusRecordingStatistics]:
        stats = BusRecordingStatistics()
        bus_recs = []
        last_bus_recs: dict[VehicleNumberType, datetime] = dict()
        time_of_end = time_of_start + self._config.get_download_duration()

        while datetime.now() < time_of_end:
            logging.info(f'Requesting bus data.')
            cur_bus_recs, cur_stats = self._download_current_buses(url)
            valid_buses = [bus_rec for bus_rec in cur_bus_recs
                           if self._is_record_valid(last_bus_recs,
                                                    bus_rec,
                                                    time_of_start)]
            logging.info(f'New {len(valid_buses)} records.')
            bus_recs += valid_buses
            stats = stats.add_requests_and_records_info(cur_stats)

        return bus_recs, stats

    def _download_current_buses(self, url: str) \
            -> tuple[list[BusRecord], BusRecordingStatistics]:
        bus_recs, cur_n_requests = self._download_valid_json(
            url,
            return_n_requests=True)
        stats = BusRecordingStatistics(cur_n_requests, 1)
        bus_recs = self._parse_bus_records(bus_recs)
        return bus_recs, stats

    def _parse_bus_records(self,
                           buses_json: dict[str, list[BusRecordJsonType]]) \
            -> list[BusRecord]:
        """Adds new records to buses_hist."""
        new_records: list[BusRecord] = []

        for bus in buses_json['result']:
            try:
                new_records.append(get_bus_record_from_json(bus, self._time_parser))
            except ValueError as e:
                logging.warning(f'Error while parsing bus record: {e}')
                continue

        return new_records

    def _download_valid_json(self,
                             url: str,
                             return_n_requests: bool = False,
                             delay: timedelta | None = None) -> Any:
        """Tries to download a valid json in a loop."""
        n_requests = 0
        while True:
            response = self._download_json(url, delay)
            n_requests += 1
            if is_response_json_valid(response):
                if return_n_requests:
                    return response, n_requests
                else:
                    return response
            else:
                log_api_error(response)

    def _download_json(self, url: str, delay: timedelta | None = None) -> Any:
        """Downloads data from URL as JSON and deserializes it."""
        txt = urllib.request.urlopen(url).read()
        self._delay(delay)
        return json.loads(txt)

    def _delay(self, delay: timedelta | None = None) -> None:
        """If delay is None, sleeps for the time specified in
        project config.
        """
        if delay is None:
            sleep(self._config.get_download_delay().total_seconds())
        else:
            sleep(delay.total_seconds())

    def _is_record_valid(self,
                         last_bus_recs: dict[VehicleNumberType, datetime],
                         bus_rec: BusRecord,
                         time_of_start: datetime) -> bool:
        """A bus record is valid when it is not a duplicate of
        the last record and is not too old.
        """
        veh_num = bus_rec.get_vehicle_number()
        if (veh_num in last_bus_recs
                and last_bus_recs[veh_num] >= bus_rec.get_time()):
            return False
        else:
            last_bus_recs[veh_num] = bus_rec.get_time()
        return bus_rec.get_time() > time_of_start

    def _is_departure_in_time_window(self,
                                     departure: Departure,
                                     time_window: tuple[datetime, datetime]) \
            -> bool:
        dep_time = departure.get_time()
        return time_window[0] <= dep_time <= time_window[1]

    def _log_start_of_recording(self,
                                time_of_end: datetime,
                                download_duration: float) -> None:
        logging.info(f'Starting recording. Duration: {download_duration} s. '
                     f'End time: {time_of_end}.')

    def _add_api_key(self, url: str) -> str:
        return url + "&apikey=" + self._config.get_api_key()

    def _get_bus_stop_lines_url(self,
                                bus_stop_id: str,
                                bus_stop_number: str) -> str:
        return self._add_api_key(self._config.get_bus_stop_lines_url()
                                 + f'&busstopId={bus_stop_id}'
                                 + f'&busstopNr={bus_stop_number}')

    def _get_departures_url(self,
                            bus_stop_id: str,
                            bus_stop_number: str,
                            line: str) -> str:
        return self._add_api_key(self._config.get_schedule_url()
                                 + f'&busstopId={bus_stop_id}'
                                 + f'&busstopNr={bus_stop_number}'
                                 + f'&line={line}')
