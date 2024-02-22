from datetime import datetime, timedelta
import mock

from bus_analysis.data_downloader import BusStop
from bus_analysis.departures_downloader import (DataDownloader,
                                                ProjectConfig,
                                                TimeParser)
from bus_analysis import myutils
from bus_analysis.myutils import BusRecord, Position


def get_download_duration_mock(self) -> timedelta:
    return timedelta(seconds=1)


def download_valid_bus_rec_json_mock(a, b, return_n_requests):
    time_parser = TimeParser('%Y-%m-%d %H:%M:%S')
    return ({
        'result': [
            {
                'Lines': '1',
                'Lon': 21.0,
                'VehicleNumber': '1000',
                'Time': time_parser.parse_to_str(datetime.now()),
                'Lat': 52.0,
                'Brigade': '1'
            }]
        },
        1)


def download_valid_bus_stops_json_mock(a, b, delay):
    return {
        "result": [{
            "values": [
                {
                    "value": "1001",
                    "key": "zespol"
                },
                {
                    "value": "01",
                    "key": "slupek"
                },
                {
                    "value": "Kijowska",
                    "key": "nazwa_zespolu"
                },
                {
                    "value": "2201",
                    "key": "id_ulicy"
                },
                {
                    "value": "52.248455",
                    "key": "szer_geo"
                },
                {
                    "value": "21.044827",
                    "key": "dlug_geo"
                },
                {
                    "value": "al.Zieleniecka",
                    "key": "kierunek"
                },
                {
                    "value": "2023-10-14 00:00:00.0",
                    "key": "obowiazuje_od"
                }
            ]}]
        }


def test_download_bus_stops():
    with mock.patch.object(ProjectConfig,
                           'get_download_duration',
                           new=get_download_duration_mock), \
          mock.patch.object(DataDownloader,
                            '_download_valid_json',
                            new=download_valid_bus_stops_json_mock):
        config = myutils.get_project_config()
        time_parser = TimeParser(config.get_time_format())
        data_downloader = DataDownloader(config, time_parser)
        bus_stop = data_downloader.download_bus_stops()[0]
        example_bus_stop = BusStop('1001',
                                   '01',
                                   Position(52.248455, 21.044827))
        assert (bus_stop.get_bus_stop_id()
                == example_bus_stop.get_bus_stop_id())
        assert (bus_stop.get_bus_stop_number()
                == example_bus_stop.get_bus_stop_number())
        assert bus_stop.get_position() == example_bus_stop.get_position()


def test_record_buses():
    with mock.patch.object(ProjectConfig,
                           'get_download_duration',
                           new=get_download_duration_mock), \
          mock.patch.object(DataDownloader,
                            '_download_valid_json',
                            new=download_valid_bus_rec_json_mock):
        config = myutils.get_project_config()
        time_parser = TimeParser(config.get_time_format())
        data_downloader = DataDownloader(config, time_parser)
        bus_rec = data_downloader.record_buses()[0][0]
        example_bus_record = BusRecord('1000',
                                       '1',
                                       '1',
                                       datetime.now(),
                                       Position(52.0, 21.0))
        assert bus_rec.get_vehicle_number() == example_bus_record.get_vehicle_number()
        assert bus_rec.get_line() == example_bus_record.get_line()
        assert bus_rec.get_brigade() == example_bus_record.get_brigade()
        assert bus_rec.get_position() == example_bus_record.get_position()
