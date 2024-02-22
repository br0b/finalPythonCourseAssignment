import logging

from data_downloader import DataDownloader
from myutils import (ProjectConfig,
                     TimeParser,
                     get_project_config,
                     save_to_csv,
                     serialize_list,
                     setup_logging)


def download_departures(data_downloader: DataDownloader,
                        config: ProjectConfig) -> None:
    bus_stops = data_downloader.download_bus_stops()
    save_to_csv(serialize_list(bus_stops), config.get_bus_stops_file())
    data_downloader.download_departures(bus_stops)


def main() -> None:
    setup_logging(logging.INFO)
    config = get_project_config()
    time_parser = TimeParser(config.get_time_format())
    data_downloader = DataDownloader(config, time_parser)
    download_departures(data_downloader, config)


if __name__ == '__main__':
    main()
