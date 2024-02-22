from data_downloader import DataDownloader
import json
import logging

from myutils import (ProjectConfig,
                     TimeParser,
                     save_to_csv,
                     serialize_list,
                     setup_logging,
                     get_project_config)


def record_buses(data_downloader: DataDownloader,
                 config: ProjectConfig,
                 time_parser: TimeParser) -> None:
    """Records buses. Save the results and statistics to files.
    """
    bus_recs, bus_stats = data_downloader.record_buses()
    save_to_csv(serialize_list(bus_recs), config.get_bus_data_file())
    bus_stats.log_stats(time_parser)
    # Save stats to file.
    with open(config.get_bus_recording_stats_file(), 'w') as file:
        json.dump(bus_stats.to_json(time_parser), file)


def main() -> None:
    setup_logging(logging.INFO)
    config = get_project_config()
    time_parser = TimeParser(config.get_time_format())
    data_downloader = DataDownloader(config, time_parser)
    record_buses(data_downloader, config, time_parser)


if __name__ == '__main__':
    main()
