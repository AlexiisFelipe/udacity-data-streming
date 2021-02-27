"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.connect-stations", value_type=Station)
output_topic = app.topic("org.chicago.cta.stations", partitions=1)

table = app.Table(
    "table-stations",
    default=str,
    partitions=1,
    changelog_topic=output_topic,
)


@app.agent(topic)
async def process(stations):
    async for station in stations:
        if station.green:
            line_color = "green"
        elif station.blue:
            line_color = "blue"
        else:
            line_color = "red"
        tranformed_station = TransformedStation(station_id=station.station_id,
                                                station_name=station.station_name,
                                                order=station.order,
                                                line=line_color)
        table[station.station_id] = tranformed_station
        
        
if __name__ == "__main__":
    app.main()
