
class StreamState():
    def __init__(self):
        self._weather_eof = False
        self._stations_eof = False
        self._trips_eof = False

    def set_weather_eof(self):
        self._weather_eof = True
    
    def set_stations_eof(self):
        self._stations_eof = True
    
    def set_trips_eof(self):
        self._trips_eof = True
    
    def not_static_data_eof_received(self):
        return not self._weather_eof or not self._stations_eof

    def not_trips_eof_received(self):
        return not self._trips_eof
