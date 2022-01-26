from datetime import datetime, timedelta
from utils.CacheManagement import CacheManagement
from utils import grpc_calls
import grpc

class Sqlite_Management:

    def __init__(self, sqlite_file_path):
        self.cache_object = CacheManagement(sqlite_file_path)
        self.channel = grpc.insecure_channel("localhost:50051")
        self.creds = [("auth-licence", "ccT0F804bU8093N3900I5ff4Sfe49Ob58aS3b"), ("auth-client", "LSCL")]
        pass

    def get_junction_lat_lon(self, group, junction):
        
        # check if table exist 
        self.cache_object.cache_execute("""
            CREATE TABLE IF NOT EXISTS `utmc_traffic_signal_static` (
            `id` INTEGER PRIMARY KEY AUTOINCREMENT,
            `SystemCodeNumber` varchar(100) NOT NULL,
            `Group_SCN` varchar(100) NOT NULL,
            `Latitude` float NOT NULL,
            `Longitude` float NOT NULL
            )"""
        )

        junction_info = self.cache_object.cache_execute(
            f"SELECT `Latitude`, `Longitude` FROM `utmc_traffic_signal_static` WHERE `SystemCodeNumber` = '{junction}' AND `Group_SCN` = '{group}'"
        )
        result = junction_info.fetchall()
        if len(result) == 0:
            lat, lon = grpc_calls.get_junction_info([junction, ], self.channel, self.creds)
            self.cache_object.cache_execute(f""" 
                INSERT INTO `utmc_traffic_signal_static` (`SystemCodeNumber`, `Group_SCN`, `Latitude`, `Longitude`) VALUES
                ('{junction}', '{group}', {lat}, {lon})
            """ )
        else:
            for res in result:
                lat = res["Latitude"]
                lon = res["Lomgitude"]
            
        return {"latitude": lat, "longitude": lon}
        
    def add_link_counts_data_sqlite(self, group, weekday, slot, link, count):
        """
            Added the link and counts data to sqlite        
        """
        self.cache_object.cache_execute("""
            CREATE TABLE IF NOT EXISTS `utmc_links_count` (
            `id` INTEGER PRIMARY KEY AUTOINCREMENT,
            `LinkSCN` varchar(100) NOT NULL,
            `Group_SCN` varchar(100) NOT NULL,
            `Day` varchar(100) NOT NULL,
            `SlotOrder` INTEGER NOT NULL,
            `TotalFlow` float NOT NULL,
            )"""
        )
        date_time = datetime.now()
        self.cache_object.cache_execute(f""" 
                INSERT INTO `utmc_links_count` (
                    `LinkSCN`, 
                    `Group_SCN`, 
                    `Day`, 
                    `SlotOrder`, 
                    `TotalFlow`, 
                    `LastUpdated`) VALUES
                ('{link}', '{group}', '{weekday}', {slot}, {count}, '{date_time}')
            """ )

    def get_links_total_count_dict(self, group, weekday, slot, zone, mode, time_period, dbcreds):
        '''
            Counts during slot time.
            Return:
                {J001_L01:232,J001_L02:1278,..}
        '''
        end_time = datetime.now()
        start_time = end_time-timedelta(minutes = time_period)
        junction_info = self.cache_object.cache_execute(
            f"SELECT `TotalFlow`, `LinkSCN` FROM `utmc_links_count` WHERE `Group_SCN` = '{group}' AND `Day` = {weekday} AND `SlotOrder` = {slot} AND `LastUpdated` > {start_time} AND `LastUpadted` < {end_time}"
        )
        result = junction_info.fetchall()

        links_total_count_dict = {}

        for data in result:
            if data[1] not in links_total_count_dict:
                links_total_count_dict[data[1]] = data[0]
            else:
                links_total_count_dict[data[1]] += data[0]

        return links_total_count_dict

    def delete_history_counts(self, days):

        delete_before = datetime.now()-timedelta(days=days)
        
        query = f"""DELETE FROM `utmc_links_count` WHERE `LastUpdated` < '{delete_before}';"""
        self.cache_object.cache_execute(query)

if __name__ == "__main__":

    sqlite_object = Sqlite_Management("Sqlite/htms.db")
    sqlite_object.get_junction_lat_lon("GRP001", "J018")

