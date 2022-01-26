from datetime import datetime, timedelta, date
import grpc
import calendar
from configparser import ConfigParser

from google.protobuf.timestamp_pb2 import Timestamp
from autogen.utmc.traffic_signal_pb2_grpc import TrafficSignalStub
from autogen.utmc.requests_pb2 import DataReadReq
from autogen.utmc.detector_pb2_grpc import DetectorStub
from autogen.tis.timetable_pb2_grpc import TimeTableStub
from autogen.tis.timetable_pb2 import DataReq

config_file_path = "config.ini"

def grpc_operation(function):

    def inner_funct(*args, **kwargs):

        config = ConfigParser()
        config.read(config_file_path)

        auth_licence = config.get("GRPC", "auth_licence", fallback="ccT0F804bU8093N3900I5ff4Sfe49Ob58aS3b")
        auth_client = config.get("GRPC", "auth_client", fallback="INTERNAL")
        creds=[("auth-licence", auth_licence), ("auth-client", auth_client)]
        channel = grpc.insecure_channel(f"{config.get('GRPC', 'host')}:{config.get('GRPC', 'port')}")
        print(f"{config.get('GRPC', 'host')}:{config.get('GRPC', 'port')}")
        # channel = grpc.insecure_channel("localhost:50051")
        kwargs["channel"] = channel
        kwargs["creds"] = creds

        return function(*args, **kwargs)

    return inner_funct


@grpc_operation
def get_junction_info(junction, channel = "", creds = ""):

    stub = TrafficSignalStub(channel)

    results = stub.GetStatic(
        DataReadReq(
            system_code_number = junction
        ),
        metadata = creds
    )

    for data in results.definitions:
        lat = data.latitude
        lon = data.longitude

    return lat, lon

@grpc_operation
def get_slots_time_tuple(group, weekday, slot, channel = "", creds = ""):
    '''
        (slot_name,start_time,end_time,duration in hours)
    '''

    stub = TimeTableStub(channel)

    results = stub.GetCalendarData(
        DataReq(
            group_scn = group,
            date = weekday,
            slot_order = slot
        ),
        metadata = creds
    )
    print(results)
    for data in results.Data:
        time_difference = ((data.start_time.ToTimedelta() - \
            data.end_time.ToTimedelta()).total_seconds())//(60*60)

        return (data.slot_order, data.start_time.ToTimedelta(), data.end_time.ToTimedelta(), time_difference)

@grpc_operation
def get_all_slot_order(group, weekday, channel = "", creds = ""):

    stub = TimeTableStub(channel)

    results = stub.GetCalendarData(
        DataReq(
            group_scn = group,
            date = weekday
        ),
        metadata = creds
    )
    print(results)
    slot_list = []
    for data in results.Data:
        slot_list.append([data.slot_order, data.start_time.ToTimedelta(), data.end_time.ToTimedelta()])

    return slot_list
