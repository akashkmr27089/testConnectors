from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Dict
from abc import ABC, abstractmethod
from pydantic.errors import ConfigError
import requests
from .services.connectors.snowflakeService import SnowflakeService
from .services.slicing.pickkerSlicingService import PickkerSlicingService

from .getOrdersIdsEndpoint import getOrdersIds
from .getTocken import getTokenByRefreshToken

from airbyte_cdk.models import Type as MessageType
import copy
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    SyncMode,
)
from typing import Any, Dict, Iterator, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig, split_config
from airbyte_cdk.utils.event_timing import create_timer



# from .AbstractSourceModified import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from .utils import EagerlyCachedStreamState as stream_state_cache
from airbyte_cdk.models import SyncMode
# from datetime import datetime
import datetime

# Base Class 
class Pickrr(HttpStream):

    # Page size
    limit = 1000
    primary_key = "id"
    order_field = "updated"
    filter_field = "updated"

    url_base = "https://"

    # Set this as a noop.
    primary_key = None

    def __init__(self, config: Dict, **kwargs):
        super().__init__(**kwargs)
        # self.marketplace = config["marketplace"]
        # self.accessToken = config["accessToken"]
        self.config = config
        print()
        # Here's where we set the variable from our input to pass it down to the source.

class IncrementalStream(Pickrr, ABC):

    # Setting the check point interval to the limit of the records output
    @property
    def state_checkpoint_interval(self) -> int:
        return super().limit

    # Setting the default cursor field for all streams
    cursor_field = "updated"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, ""), current_stream_state.get(self.cursor_field, ""))}

    # @stream_state_cache.cache_stream_state
    def request_params(self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs):
        params = super().request_params(stream_state=stream_state, next_page_token=next_page_token, **kwargs)
        # If there is a next page token then we should only send pagination-related parameters.
        # if not next_page_token:
        #     params["order"] = f"{self.order_field} asc"
        #     if stream_state:
        #         params[self.filter_field] = stream_state.get(self.cursor_field)
        return params

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state object
        and returning an updated state object.
        """
        latest_state = latest_record.get(self.cursor_field)
        current_state = current_stream_state.get(self.cursor_field) or latest_state
        return {self.cursor_field: max(latest_state, current_state)}

    # Parse the stream_slice with respect to stream_state for Incremental refresh
    # cases where we slice the stream, the endpoints for those classes don't accept any other filtering,
    # but they provide us with the updated_at field in most cases, so we used that as incremental filtering during the order slicing.
    def filter_records_newer_than_state(self, stream_state: Mapping[str, Any] = None, records_slice: Mapping[str, Any] = None) -> Iterable:
        # Getting records >= state
        # stream_state = True
        if stream_state:
            for record in records_slice:
                if record.get(self.cursor_field, "") >= stream_state.get(self.cursor_field):
                    yield record
        else:
            yield from records_slice

    @property
    def use_cache(self):
        return False

#Snowflake Service 
class GetTrackingID:

    def __init__(self, TDatabase: str, config) -> None:
        self.config = config
        self.TDatabase = TDatabase
        # print("###"*32, TDatabase)
        self.dataCollectorType = 'snowflake'
        self.getParams = {"pos": 8}

    def getTrackingIdSnowflake(self):
        self.sliceService = PickkerSlicingService()
        self.snowService =  SnowflakeService(user=self.config["snowflake_username"],
                                                password=self.config["password"],
                                                account=self.config["account"],
                                                warehouse=self.config["warehouse"],
                                                database=self.config["database"],
                                                role=self.config["role"])

        #connect to snowflake 
        self.snowService.connect()
        self.snowflakeConn = self.snowService.returnConnectionObject()
        if(self.snowflakeConn != None):
            # Connection is Established, Now We can querry
            # print("TESTING ", 'SELECT * FROM "{}"."{}"."{}" WHERE {} ILIKE '.format(self.config["database"], self.config["source_schema"], self.config["source_table"], self.config["column_name"]) + "'%pickrr%'")
            print(self.config["test"], self.config["test"].name)
            sqlScript = 'SELECT * FROM "{}"."{}"."{}" WHERE {} ILIKE '.format(self.config["database"], self.config["source_schema"], self.config["source_table"], self.config["column_name"]) + "'%pickrr%'"
            if(self.config["test"].name == 'incremental'):
                currentServerDate = datetime.datetime.today()
                pastServerDate = currentServerDate - datetime.timedelta(45)
                currServerDateString = currentServerDate.strftime("%Y-%m-%dT") + "00:00:00Z"
                pastServerDateString = pastServerDate.strftime("%Y-%m-%dT") + "00:00:00Z"
                print("Previous Date {} Current Date {}".format(pastServerDateString, currServerDateString))
                # sqlScript = 'SELECT * FROM "{}"."{}"."{}" WHERE {} ILIKE '.format(self.config["database"], self.config["source_schema"], self.config["source_table"], self.config["column_name"]) + "'%pickrr%'" + ' and to_date(CREATED_AT) > to_date(' + "'2021-10-25T12:00:00Z'" + ') and to_date(CREATED_AT) < to_date(' + "'2021-11-26T12:00:00Z'" + ') order by CREATED_AT '
                # print("Previous SqlScript :", sqlScript)
                sqlScript = 'SELECT * FROM "{}"."{}"."{}" WHERE {} ILIKE '.format(self.config["database"], self.config["source_schema"], self.config["source_table"], self.config["column_name"]) + "'%pickrr%'" + ' and to_date(CREATED_AT) > to_date(' + f"'{pastServerDateString}'" + ') and to_date(CREATED_AT) < to_date(' + f"'{currServerDateString}'" + ') order by CREATED_AT '

            print(sqlScript)
            data = self.snowService.executeQuerry(sqlScript)
            # print(self.snowService.executeQuerry('SELECT count(*) FROM "{}"."{}"."{}" WHERE {} ILIKE '.format(self.config["database"], self.config["source_schema"], self.config["source_table"], self.config["column_name"]) + "'%pickrr%'" + ' and to_date(CREATED_AT) > to_date(' + "'2021-10-25T12:00:00Z'" + ') and to_date(CREATED_AT) < to_date(' + "'2021-11-26T12:00:00Z'" + ') order by CREATED_AT '))
            # Put the Cursor fields here 
            slicingResponse = self.sliceService.getSlicedDataList(self.dataCollectorType, data, self.getParams)
            # print(slicingResponse)
            return slicingResponse

    def getTrackingIdList(self):
        preData = []
        # print("##"*30, self.TDatabase)
        preData = self.getTrackingIdSnowflake()
        # if(self.TDatabase == 'snowlflake'):
        #     print("##*"*30, self.TDatabase)
        #     preData = self.getTrackingIdSnowflake()
        return preData
                           
# This will handle the connectivity and sclicing of the Objects 
class ChildSubstream(IncrementalStream):

    """
    ChildSubstream - provides slicing functionality for streams using parts of data from parent stream.
    For example:
       - `Refunds Orders` is the entity of `Orders`,
       - `OrdersRisks` is the entity of `Orders`,
       - `DiscountCodes` is the entity of `PriceRules`, etc.

    ::  @ parent_stream_class - defines the parent stream object to read from
    ::  @ slice_key - defines the name of the property in stream slices dict.
    ::  @ record_field_name - the name of the field inside of parent stream record. Default is `id`.
    """

    parent_stream_class: object = None
    slice_key: str = None
    record_field_name: str = "id"
    limit = 1000

    def request_params(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = {"limit": self.limit}
        if next_page_token:
            params.update(**next_page_token)
        return params

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Reading the parent stream for slices with structure:
        EXAMPLE: for given record_field_name as `id` of Orders,

        Output: [ {slice_key: 123}, {slice_key: 456}, ..., {slice_key: 999} ]
        """

        # print(stream_state,self.parent_stream_class)
        # parent_stream = self.parent_stream_class(TDatabase = "snowflake", config=self.config)
        # parent_stream_state = stream_state_cache.cached_state.get(parent_stream.name)


        trackingClass = GetTrackingID('snowflake', config=self.config)
        getTrackingid = trackingClass.getTrackingIdList()
        print()
        if(not getTrackingid['error']):
            return getTrackingid['data']
        else:
            return []

        # return [{"code": "1531529757301"}, {"code": "1531529757301"}, {"code": "1531529757301"}]

    def read_records(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        **kwargs,
    ) -> Iterable[Mapping[str, Any]]:
        """Reading child streams records for each `id`"""
        # self.logger.info(f"Reading {self.name} for {self.slice_key}: {stream_slice.get(self.slice_key)}")
        records = super().read_records(stream_slice=stream_slice, **kwargs)
        yield from self.filter_records_newer_than_state(stream_state=stream_state, records_slice=records)

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state object
        and returning an updated state object.
        """
        latest_state = latest_record.get(self.cursor_field)
        current_state = current_stream_state.get(self.cursor_field) or latest_state
        return {self.cursor_field: max(latest_state, current_state)}

class GetOrdersByIds(ChildSubstream):
    # data_field = "orders"
    parent_stream_class: object = GetTrackingID
    # slice_key = "code"
    # cursor_field = "created"

    def __init__(self, config, **kwargs):
        self.auth_token = config["auth_token"]
        super().__init__(config, **kwargs)

    def path(self, **kwargs) -> str:
        # This defines the path to the endpoint that we want to hit.
        return "https://async.pickrr.com/track/tracking/"

    # @stream_state_cache.cache_stream_state
    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the Pokemon name as a query param so we do that in this method.
        # print(self.email, self.password)
        return {"tracking_id": stream_slice["code"], "auth_token": self.config['auth_token']}

    def _create_prepared_request(
        self, path: str, headers: Mapping = None, params: Mapping = None, json: Any = None, data: Any = None
    ) -> requests.PreparedRequest:
        args = {"method": "GET", "url": path, "headers": headers, "params": params}

        # if self.http_method.upper() == "POST":
        args["json"] = json
        args["data"] = data
        # args["headers"] = {"Authorization": f"bearer {self.accessToken}", "Content-Type": "application/json"}
        # print("Type of", args )

        return self._session.prepare_request(requests.Request(**args))

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        # The response is a simple JSON whose schema matches our stream's schema exactly,
        # so we just return a list containing the response.
        print()
        print()
        # a = response.json()['status']["current_status_time"]
        # print("Api Timming :", a, datetime.datetime.strptime(a, '%d %b %Y, %H:%M'), datetime.datetime.strptime(a, '%d %b %Y, %H:%M') > datetime.datetime.strptime('10 Jan 2022, 00:00', '%d %b %Y, %H:%M'))
        return [response.json()]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # While the PokeAPI does offer pagination, we will only ever retrieve one Pokemon with this implementation,
        # so we just return None to indicate that there will never be any more pages in the response.
        return None

class SourcePickkr(AbstractSource):

    def _read_incremental(
        self, stream_instance: Stream, configured_stream: ConfiguredAirbyteStream, internal_config: InternalConfig
    ) -> Iterator[AirbyteMessage]:
        slices = stream_instance.stream_slices(sync_mode=SyncMode.full_refresh, cursor_field=configured_stream.cursor_field)
        total_records_counter = 0
        for slice in slices:
            records = stream_instance.read_records(
                stream_slice=slice, sync_mode=SyncMode.full_refresh, cursor_field=configured_stream.cursor_field
            )
            for record in records:
                yield self._as_airbyte_record(configured_stream.stream.name, record)
                total_records_counter += 1
                if self._limit_reached(internal_config, total_records_counter):
                    return
    
    def read(
        self, logger: AirbyteLogger, config: Mapping[str, Any], catalog: ConfiguredAirbyteCatalog, state: MutableMapping[str, Any] = None
    ) -> Iterator[AirbyteMessage]:
        """Implements the Read operation from the Airbyte Specification. See https://docs.airbyte.io/architecture/airbyte-specification."""
        connector_state = copy.deepcopy(state or {})
        logger.info(f"Starting syncing {self.name}")
        config, internal_config = split_config(config)
        # TODO assert all streams exist in the connector
        # get the streams once in case the connector needs to make any queries to generate them
        stream_instances = {s.name: s for s in self.streams(config)}
        self._stream_to_instance_map = stream_instances
        self.testConfig = catalog.streams[0].sync_mode
        config['test'] = catalog.streams[0].sync_mode
        print(self.testConfig)
        with create_timer(self.name) as timer:
            for configured_stream in catalog.streams:
                stream_instance = stream_instances.get(configured_stream.stream.name)
                if not stream_instance:
                    raise KeyError(
                        f"The requested stream {configured_stream.stream.name} was not found in the source. Available streams: {stream_instances.keys()}"
                    )
                try:
                    yield from self._read_stream(
                        logger=logger,
                        stream_instance=stream_instance,
                        configured_stream=configured_stream,
                        connector_state=connector_state,
                        internal_config=internal_config,
                    )
                except Exception as e:
                    logger.exception(f"Encountered an exception while reading stream {self.name}")
                    raise e
                finally:
                    logger.info(f"Finished syncing {self.name}")
                    logger.info(timer.report())
        logger.info(f"Finished syncing {self.name}")

    def _read_stream(
        self,
        logger: AirbyteLogger,
        stream_instance: Stream,
        configured_stream: ConfiguredAirbyteStream,
        connector_state: MutableMapping[str, Any],
        internal_config: InternalConfig,
    ) -> Iterator[AirbyteMessage]:
        if internal_config.page_size and isinstance(stream_instance, HttpStream):
            logger.info(f"Setting page size for {stream_instance.name} to {internal_config.page_size}")
            stream_instance.page_size = internal_config.page_size
        self.typeofIncremental = configured_stream.sync_mode
        use_incremental = configured_stream.sync_mode == SyncMode.incremental and stream_instance.supports_incremental
        if use_incremental:
            # record_iterator = self._read_incremental(logger, stream_instance, configured_stream, connector_state, internal_config)
            record_iterator = self._read_incremental(stream_instance, configured_stream, internal_config)
        else:
            record_iterator = self._read_full_refresh(stream_instance, configured_stream, internal_config)
        record_counter = 0
        stream_name = configured_stream.stream.name
        logger.info(f"Syncing stream: {stream_name} ")
        for record in record_iterator:
            if record.type == MessageType.RECORD:
                record_counter += 1
            yield record
        logger.info(f"Read {record_counter} records from {stream_name} stream")

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        self.config = config
        self.snowService =  SnowflakeService(user=self.config["snowflake_username"],
                                        password=self.config["password"],
                                        account=self.config["account"],
                                        warehouse=self.config["warehouse"],
                                        database=self.config["database"],
                                        role=self.config["role"])
        self.snowService.connect()
        test = self.snowService.returnConnectionObject()
        if(test == None):
            return False, f"Cannot validate Credentials for the Given Snowflake"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [GetOrdersByIds(config)]
