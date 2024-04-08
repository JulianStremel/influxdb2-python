"""Module for interacting with InfluxDB 2.0"""

import datetime
from enum import Enum
import requests

class Status(Enum):
    """Status codes for InfluxDB 2.0 API"""
    OK                          = 200
    NO_CONTENT                  = 204
    BAD_REQUEST                 = 400
    UNAUTHORIZED                = 401
    NOT_FOUND                   = 404
    METHOD_NOT_ALLOWED          = 405
    REQUEST_ENTITY_TOO_LARGE    = 413
    UNPROCESSABLE_ENTITY        = 422
    TOO_MANY_REQUESTS           = 429
    INTERNAL_SERVER_ERROR       = 500
    SERVICE_UNAVAILABLE         = 503


class InfluxPoint:
    """Class for creating InfluxDB 2.0 points"""
    def __check_key__(self, key:str):
        assert isinstance(key, str)
        if key == "":
            raise ValueError("key cannot be empty")
        if list(key)[0] == "_":
            raise ValueError(f"key cannot start with an underscore -> {key}")

    class Tag:
        """class for creating tags in InfluxDB 2.0 points"""
        def __init__(self, key:str, value:str):
            assert isinstance(key, str)
            assert isinstance(value, str)
            if key == "":
                raise ValueError("key cannot be empty")
            if list(key)[0] == "_":
                raise ValueError(f"key cannot start with an underscore -> {key}")
            self.key = key
            self.value = value

        def __str__(self) -> str:
            raise NotImplementedError("This function is not yet implemented") from None

        def __repr__(self) -> str:
            return f"Tag({self.key}, {self.value})"

    class Field:
        """class for creating fields in InfluxDB 2.0 points"""
        _type:type
        def __init__(self, key:str, value:str|float|int|bool):
            assert isinstance(key, str)
            if key == "":
                raise ValueError("key cannot be empty")
            if list(key)[0] == "_":
                raise ValueError(f"key cannot start with an underscore -> {key}")
            self._type = type(value)
            self.key = key
            self.value = value

        def __str__(self) -> str:
            raise NotImplementedError("This function is not yet implemented") from None

        def __repr__(self) -> str:
            return f"Field({self.key}, {self.value})"

    measurement:str
    tags:list[Tag]
    fields:list[Field]
    timestamp:int|None

    def __init__(self, measurement:str):
        assert isinstance(measurement, str)
        if measurement == "":
            raise ValueError("measurement cannot be empty")
        if list(measurement)[0] == "_":
            raise ValueError(f"measurement cannot start with an underscore -> {measurement}")
        self.measurement = measurement
        self.timestamp = None
        self.tags = []
        self.fields = []

    def add_tag(self, key:str, value:str):
        """add a tag to the point"""
        tag = self.Tag(key, value)
        self.tags.append(tag)

    def add_field(self, key:str, value:str):
        """add a field to the point"""
        field = self.Field(key, value)
        self.fields.append(field)

    def add_timestamp(self, timestamp:int):
        """add a timestamp to the point"""
        assert isinstance(timestamp, int)
        try:
            datetime.datetime.fromtimestamp(timestamp)
        except:
            raise ValueError("Invalid timestamp") from None
        self.timestamp = timestamp

    def __str__(self):
        raise NotImplementedError("This function is not yet implemented")




class InfluxClient:
    """Class for interacting with InfluxDB 2.0"""
    endpoint:str = "/api/v2/"
    url:str
    token:str
    org:str
    bucket:str
    session:requests.Session


    def __init__(self, url:str, token:str , org:str, bucket:str):
        assert isinstance(url, str)
        assert isinstance(token, str)
        assert isinstance(org, str)
        assert isinstance(bucket, str)
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {self.token}"
        })

    def __match_status__(self, status:int) -> tuple[bool,str|None]:
        ret = None
        match status:
            case Status.OK:
                ret = (True, None)
            case Status.BAD_REQUEST:
                ret = (False, "Bad request")
            case Status.UNAUTHORIZED:
                ret = (False, "Unauthorized")
            case Status.REQUEST_ENTITY_TOO_LARGE:
                ret = (False, "Request entity too large")
            case Status.UNPROCESSABLE_ENTITY:
                ret = (False, "Unprocessable entity")
            case Status.TOO_MANY_REQUESTS:
                ret = (False, "Too many requests")
            case Status.INTERNAL_SERVER_ERROR:
                ret = (False, "Internal server error")
            case Status.SERVICE_UNAVAILABLE:
                ret = (False, "Service unavailable")
            case _:
                ret = (False, f"Unknown statuscode {status} ")
        return ret
    # POST
    def write(self, point:InfluxPoint) -> tuple[bool,str|None]:
        """write a point to the database"""
        assert isinstance(point, InfluxPoint)
        url = f"{self.url}{self.endpoint}write"
        params = {
            "bucket": self.bucket,
            "org": self.org,
        }
        headers = {
            "Accept": "application/json",
            "Content-Encoding": "gzip",
            "Content-Type": "text/plain; charset=utf-8",
        }
        session = self.session.post(url, params=params, headers=headers, data=str(point))
        return self.__match_status__(session.status_code)

    #POST
    def run(self) -> tuple[bool,str|None]:
        """run a task in the database"""
        raise NotImplementedError("This function is not yet implemented")

    #GET
    def list(self) -> tuple[bool,str|None]:
        """list all tasks in the database"""
        raise NotImplementedError("This function is not yet implemented")

    #POST
    def create(self) -> tuple[bool,str|None]:
        """create a task in the database"""
        raise NotImplementedError("This function is not yet implemented")

    #PUT
    def update(self) -> tuple[bool,str|None]:
        """update a task in the database"""
        raise NotImplementedError("This function is not yet implemented")

    #DELETE
    def delete(self) -> tuple[bool,str|None]:
        """delete a task in the database"""
        raise NotImplementedError("This function is not yet implemented")
