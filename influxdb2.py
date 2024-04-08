import requests
import time 
import datetime
from enum import Enum

class Status(Enum):
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

    def __check_key__(self, key:str):
        if key == ""|None:
            raise ValueError("key cannot be empty")
        if list(key)[0] == "_":
            raise ValueError(f"key cannot start with an underscore -> {key}")

    class tag:
        def __init__(self, key:str, value:str):
            assert isinstance(key, str)
            assert isinstance(value, str)
            if key == ""|None:
                raise ValueError("key cannot be empty")
            if list(key)[0] == "_":
                raise ValueError(f"key cannot start with an underscore -> {key}")
            self.key = key
            self.value = value

    class field:
        _type:type
        def __init__(self, key:str, value:str|float|int|bool):
            assert isinstance(key, str)
            if key == ""|None:
                raise ValueError("key cannot be empty")
            if list(key)[0] == "_":
                raise ValueError(f"key cannot start with an underscore -> {key}")
            self._type = type(value)
            self.key = key
            self.value = value

    measurement:str
    tags:list[tag]
    fields:list[field]
    timestamp:int|None

    def __init__(self, measurement:str):
        assert isinstance(measurement, str)
        if measurement == ""|None:
            raise ValueError("measurement cannot be empty")
        if list(measurement)[0] == "_":
            raise ValueError(f"measurement cannot start with an underscore -> {measurement}")
        self.measurement = measurement
        self.timestamp = None
        self.tags = {}
        self.fields = {}
    
    def add_tag(self, key:str, value:str):
        tag = self.tag(key, value)
        if tag.key in self.tags.keys():
            raise ValueError("Tag already exists")
        self.tags.append(tag)
    
    def add_field(self, key:str, value:str):
        field = self.field(key, value)
        if field.key in self.fields.keys():
            raise ValueError("Field already exists")
        self.fields.append(field)
    
    def add_timestamp(self, timestamp:int):
        assert isinstance(timestamp, int)
        try:
            datetime.datetime.fromtimestamp(timestamp)
        except:
            raise ValueError("Invalid timestamp")
        self.timestamp = timestamp

    def __str__(self):
        first = True
        for key, value in self.tags.items():
            if value == ""|None:
                continue 
            if first:
                tags += f"{key}={value}"
                first = False
            else:
                tags += f",{key}={value}"
        tags+= " "

        first = True
        for key, value in self.fields.items():
            if first:
                fields += f"{key}={value}"
                first = False
            else:
                fields += f",{key}={value}"
        fields+= " "

        return f"{self.measurement},{self.tags}, Fields: {self.fields}, Timestamp: {self.timestamp}, Precision: {self.precision}"




class InfluxClient:

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
        match status:
            case Status.OK:
                return (True, None)
            case Status.BAD_REQUEST:
                return (False, "Bad request")
            case Status.UNAUTHORIZED:
                return (False, "Unauthorized")
            case Status.REQUEST_ENTITY_TOO_LARGE:
                return (False, "Request entity too large")
            case Status.UNPROCESSABLE_ENTITY:
                return (False, "Unprocessable entity")
            case Status.TOO_MANY_REQUESTS:
                return (False, "Too many requests")
            case Status.INTERNAL_SERVER_ERROR:
                return (False, "Internal server error")
            case Status.SERVICE_UNAVAILABLE:
                return (False, "Service unavailable")
            case _:
                return (False, f"Unknown statuscode {status} ")

    # POST
    def write(self, point:InfluxPoint) -> tuple[bool,str|None]:
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
    def run() -> tuple[bool,str|None]:
        raise NotImplementedError("This function is not yet implemented")
    
    #GET
    def list() -> tuple[bool,str|None]:
        raise NotImplementedError("This function is not yet implemented")

    #POST
    def create() -> tuple[bool,str|None]:
        raise NotImplementedError("This function is not yet implemented")

    #PUT
    def update() -> tuple[bool,str|None]:
        raise NotImplementedError("This function is not yet implemented")

    #DELETE
    def delete() -> tuple[bool,str|None]:
        raise NotImplementedError("This function is not yet implemented")
