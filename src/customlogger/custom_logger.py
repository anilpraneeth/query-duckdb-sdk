import logging
import json
from pythonjsonlogger import jsonlogger

class CustomLogger(logging.Logger):
    def logjson(self, level, message, json_obj=None):
        if isinstance(level, str):
            level = getattr(logging, level.upper())
        
        if json_obj is None:
            json_obj = {}
            
        self.log(level, message, extra={'json_obj': json_obj})

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if hasattr(record, 'json_obj'):
            log_record.update(record.json_obj) 